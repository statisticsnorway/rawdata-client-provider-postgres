package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class PostgresRawdataConsumer implements RawdataConsumer {

    static final CountDownLatch OPEN_LATCH = new CountDownLatch(0);
    final int DB_POLL_PREFETCH_POLL_INTERVAL_WHEN_EMPTY_MILLISECONDS = 1000;

    final TransactionFactory transactionFactory;
    final String topic;
    final AtomicReference<PostgresRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Lock pollLock = new ReentrantLock();
    final Condition condition = pollLock.newCondition();
    final Deque<PostgresRawdataMessage> messageBuffer = new ConcurrentLinkedDeque<>();
    final AtomicReference<CompletableFuture<Integer>> pendingPrefetch = new AtomicReference<>(CompletableFuture.completedFuture(0));
    final AtomicReference<Long> pendingPrefetchExpiry = new AtomicReference<>(System.currentTimeMillis());
    final int prefetchSize;

    PostgresRawdataConsumer(TransactionFactory transactionFactory, String topic, PostgresRawdataMessageId initialPosition, int prefetchSize) {
        this.transactionFactory = transactionFactory;
        this.prefetchSize = prefetchSize;
        this.topic = topic;
        if (initialPosition == null) {
            initialPosition = new PostgresRawdataMessageId(topic, new ULID.Value(0, 0), null);
        }
        position.set(initialPosition);
    }

    @Override
    public String topic() {
        return topic;
    }

    PostgresRawdataMessage findNextMessage() {
        CountDownLatch latch = OPEN_LATCH;
        if (messageBuffer.size() < 1 + (prefetchSize / 2) && pendingPrefetch.get().isDone()
                && (pendingPrefetch.get().join() > 0 || pendingPrefetchExpiry.get() <= System.currentTimeMillis())) {
            pendingPrefetch.set(fetchNextBatchAsync(latch = new CountDownLatch(1)));
            pendingPrefetchExpiry.set(System.currentTimeMillis() + DB_POLL_PREFETCH_POLL_INTERVAL_WHEN_EMPTY_MILLISECONDS);
        }
        while (messageBuffer.isEmpty() && !pendingPrefetch.get().isDone()) {
            try {
                latch.await(5, TimeUnit.SECONDS); // wait for completion of the prefetch first row only
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (messageBuffer.isEmpty()) {
                pendingPrefetch.get().join(); // wait for entire fetch to complete
            }
        }
        return messageBuffer.pollFirst();
    }

    private CompletableFuture<Integer> fetchNextBatchAsync(CountDownLatch cdl) {
        return transactionFactory.runAsyncInIsolatedTransaction(tx -> {
            try {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT c.name, c.data, p.ulid, p.opaque_id FROM \"%s_content\" c JOIN (SELECT ulid, opaque_id FROM \"%s_positions\" WHERE ulid > ? ORDER BY ulid LIMIT ?) p ON c.position_fk_ulid = p.ulid ORDER BY c.position_fk_ulid, c.name", topic, topic));
                UUID currentUuid = new UUID(position.get().id.getMostSignificantBits(), position.get().id.getLeastSignificantBits());
                ps.setObject(1, currentUuid);
                ps.setInt(2, prefetchSize);
                ResultSet rs = ps.executeQuery();
                PostgresRawdataMessage prevMessage = null;
                ULID.Value prevUlid = null;
                String prevOpaqueId = null;
                Map<String, byte[]> contentMap = new LinkedHashMap<>();
                int i = 0;
                while (rs.next()) {
                    String name = rs.getString(1);
                    byte[] data = rs.getBytes(2);
                    UUID uuid = (UUID) rs.getObject(3);
                    String opaqueId = rs.getString(4);
                    ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                    if (prevUlid == null) {
                        prevUlid = ulid;
                        prevOpaqueId = opaqueId;
                    }
                    if (!ulid.equals(prevUlid)) {
                        messageBuffer.add(prevMessage = new PostgresRawdataMessage(new PostgresRawdataMessageId(topic, prevUlid, prevOpaqueId), new PostgresRawdataMessageContent(prevOpaqueId, contentMap)));
                        if (i++ == 0) {
                            cdl.countDown(); // early signal that at least one message is available.
                        }
                        contentMap = new LinkedHashMap<>();
                    }
                    contentMap.put(name, data);
                    prevUlid = ulid;
                    prevOpaqueId = opaqueId;
                }
                if (prevUlid != null) {
                    i++;
                    messageBuffer.add(prevMessage = new PostgresRawdataMessage(new PostgresRawdataMessageId(topic, prevUlid, prevOpaqueId), new PostgresRawdataMessageContent(prevOpaqueId, contentMap)));
                }
                if (prevMessage != null) {
                    position.set(prevMessage.id());
                }
                return i;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            } finally {
                cdl.countDown();
            }
        }, true);
    }

    @Override
    public PostgresRawdataMessageContent receive(int timeout, TimeUnit unit) throws InterruptedException {
        int pollIntervalNanos = 250 * 1000 * 1000;
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
        if (!pollLock.tryLock()) {
            throw new RuntimeException("Concurrent access to receive not allowed");
        }
        try {
            PostgresRawdataMessage message = findNextMessage();
            while (message == null) {
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                condition.await(Math.min(durationNano, pollIntervalNanos), TimeUnit.NANOSECONDS);
                message = findNextMessage();
            }
            return message.content();
        } finally {
            pollLock.unlock();
        }

    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public String toString() {
        return "PostgresRawdataConsumer{" +
                "topic='" + topic + '\'' +
                "position=" + position.get() +
                ", closed=" + closed +
                '}';
    }
}
