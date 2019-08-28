package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PostgresRawdataConsumer implements RawdataConsumer {

    final TransactionFactory transactionFactory;
    final String topic;
    final AtomicReference<PostgresRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Lock pollLock = new ReentrantLock();
    final Condition condition = pollLock.newCondition();

    public PostgresRawdataConsumer(TransactionFactory transactionFactory, String topic, PostgresRawdataMessageId initialPosition) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
        if (initialPosition == null) {
            initialPosition = new PostgresRawdataMessageId(topic, -1, null);
        }
        position.set(initialPosition);
    }

    @Override
    public String topic() {
        return topic;
    }

    PostgresRawdataMessage findMessageContentOfIdAfterPosition(PostgresRawdataMessageId currentId) {
        Map<String, byte[]> contentMap = new LinkedHashMap<>();
        long id = -1;
        String opaqueId = null;
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT c.name, c.data, p.id, p.opaque_id FROM \"%s_content\" c JOIN (SELECT id, opaque_id FROM \"%s_positions\" WHERE id > ? ORDER BY id LIMIT 1) p ON c.position_fk_id = p.id ORDER BY c.position_fk_id, c.name", topic, topic));
                ps.setLong(1, currentId.id);
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String name = rs.getString(1);
                    byte[] data = rs.getBytes(2);
                    contentMap.put(name, data);
                    id = rs.getLong(3);
                    opaqueId = rs.getString(4);
                }
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
        if (id == -1) {
            return null;
        }
        return new PostgresRawdataMessage(new PostgresRawdataMessageId(topic, id, opaqueId), new PostgresRawdataMessageContent(opaqueId, contentMap));
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
            PostgresRawdataMessage message = findMessageContentOfIdAfterPosition(position.get());
            while (message == null) {
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                condition.await(Math.min(durationNano, pollIntervalNanos), TimeUnit.NANOSECONDS);
                message = findMessageContentOfIdAfterPosition(position.get());
            }
            position.set(message.id());
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
