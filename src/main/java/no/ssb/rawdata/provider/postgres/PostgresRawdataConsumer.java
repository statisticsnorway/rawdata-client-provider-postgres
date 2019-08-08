package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageId;

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

    final PostgresTransactionFactory transactionFactory;
    final String topic;
    final String subscription;
    final AtomicReference<PostgresRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Lock pollLock = new ReentrantLock();
    final Condition condition = pollLock.newCondition();

    public PostgresRawdataConsumer(PostgresTransactionFactory transactionFactory, String topic, String subscription) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
        this.subscription = subscription;
        Long lastAckedPositionOfSubscription = getLastAckedPositionOfSubscription();
        if (lastAckedPositionOfSubscription == null) {
            // create a new subscription
            createNewPersistentSubscription(-1);
            position.set(new PostgresRawdataMessageId(topic, -1, null));
        } else {
            // initialize from most recent ack checkpoint
            String lastAckedOpaqueId = getOpaqueIdOfId(lastAckedPositionOfSubscription);
            position.set(new PostgresRawdataMessageId(topic, lastAckedPositionOfSubscription, lastAckedOpaqueId));
        }
    }

    Long getLastAckedPositionOfSubscription() {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT position FROM subscription WHERE topic = ? AND subscription = ?");
                ps.setString(1, topic);
                ps.setString(2, subscription);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return null; // no subscription
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    void createNewPersistentSubscription(long initialPosition) {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("INSERT INTO subscription (topic, subscription, position) VALUES (?, ?, ?)");
                ps.setString(1, topic);
                ps.setString(2, subscription);
                ps.setLong(3, initialPosition);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    String getOpaqueIdOfId(long id) {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT opaqueId FROM positions WHERE id = ?");
                ps.setLong(1, id);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null; // no such id exist
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String subscription() {
        return subscription;
    }

    @Override
    public boolean hasMessageAvailable() {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT id FROM positions WHERE id > ? ORDER BY id DESC LIMIT 1");
                ps.setLong(1, position.get().id);
                ResultSet rs = ps.executeQuery();
                return rs.next();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    PostgresRawdataMessage findMessageContentOfIdAfterPosition(PostgresRawdataMessageId currentId) {
        Map<String, byte[]> contentMap = new LinkedHashMap<>();
        long id = -1;
        String opaqueId = null;
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT c.name, c.data, p.id, p.opaque_id FROM content c JOIN (SELECT id, opaque_id FROM positions WHERE topic = ? AND id > ? ORDER BY id LIMIT 1) p ON c.position_fk_id = p.id ORDER BY c.id");
                ps.setString(1, topic);
                ps.setLong(2, currentId.id);
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
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException {
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
            return message;
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
    public void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("UPDATE subscription SET position = ? WHERE topic = ? AND subscription = ?");
                ps.setLong(1, ((PostgresRawdataMessageId) id).id);
                ps.setString(2, topic);
                ps.setString(3, subscription);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
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
                ", subscription='" + subscription + '\'' +
                ", position=" + position.get() +
                ", closed=" + closed +
                '}';
    }
}
