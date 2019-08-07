package no.ssb.rawdata.provider.postgres;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class PostgresRawdataTopic {

    final String topic;
    final ReentrantLock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();
    private final PostgresTransactionFactory transactionFactory;

    PostgresRawdataTopic(PostgresTransactionFactory transactionFactory, String topic) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
    }

    private Long findLastMessageId() {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT id FROM positions WHERE topic = ? ORDER BY id DESC LIMIT 1");
                ps.setString(1, topic);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return -1L;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    private String findExternalId(PostgresRawdataMessageId id) {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT opaque_id FROM positions WHERE topic = ? AND id = ? ORDER BY id DESC LIMIT 1");
                ps.setString(1, topic);
                ps.setLong(2, id.index);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    private Map<String, byte[]> findMessageContentById(PostgresRawdataMessageId id) {
        Map<String, byte[]> contentMap = new LinkedHashMap<>();
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT name, data FROM content WHERE position_fk_id = ? ORDER BY id");
                ps.setLong(1, id.index);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    String name = rs.getString(1);
                    byte[] data = rs.getBytes(2);
                    contentMap.put(name, data);
                }
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
        return contentMap;
    }

    private Long findNextMessageId(PostgresRawdataMessageId id) {
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT id FROM positions WHERE id > ? AND topic = ? ORDER BY id DESC LIMIT 1");
                ps.setLong(1, id.index);
                ps.setString(2, topic);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return -1L;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }


    private Long persistMessageContent(PostgresRawdataMessageContent content) {
        try (PostgresTransaction tx = transactionFactory.createTransaction(false)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("INSERT INTO positions (topic, opaque_id, ts) VALUES (?, ?, ?) RETURNING id");
                ps.setString(1, topic);
                ps.setString(2, content.externalId());
                ps.setTimestamp(3, Timestamp.from(ZonedDateTime.now().toInstant()));
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    long id = rs.getLong(1);

                    PreparedStatement psNested = tx.connection.prepareStatement("INSERT INTO content (position_fk_id, name, data) VALUES (?, ?, ?)");
                    int n = 1;
                    for (String contentKey : content.keys()) {
                        psNested.setLong(1, id);
                        psNested.setString(2, contentKey);
                        psNested.setBytes(3, content.get(contentKey));
                        psNested.addBatch();
                        n++;
                    }
                    return psNested.executeBatch().length == n - 1 ? id : null;
                }
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
        return null;
    }

    PostgresRawdataMessageId lastMessageId() {
        checkHasLock();
        Long lastMessageId = findLastMessageId();
        if (lastMessageId == -1L) {
            return null;
        }
        return new PostgresRawdataMessageId(topic, lastMessageId);
    }

    private void checkHasLock() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("The calling thread must hold the lock before calling this method");
        }
    }

    PostgresRawdataMessageId write(PostgresRawdataMessageContent content) {
        checkHasLock();
        Long messageId = persistMessageContent(content);
        PostgresRawdataMessageId id = new PostgresRawdataMessageId(topic, messageId);
        signalProduction();
        return id;
    }

    boolean hasNext(PostgresRawdataMessageId id) {
        checkHasLock();
        if (id.index > findLastMessageId()) {
            return false;
        }
        return true;
    }

    PostgresRawdataMessage readNext(PostgresRawdataMessageId id) {
        checkHasLock();
        return read(new PostgresRawdataMessageId(topic, findNextMessageId(id)));
    }

    public PostgresRawdataMessage read(PostgresRawdataMessageId id) {
        checkHasLock();
        Map<String, byte[]> contentMap = findMessageContentById(id);
        return new PostgresRawdataMessage(id, new PostgresRawdataMessageContent(findExternalId(id), contentMap));
    }

    boolean isLegalPosition(PostgresRawdataMessageId id) {
        long nextIndex = findNextMessageId(id);
        if (nextIndex == -1L) {
            return true;
        }
        if (nextIndex > id.index) {
            return false;
        }
        return true;
    }

    boolean tryLock() {
        return lock.tryLock();
    }

    void tryLock(int timeout, TimeUnit unit) {
        try {
            if (!lock.tryLock(timeout, unit)) {
                throw new RuntimeException("timeout while waiting for lock");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    void unlock() {
        lock.unlock();
    }

    void awaitProduction(long duration, TimeUnit unit) throws InterruptedException {
        condition.await(duration, unit);
    }

    void signalProduction() {
        condition.signalAll();
    }

    @Override
    public String toString() {
        return "PostgresRawdataTopic{" +
                "topic='" + topic + '\'' +
                '}';
    }
}
