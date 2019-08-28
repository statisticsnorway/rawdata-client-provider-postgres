package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class PostgresRawdataProducer implements RawdataProducer {

    private final TransactionFactory transactionFactory;
    private final String topic;
    private final Map<String, PostgresRawdataMessageContent> buffer = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    PostgresRawdataProducer(TransactionFactory transactionFactory, String topic) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String lastPosition() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT opaque_id FROM \"%s_positions\" ORDER BY id DESC LIMIT 1", topic));
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

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new RawdataMessage.Builder() {
            String position;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessage.Builder position(String position) {
                this.position = position;
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public PostgresRawdataMessageContent build() {
                return new PostgresRawdataMessageContent(position, data);
            }
        };
    }

    @Override
    public RawdataMessage buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public RawdataMessage buffer(RawdataMessage message) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(message.position(), (PostgresRawdataMessageContent) message);
        return message;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataContentNotBufferedException {
        for (String opaqueId : positions) {
            if (!buffer.containsKey(opaqueId)) {
                throw new RawdataContentNotBufferedException(String.format("opaqueId %s is not in buffer", opaqueId));
            }
        }
        Map<String, Long> idByOpaqueId = new LinkedHashMap<>();
        try (Transaction tx = transactionFactory.createTransaction(false)) {

            PreparedStatement positionUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_positions\" (opaque_id, ts) VALUES (?, ?)", topic), new String[]{"id"});
            for (String opaqueId : positions) {
                positionUpdate.setString(1, opaqueId);
                positionUpdate.setTimestamp(2, Timestamp.from(ZonedDateTime.now().toInstant()));
                positionUpdate.executeUpdate();
                ResultSet rs = positionUpdate.getGeneratedKeys();
                if (rs.next()) {
                    idByOpaqueId.put(opaqueId, rs.getLong(1));
                }
            }

            PreparedStatement contentUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_content\" (position_fk_id, name, data) VALUES (?, ?, ?)", topic));
            for (String opaqueId : positions) {
                long id = idByOpaqueId.get(opaqueId);
                PostgresRawdataMessageContent content = buffer.get(opaqueId);
                for (String contentKey : content.keys()) {
                    contentUpdate.setLong(1, id);
                    contentUpdate.setString(2, contentKey);
                    contentUpdate.setBytes(3, content.get(contentKey));
                    contentUpdate.addBatch();
                }
            }
            contentUpdate.executeBatch();

        } catch (SQLException e) {
            throw new PersistenceException(e);
        }

        // remove from buffer after successful database transaction
        for (String opaqueId : positions) {
            buffer.remove(opaqueId);
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        return CompletableFuture.runAsync(() -> publish(positions));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
