package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

class PostgresRawdataProducer implements RawdataProducer {

    private final PostgresTransactionFactory transactionFactory;
    private final String topic;
    private final Map<String, PostgresRawdataMessageContent> buffer = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    PostgresRawdataProducer(PostgresTransactionFactory transactionFactory, String topic) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String lastExternalId() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try (PostgresTransaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection.prepareStatement("SELECT opaque_id FROM positions WHERE topic = ? ORDER BY id DESC LIMIT 1");
                ps.setString(1, topic);
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
    public RawdataMessageContent.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new RawdataMessageContent.Builder() {
            String externalId;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessageContent.Builder externalId(String externalId) {
                this.externalId = externalId;
                return this;
            }

            @Override
            public RawdataMessageContent.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public PostgresRawdataMessageContent build() {
                return new PostgresRawdataMessageContent(externalId, data);
            }
        };
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(content.externalId(), (PostgresRawdataMessageContent) content);
        return content;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> opaqueIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        return publish(opaqueIds.toArray(new String[opaqueIds.size()]));
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... opaqueIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        try {
            List<CompletableFuture<? extends RawdataMessageId>> futures = publishAsync(opaqueIds);
            List<PostgresRawdataMessageId> result = new ArrayList<>();
            for (CompletableFuture<? extends RawdataMessageId> future : futures) {
                result.add((PostgresRawdataMessageId) future.join());
            }
            return result;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RawdataContentNotBufferedException) {
                throw (RawdataContentNotBufferedException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    @Override
    public List<CompletableFuture<? extends RawdataMessageId>> publishAsync(String... opaqueIds) {
        for (String opaqueId : opaqueIds) {
            if (!buffer.containsKey(opaqueId)) {
                throw new RawdataContentNotBufferedException(String.format("opaqueId %s is not in buffer", opaqueId));
            }
        }
        Map<String, Long> idByOpaqueId = new LinkedHashMap<>();
        try (PostgresTransaction tx = transactionFactory.createTransaction(false)) {

            PreparedStatement positionUpdate = tx.connection.prepareStatement("INSERT INTO positions (topic, opaque_id, ts) VALUES (?, ?, ?) RETURNING id");
            for (String opaqueId : opaqueIds) {
                positionUpdate.setString(1, topic);
                positionUpdate.setString(2, opaqueId);
                positionUpdate.setTimestamp(3, Timestamp.from(ZonedDateTime.now().toInstant()));
                ResultSet rs = positionUpdate.executeQuery();
                if (rs.next()) {
                    idByOpaqueId.put(opaqueId, rs.getLong(1));
                }
            }

            PreparedStatement contentUpdate = tx.connection.prepareStatement("INSERT INTO content (position_fk_id, name, data) VALUES (?, ?, ?)");
            for (String opaqueId : opaqueIds) {
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
        for (String opaqueId : opaqueIds) {
            buffer.remove(opaqueId);
        }

        List<CompletableFuture<? extends RawdataMessageId>> result = new ArrayList<>();
        for (String opaqueId : opaqueIds) {
            CompletableFuture<PostgresRawdataMessageId> future = CompletableFuture.completedFuture(new PostgresRawdataMessageId(topic, idByOpaqueId.get(opaqueId), opaqueId));
            result.add(future);
        }

        return result;
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
