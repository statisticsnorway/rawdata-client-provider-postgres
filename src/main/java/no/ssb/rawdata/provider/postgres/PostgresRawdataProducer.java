package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class PostgresRawdataProducer implements RawdataProducer {

    private final TransactionFactory transactionFactory;
    private final String topic;
    private final Map<String, PostgresRawdataMessageContent> buffer = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ULID ulid = new ULID();
    private final AtomicReference<ULID.Value> previousIdRef = new AtomicReference<>(ulid.nextValue());

    PostgresRawdataProducer(TransactionFactory transactionFactory, String topic) {
        this.transactionFactory = transactionFactory;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
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
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        for (String opaqueId : positions) {
            if (!buffer.containsKey(opaqueId)) {
                throw new RawdataNotBufferedException(String.format("opaqueId %s is not in buffer", opaqueId));
            }
        }
        Map<String, ULID.Value> ulidByPosition = new LinkedHashMap<>();
        for (String position : positions) {
            ULID.Value id = null;
            do {
                ULID.Value previousUlid = previousIdRef.get();
                ULID.Value attemptedId = ulid.nextStrictlyMonotonicValue(previousUlid).orElseThrow();
                if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                    id = attemptedId;
                }
            } while (id == null);
            ulidByPosition.put(position, id);
        }

        Map<String, UUID> idByOpaqueId = new LinkedHashMap<>();
        try (Transaction tx = transactionFactory.createTransaction(false)) {

            PreparedStatement positionUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_positions\" (ulid, opaque_id, ts) VALUES (?, ?, ?)", topic));
            for (String opaqueId : positions) {
                ULID.Value ulidValue = ulidByPosition.get(opaqueId);
                UUID uuid = UUID.nameUUIDFromBytes(ulidValue.toBytes());
                idByOpaqueId.put(opaqueId, uuid);
                positionUpdate.setObject(1, uuid);
                positionUpdate.setString(2, opaqueId);
                positionUpdate.setTimestamp(3, Timestamp.from(ZonedDateTime.now().toInstant()));
                positionUpdate.addBatch();
            }
            positionUpdate.executeBatch();

            PreparedStatement contentUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_content\" (position_fk_ulid, name, data) VALUES (?, ?, ?)", topic));
            for (String opaqueId : positions) {
                UUID uuid = idByOpaqueId.get(opaqueId);
                PostgresRawdataMessageContent content = buffer.get(opaqueId);
                for (String contentKey : content.keys()) {
                    contentUpdate.setObject(1, uuid);
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
