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
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class PostgresRawdataProducer implements RawdataProducer {

    private final TransactionFactory transactionFactory;
    private final String topic;
    private final Map<String, PostgresRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();
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
        return new PostgresRawdataMessage.Builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        PostgresRawdataMessage.Builder builder = (PostgresRawdataMessage.Builder) _builder;
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }

        try (Transaction tx = transactionFactory.createTransaction(false)) {

            PreparedStatement positionUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_positions\" (ulid, ordering_group, sequence_number, position, ts) VALUES (?, ?, ?, ?, ?)", topic));

            PreparedStatement contentUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_content\" (ulid, name, data) VALUES (?, ?, ?)", topic));

            for (String position : positions) {

                PostgresRawdataMessage.Builder builder = buffer.get(position);

                ULID.Value id = getOrGenerateNextUlid(builder);
                UUID uuid = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());

                /*
                 * position
                 */
                positionUpdate.setObject(1, uuid);
                positionUpdate.setString(2, builder.orderingGroup);
                positionUpdate.setLong(3, builder.sequenceNumber);
                positionUpdate.setString(4, position);
                positionUpdate.setTimestamp(5, Timestamp.from(new Date(id.timestamp()).toInstant()));
                positionUpdate.addBatch();

                /*
                 * content
                 */

                for (Map.Entry<String, byte[]> entry : builder.data.entrySet()) {
                    contentUpdate.setObject(1, uuid);
                    contentUpdate.setString(2, entry.getKey());
                    contentUpdate.setBytes(3, entry.getValue());
                    contentUpdate.addBatch();
                }
            }

            positionUpdate.executeBatch();

            contentUpdate.executeBatch();

        } catch (SQLException e) {
            throw new PersistenceException(e);
        }

        // remove from buffer after successful database transaction
        for (String position : positions) {
            buffer.remove(position);
        }
    }

    private ULID.Value getOrGenerateNextUlid(PostgresRawdataMessage.Builder builder) {
        ULID.Value id = builder.ulid;
        while (id == null) {
            ULID.Value previousUlid = previousIdRef.get();
            ULID.Value attemptedId = RawdataProducer.nextMonotonicUlid(this.ulid, previousUlid);
            if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                id = attemptedId;
            }
        }
        return id;
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
