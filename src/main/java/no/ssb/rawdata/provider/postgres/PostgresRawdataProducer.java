package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class PostgresRawdataProducer implements RawdataProducer {

    private final TransactionFactory transactionFactory;
    private final String topic;
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
    public void publish(RawdataMessage... messages) throws RawdataClosedException {
        try (Transaction tx = transactionFactory.createTransaction(false)) {

            try (PreparedStatement positionUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_positions\" (ulid, ordering_group, sequence_number, position, ts) VALUES (?, ?, ?, ?, ?)", topic))) {

                try (PreparedStatement contentUpdate = tx.connection().prepareStatement(String.format("INSERT INTO \"%s_content\" (ulid, name, data) VALUES (?, ?, ?)", topic))) {

                    for (RawdataMessage message : messages) {

                        ULID.Value id = getOrGenerateNextUlid(message);
                        UUID uuid = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());

                        /*
                         * position
                         */
                        positionUpdate.setObject(1, uuid);
                        positionUpdate.setString(2, message.orderingGroup());
                        positionUpdate.setLong(3, message.sequenceNumber());
                        positionUpdate.setString(4, message.position());
                        positionUpdate.setTimestamp(5, Timestamp.from(new Date(id.timestamp()).toInstant()));
                        positionUpdate.addBatch();

                        /*
                         * content
                         */

                        for (Map.Entry<String, byte[]> entry : message.data().entrySet()) {
                            contentUpdate.setObject(1, uuid);
                            contentUpdate.setString(2, entry.getKey());
                            contentUpdate.setBytes(3, entry.getValue());
                            contentUpdate.addBatch();
                        }
                    }

                    positionUpdate.executeBatch();

                    contentUpdate.executeBatch();
                }
            }

        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    private ULID.Value getOrGenerateNextUlid(RawdataMessage message) {
        ULID.Value id = message.ulid();
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
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        return CompletableFuture.runAsync(() -> publish(messages));
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
