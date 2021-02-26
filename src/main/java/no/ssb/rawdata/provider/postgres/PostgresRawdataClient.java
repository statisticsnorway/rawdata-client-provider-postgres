package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

class PostgresRawdataClient implements RawdataClient {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PostgresRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PostgresRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    final TransactionFactory transactionFactory;
    final int consumerPrefetchSize;
    final int dbPrefetchPollIntervalWhenEmptyMilliseconds;

    PostgresRawdataClient(TransactionFactory transactionFactory, int consumerPrefetchSize, int dbPrefetchPollIntervalWhenEmptyMilliseconds) {
        this.transactionFactory = transactionFactory;
        this.consumerPrefetchSize = consumerPrefetchSize;
        this.dbPrefetchPollIntervalWhenEmptyMilliseconds = dbPrefetchPollIntervalWhenEmptyMilliseconds;
    }

    @Override
    public RawdataProducer producer(String topicName) {
        createTopicIfNotExists(topicName);
        PostgresRawdataProducer producer = new PostgresRawdataProducer(transactionFactory, topicName);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        createTopicIfNotExists(topic);
        PostgresRawdataConsumer consumer = new PostgresRawdataConsumer(transactionFactory, topic, (PostgresCursor) cursor, consumerPrefetchSize, dbPrefetchPollIntervalWhenEmptyMilliseconds);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new PostgresCursor(ulid, inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        ULID.Value lowerUlid = RawdataConsumer.beginningOf(approxTimestamp - tolerance.toMillis());
        ULID.Value upperUlid = RawdataConsumer.beginningOf(approxTimestamp + tolerance.toMillis());
        UUID lowerBound = new UUID(lowerUlid.getMostSignificantBits(), lowerUlid.getLeastSignificantBits());
        UUID upperBound = new UUID(upperUlid.getMostSignificantBits(), upperUlid.getLeastSignificantBits());

        try (Transaction tx = transactionFactory.createTransaction(true)) {
            try (PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT ulid FROM \"%s_positions\" WHERE position = ? AND ? <= ulid AND ulid < ?", topic))) {
                ps.setString(1, position);
                ps.setObject(2, lowerBound);
                ps.setObject(3, upperBound);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        UUID uuid = (UUID) rs.getObject(1);
                        ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                        return new PostgresCursor(ulid, inclusive);
                    }
                    throw new RawdataNoSuchPositionException("Position not found: " + position);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        createTopicIfNotExists(topic);
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            ULID.Value ulid = null;
            String orderingGroup = null;
            long sequence = 0;
            String position = null;
            Map<String, byte[]> contentMap = new LinkedHashMap<>();
            String sql = String.format(
                    "SELECT p.ulid, p.ordering_group, p.sequence_number, p.position, c.name, c.data " +
                            "FROM (SELECT ulid, ordering_group, sequence_number, position FROM \"%s_positions\" ORDER BY ulid DESC LIMIT 1) p " +
                            "LEFT JOIN \"%s_content\" c ON p.ulid = c.ulid",
                    topic, topic);
            try (PreparedStatement ps = tx.connection().prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        UUID uuid = (UUID) rs.getObject(1);
                        ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                        orderingGroup = rs.getString(2);
                        sequence = rs.getLong(3);
                        position = rs.getString(4);
                        String name = rs.getString(5);
                        byte[] data = rs.getBytes(6);
                        contentMap.put(name, data);
                    }
                    if (ulid == null) {
                        return null;
                    }
                    return RawdataMessage.builder()
                            .ulid(ulid)
                            .orderingGroup(orderingGroup)
                            .sequenceNumber(sequence)
                            .position(position)
                            .data(contentMap)
                            .build();
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }


    void createTopicIfNotExists(String topic) {
        if (!transactionFactory.checkIfTableTopicExists(topic, "positions") || !transactionFactory.checkIfTableTopicExists(topic, "content")) {
            dropOrCreateTopicTables(topic, "no/ssb/rawdata/provider/postgres/init/init-topic-stream.sql");
        }
    }

    void createTopicMetadataIfNotExists(String topic) {
        if (!transactionFactory.checkIfTableTopicExists(topic, "metadata")) {
            dropOrCreateTopicTables(topic, "no/ssb/rawdata/provider/postgres/init/init-topic-metadata.sql");
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public PostgresRawdataMetadataClient metadata(String topic) {
        createTopicMetadataIfNotExists(topic);
        return new PostgresRawdataMetadataClient(topic, transactionFactory);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return; // already closed
        }
        for (PostgresRawdataProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        for (PostgresRawdataConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        transactionFactory.close();
    }

    void dropOrCreateTopicTables(String topic, String sqlResource) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource(sqlResource);
            try (Connection conn = transactionFactory.dataSource().getConnection()) {
                conn.beginRequest();

                try (Scanner s = new Scanner(initSQL.replaceAll("TOPIC", topic))) {
                    s.useDelimiter("(;(\r)?\n)|(--\n)");
                    try (Statement st = conn.createStatement()) {
                        while (s.hasNext()) {
                            String line = s.next();
                            if (line.startsWith("/*!") && line.endsWith("*/")) {
                                int i = line.indexOf(' ');
                                line = line.substring(i + 1, line.length() - " */".length());
                            }

                            if (line.trim().length() > 0) {
                                st.execute(line);
                            }
                        }
                        conn.commit();
                    }
                }

                conn.endRequest();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
