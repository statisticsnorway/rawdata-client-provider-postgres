package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive) {
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT ulid FROM \"%s_positions\" WHERE opaque_id = ?", topic));
            ps.setString(1, position);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                UUID uuid = (UUID) rs.getObject(1);
                ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                return new PostgresCursor(ulid, inclusive);
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            ULID.Value ulid = null;
            String opaqueId = null;
            Map<String, byte[]> contentMap = new LinkedHashMap<>();
            String sql = String.format(
                    "SELECT p.ulid, p.opaque_id, c.name, c.data " +
                            "FROM (SELECT ulid, opaque_id FROM \"%s_positions\" ORDER BY ulid DESC LIMIT 1) p " +
                            "LEFT JOIN \"%s_content\" c ON p.ulid = c.position_fk_ulid",
                    topic, topic);
            PreparedStatement ps = tx.connection().prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                UUID uuid = (UUID) rs.getObject(1);
                ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                opaqueId = rs.getString(2);
                String name = rs.getString(3);
                byte[] data = rs.getBytes(4);
                contentMap.put(name, data);
            }
            if (ulid == null) {
                return null;
            }
            return new PostgresRawdataMessage(ulid, opaqueId, contentMap);
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }


    void createTopicIfNotExists(String topic) {
        if (!transactionFactory.checkIfTableTopicExists(topic, "positions") || !transactionFactory.checkIfTableTopicExists(topic, "content")) {
            dropOrCreateDatabase(topic);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        for (PostgresRawdataProducer producer : producers) {
            producer.close();
        }
        producers.clear();
        for (PostgresRawdataConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        transactionFactory.close();
        closed.set(true);
    }

    void dropOrCreateDatabase(String topic) {
        try {
            String initSQL = FileAndClasspathReaderUtils.readFileOrClasspathResource("postgres/init-db.sql");
            Connection conn = transactionFactory.dataSource().getConnection();
            conn.beginRequest();

            try (Scanner s = new Scanner(initSQL.replaceAll("TOPIC", topic))) {
                s.useDelimiter("(;(\r)?\n)|(--\n)");
                try (Statement st = conn.createStatement()) {
                    try {
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
                    } finally {
                        st.close();
                    }
                }
            }

            conn.endRequest();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
