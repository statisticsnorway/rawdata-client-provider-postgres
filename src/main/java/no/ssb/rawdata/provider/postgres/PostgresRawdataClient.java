package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresRawdataClient implements RawdataClient {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PostgresRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PostgresRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    final TransactionFactory transactionFactory;

    PostgresRawdataClient(TransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    @Override
    public RawdataProducer producer(String topicName) {
        createTopicIfNotExists(topicName);
        PostgresRawdataProducer producer = new PostgresRawdataProducer(transactionFactory, topicName);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topicName, String initialPosition) {
        createTopicIfNotExists(topicName);
        PostgresRawdataMessageId initialMessageId = findMessageId(topicName, initialPosition);
        PostgresRawdataConsumer consumer = new PostgresRawdataConsumer(transactionFactory, topicName, initialMessageId);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public String lastPosition(String topic) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            try {
                PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT opaque_id FROM \"%s_positions\" ORDER BY ulid DESC LIMIT 1", topic));
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


    void createTopicIfNotExists(String topic) {
        if (!transactionFactory.checkIfTableTopicExists(topic, "positions") || !transactionFactory.checkIfTableTopicExists(topic, "content")) {
            dropOrCreateDatabase(topic);
        }
    }

    PostgresRawdataMessageId findMessageId(String topic, String position) {
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            PreparedStatement ps = tx.connection().prepareStatement(String.format("SELECT ulid FROM \"%s_positions\" WHERE opaque_id = ?", topic));
            ps.setString(1, position);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                UUID uuid = (UUID) rs.getObject(1);
                ULID.Value ulid = new ULID.Value(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                return new PostgresRawdataMessageId(topic, ulid, position);
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
