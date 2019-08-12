package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.Transaction;
import no.ssb.rawdata.provider.postgres.tx.TransactionFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresRawdataClient implements RawdataClient {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PostgresRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PostgresRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    private final TransactionFactory transactionFactory;

    public PostgresRawdataClient(TransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    @Override
    public RawdataProducer producer(String topicName) {
        PostgresRawdataProducer producer = new PostgresRawdataProducer(transactionFactory, topicName);
        this.producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topicName, String initialPosition) {
        PostgresRawdataMessageId initialMessageId = findMessageId(topicName, initialPosition);
        PostgresRawdataConsumer consumer = new PostgresRawdataConsumer(transactionFactory, topicName, initialMessageId);
        consumers.add(consumer);
        return consumer;
    }

    PostgresRawdataMessageId findMessageId(String topic, String externalId) {
        try (Transaction tx = transactionFactory.createTransaction(true)) {
            PreparedStatement ps = tx.connection().prepareStatement("SELECT id FROM positions WHERE topic = ? AND opaque_id = ?");
            ps.setString(1, topic);
            ps.setString(2, externalId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                return new PostgresRawdataMessageId(topic, id, externalId);
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
}
