package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresRawdataClient implements RawdataClient {

    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PostgresRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PostgresRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    private final PostgresTransactionFactory transactionFactory;

    public PostgresRawdataClient(PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    @Override
    public RawdataProducer producer(String topicName) {
        PostgresRawdataProducer producer = new PostgresRawdataProducer(transactionFactory, topicName);
        this.producers.add(producer);
        return producer;

    }

    @Override
    public RawdataConsumer consumer(String topicName, String subscription) {
        PostgresRawdataConsumer consumer = new PostgresRawdataConsumer(transactionFactory, topicName, subscription);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
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
