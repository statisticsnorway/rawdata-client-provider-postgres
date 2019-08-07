package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresRawdataClient implements RawdataClient {

    final Map<String, PostgresRawdataTopic> topicByName = new ConcurrentHashMap<>();
    final Map<String, Map<String, PostgresRawdataConsumer>> subscriptionByNameByTopic = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final List<PostgresRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<PostgresRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    private final PostgresTransactionFactory transactionFactory;

    public PostgresRawdataClient(PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

    @Override
    public RawdataProducer producer(String topicName) {
        PostgresRawdataProducer producer = new PostgresRawdataProducer(topicByName.computeIfAbsent(topicName, t -> new PostgresRawdataTopic(transactionFactory, t)));
        this.producers.add(producer);
        return producer;

    }

    @Override
    public RawdataConsumer consumer(String topicName, String subscription) {
        PostgresRawdataConsumer consumer = subscriptionByNameByTopic.computeIfAbsent(topicName, tn -> new ConcurrentHashMap<>())
                .computeIfAbsent(subscription, s ->
                        new PostgresRawdataConsumer(topicByName.computeIfAbsent(topicName, t ->
                                new PostgresRawdataTopic(transactionFactory, t)), s, new PostgresRawdataMessageId(topicName, -1L)
                        )
                );
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
