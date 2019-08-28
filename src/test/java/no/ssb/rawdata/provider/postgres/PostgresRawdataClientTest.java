package no.ssb.rawdata.provider.postgres;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PostgresRawdataClientTest {

    static DynamicConfiguration configuration() {
        return new StoreBasedDynamicConfiguration.Builder()
                .values("rawdata.client.provider", "postgres")
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "rdc")
                .values("postgres.driver.password", "rdc")
                .values("postgres.driver.database", "rdc")
                .values("h2.enabled", "false")
                .values("h2.driver.url", "jdbc:h2:mem:rdc;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .build();
    }

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() {
        DynamicConfiguration configuration = configuration();
        client = ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
        assertTrue(client instanceof PostgresRawdataClient);
        dropTables("the-topic");
    }

    private void dropTables(String topic) {
        ((PostgresRawdataClient) client).dropOrCreateDatabase(topic);
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeReadByProducer() {
        RawdataProducer producer = client.producer("the-topic");

        assertEquals(producer.lastPosition(), null);
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        RawdataProducer producer = client.producer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        producer.publish("a", "b");

        assertEquals(producer.lastPosition(), "b");

        producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish("c");

        assertEquals(producer.lastPosition(), "c");
    }

    @Test(expectedExceptions = RawdataContentNotBufferedException.class)
    public void thatPublishNonBufferedMessagesThrowsException() {
        RawdataProducer producer = client.producer("the-topic");
        producer.publish("unbuffered-1");
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.publish(expected1.position());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message, expected1);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        producer.publish(expected1.position());

        RawdataMessage message = future.join();
        assertEquals(message, expected1);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1, expected1);
        assertEquals(message2, expected2);
        assertEquals(message3, expected3);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0), expected1);
        assertEquals(messages.get(1), expected2);
        assertEquals(messages.get(2), expected3);
    }

    private CompletableFuture<List<RawdataMessage>> receiveAsyncAddMessageAndRepeatRecursive(RawdataConsumer consumer, String endPosition, List<RawdataMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.position())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer1 = client.consumer("the-topic");
        RawdataConsumer consumer2 = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<RawdataMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        RawdataMessage expected1 = producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
        RawdataMessage expected2 = producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
        RawdataMessage expected3 = producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
        producer.publish(expected1.position(), expected2.position(), expected3.position());

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0), expected1);
        assertEquals(messages1.get(1), expected2);
        assertEquals(messages1.get(2), expected3);

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0), expected1);
        assertEquals(messages2.get(1), expected2);
        assertEquals(messages2.get(2), expected3);
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d")) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }
}
