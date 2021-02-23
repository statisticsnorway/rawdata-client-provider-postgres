package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PostgresRawdataClientTck {

    static DynamicConfiguration configuration() {
        return new StoreBasedDynamicConfiguration.Builder()
                .values("rawdata.client.provider", "postgres")
                .values("rawdata.postgres.consumer.prefetch-size", "1")
                .values("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "1000")
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "rdc")
                .values("postgres.driver.password", "rdc")
                .values("postgres.driver.database", "rdc")
                .values("h2.enabled", "true")
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
        ((PostgresRawdataClient) client).dropOrCreateTopicTables(topic, "no/ssb/rawdata/provider/postgres/init/init-topic-stream.sql");
        ((PostgresRawdataClient) client).dropOrCreateTopicTables(topic, "no/ssb/rawdata/provider/postgres/init/init-topic-metadata.sql");
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeRead() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        RawdataProducer producer = client.producer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.publish("a", "b");

        assertEquals(client.lastMessage("the-topic").position(), "b");

        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("c");

        assertEquals(client.lastMessage("the-topic").position(), "c");
    }

    @Test(expectedExceptions = RawdataNotBufferedException.class)
    public void thatPublishNonBufferedMessagesThrowsException() {
        RawdataProducer producer = client.producer("the-topic");
        producer.publish("unbuffered-1");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        ULID.Value ulid = new ULID().nextValue();
        producer.buffer(producer.builder().ulid(ulid).orderingGroup("og1").sequenceNumber(1).position("a").put("payload1", new byte[3]).put("payload2", new byte[7]));
        producer.publish("a");

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.ulid(), ulid);
        assertEquals(message.orderingGroup(), "og1");
        assertEquals(message.sequenceNumber(), 1);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
        assertEquals(message.get("payload1"), new byte[3]);
        assertEquals(message.get("payload2"), new byte[7]);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = future.join();
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1.position(), "a");
        assertEquals(message2.position(), "b");
        assertEquals(message3.position(), "c");
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0).position(), "a");
        assertEquals(messages.get(1).position(), "b");
        assertEquals(messages.get(2).position(), "c");
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

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0).position(), "a");
        assertEquals(messages1.get(1).position(), "b");
        assertEquals(messages1.get(2).position(), "c");

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0).position(), "a");
        assertEquals(messages2.get(1).position(), "b");
        assertEquals(messages2.get(2).position(), "c");
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
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
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            timestampBeforeA = System.currentTimeMillis();
            producer.publish("a");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            timestampBeforeB = System.currentTimeMillis();
            producer.publish("b");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeC = System.currentTimeMillis();
            producer.publish("c");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeD = System.currentTimeMillis();
            producer.publish("d");
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "a");
        }
    }

    @Test
    public void thatPositionCursorOfValidPositionIsFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c");
        }
        assertNotNull(client.cursorOf("the-topic", "a", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "b", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfInvalidPositionIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c");
        }
        assertNull(client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfEmptyTopicIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
        }
        client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1));
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        RawdataMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put("key-2", "Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-1"), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Value-2");
        metadata.put("key-2", "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove("key-1");
        assertEquals(metadata.keys().size(), 1);
        metadata.remove("key-2");
        assertEquals(metadata.keys().size(), 0);
    }
}
