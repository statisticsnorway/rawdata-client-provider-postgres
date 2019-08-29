package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class PostgresUUIDOrderingTest {

    static DynamicConfiguration configuration() {
        return new StoreBasedDynamicConfiguration.Builder()
                .values("rawdata.client.provider", "postgres")
                .values("postgres.driver.host", "localhost")
                .values("postgres.driver.port", "5432")
                .values("postgres.driver.user", "rdc")
                .values("postgres.driver.password", "rdc")
                .values("postgres.driver.database", "rdc")
                .values("h2.enabled", "true")
                .values("h2.driver.url", "jdbc:h2:mem:rdc;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .build();
    }

    PostgresRawdataClient client;

    @BeforeMethod
    public void createRawdataClient() {
        DynamicConfiguration configuration = configuration();
        client = (PostgresRawdataClient) ProviderConfigurator.configure(configuration.asMap(), configuration.evaluateToString("rawdata.client.provider"), RawdataClientInitializer.class);
        dropTables("T1");
    }

    private void dropTables(String topic) {
        client.dropOrCreateDatabase(topic);
    }

    @Test
    public void thatULIDtoUUIDAndBackConversionWorks() {
        ULID ulid = new ULID();
        ULID.Value value = ulid.nextValue();
        convertAndCompare(value);
        convertAndCompare(ulid.nextMonotonicValue(value));
    }

    private void convertAndCompare(ULID.Value value) {
        UUID uuidFromMsbLsb = new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
        ULID.Value actual = new ULID.Value(uuidFromMsbLsb.getMostSignificantBits(), uuidFromMsbLsb.getLeastSignificantBits());
        assertEquals(actual, value);
    }

    @Test
    public void thatUUIDOrderingIsPreservedInDatabase() throws Exception {
        Random random = new Random();
        int N = 10000;
        List<String> expectedPositions = new ArrayList<>(N);
        try (RawdataProducer producer = client.producer("T1")) {
            for (int i = 0; i < N; i++) {
                String position = random.nextInt(N) + "a" + i;
                producer.buffer(producer.builder().position(position).put("payload", new byte[8]));
                expectedPositions.add(position);
            }
            producer.publish(expectedPositions);
        }
        List<String> actualPositions = new ArrayList<>(N);
        try (RawdataConsumer consumer = client.consumer("T1")) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            while (message != null) {
                actualPositions.add(message.position());
                message = consumer.receive(100, TimeUnit.MILLISECONDS);
            }
        }
        assertEquals(actualPositions, expectedPositions);
    }
}
