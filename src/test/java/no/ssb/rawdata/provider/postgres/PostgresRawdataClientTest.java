package no.ssb.rawdata.provider.postgres;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PostgresRawdataClientTest {

    static DynamicConfiguration configuration() {
        return new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("application-test.properties")
                .build();
    }

    @Test
    public void thatRawdataClientIsAvailableThroughServiceProviderMechanism() {
        RawdataClient client = ProviderConfigurator.configure(configuration().asMap(), "postgres", RawdataClientInitializer.class);
        assertNotNull(client);
        assertTrue(client instanceof PostgresRawdataClient);

    }

}
