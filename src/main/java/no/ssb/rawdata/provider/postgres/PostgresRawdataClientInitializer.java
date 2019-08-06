package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.util.Map;
import java.util.Set;

@ProviderName("postgres")
public class PostgresRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "postgres";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "postgres.driver.host",
                "postgres.driver.port",
                "postgres.driver.user",
                "postgres.driver.password",
                "postgres.driver.database"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> map) {
        return new PostgresRawdataClient();
    }
}
