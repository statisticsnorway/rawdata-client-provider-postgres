package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessageContent;

import java.util.Set;

public class PostgresRawdataMessageContent implements RawdataMessageContent {

    @Override
    public String externalId() {
        return null;
    }

    @Override
    public Set<String> keys() {
        return null;
    }

    @Override
    public byte[] get(String key) {
        return new byte[0];
    }
}
