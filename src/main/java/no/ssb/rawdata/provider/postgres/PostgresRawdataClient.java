package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;

public class PostgresRawdataClient implements RawdataClient {

    @Override
    public RawdataProducer producer(String topic) {
        return null;
    }

    @Override
    public RawdataConsumer consumer(String topic, String subscription) {
        return null;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
