package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PostgresRawdataConsumer implements RawdataConsumer {

    @Override
    public String topic() {
        return null;
    }

    @Override
    public String subscription() {
        return null;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit timeUnit) throws InterruptedException, RawdataClosedException {
        return null;
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return null;
    }

    @Override
    public void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException {

    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
