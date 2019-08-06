package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.rawdata.provider.postgres.tx.PostgresTransactionFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresRawdataClient implements RawdataClient {

    private final PostgresTransactionFactory transactionFactory;
    final AtomicBoolean closed = new AtomicBoolean(false);

    public PostgresRawdataClient(PostgresTransactionFactory transactionFactory) {
        this.transactionFactory = transactionFactory;
    }

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
        return closed.get();
    }

    @Override
    public void close() throws Exception {

        transactionFactory.close();
    }
}
