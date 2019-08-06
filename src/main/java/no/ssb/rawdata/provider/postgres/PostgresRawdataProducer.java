package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.List;

public class PostgresRawdataProducer implements RawdataProducer {
    
    @Override
    public String topic() {
        return null;
    }

    @Override
    public String lastExternalId() throws RawdataClosedException {
        return null;
    }

    @Override
    public RawdataMessageContent.Builder builder() throws RawdataClosedException {
        return null;
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException {
        return null;
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException {
        return null;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        return null;
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
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
