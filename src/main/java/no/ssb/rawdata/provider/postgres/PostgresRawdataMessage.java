package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;

public class PostgresRawdataMessage implements RawdataMessage {

    @Override
    public RawdataMessageId id() {
        return null;
    }

    @Override
    public RawdataMessageContent content() {
        return null;
    }
}
