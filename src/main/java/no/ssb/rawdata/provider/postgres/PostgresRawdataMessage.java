package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;

import java.util.Objects;

public class PostgresRawdataMessage implements RawdataMessage {

    private final PostgresRawdataMessageId id;
    private final PostgresRawdataMessageContent content;

    public PostgresRawdataMessage(PostgresRawdataMessageId id, PostgresRawdataMessageContent content) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        if (content == null) {
            throw new IllegalArgumentException("content cannot be null");
        }
        this.id = id;
        this.content = content;
    }

    @Override
    public RawdataMessageId id() {
        return id;
    }

    @Override
    public RawdataMessageContent content() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PostgresRawdataMessage)) return false;
        PostgresRawdataMessage that = (PostgresRawdataMessage) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, content);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessage{" +
                "id=" + id +
                ", content=" + content +
                '}';
    }
}
