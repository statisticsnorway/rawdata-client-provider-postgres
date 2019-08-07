package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessageContent;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PostgresRawdataMessageContent implements RawdataMessageContent {

    private final String externalId;
    private final Map<String, byte[]> data;

    public PostgresRawdataMessageContent(String externalId, Map<String, byte[]> data) {
        if (externalId == null) {
            throw new IllegalArgumentException("externalId cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.externalId = externalId;
        this.data = data;
    }

    @Override
    public String externalId() {
        return externalId;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PostgresRawdataMessageContent)) return false;
        PostgresRawdataMessageContent that = (PostgresRawdataMessageContent) o;
        return Objects.equals(externalId, that.externalId) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(externalId, data);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageContent{" +
                "externalId='" + externalId + '\'' +
                ", data=" + data +
                '}';
    }
}
