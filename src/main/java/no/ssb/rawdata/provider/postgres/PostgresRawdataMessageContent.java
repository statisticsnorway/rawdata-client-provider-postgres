package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessageContent;

import java.util.Arrays;
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
        if (o == null || getClass() != o.getClass()) return false;
        PostgresRawdataMessageContent that = (PostgresRawdataMessageContent) o;
        return externalId.equals(that.externalId) &&
                this.data.keySet().equals(that.data.keySet()) &&
                this.data.keySet().stream().allMatch(key -> Arrays.equals(this.data.get(key), that.data.get(key)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(externalId);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageContent{" +
                "externalId='" + externalId + '\'' +
                ", data=" + data +
                '}';
    }
}
