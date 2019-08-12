package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PostgresRawdataMessageContent implements RawdataMessage {

    private final String position;
    private final Map<String, byte[]> data;

    public PostgresRawdataMessageContent(String position, Map<String, byte[]> data) {
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.position = position;
        this.data = data;
    }

    @Override
    public String position() {
        return position;
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
        return position.equals(that.position) &&
                this.data.keySet().equals(that.data.keySet()) &&
                this.data.keySet().stream().allMatch(key -> Arrays.equals(this.data.get(key), that.data.get(key)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageContent{" +
                "position='" + position + '\'' +
                ", data=" + data +
                '}';
    }
}
