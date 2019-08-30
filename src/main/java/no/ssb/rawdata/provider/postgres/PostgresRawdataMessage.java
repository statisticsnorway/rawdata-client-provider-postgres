package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class PostgresRawdataMessage implements RawdataMessage {

    private final ULID.Value ulid;
    private final String position;
    private final Map<String, byte[]> data;

    PostgresRawdataMessage(ULID.Value ulid, String position, Map<String, byte[]> data) {
        this.ulid = ulid;
        this.position = position;
        this.data = data;
    }

    @Override
    public ULID.Value ulid() {
        return ulid;
    }

    @Override
    public long sequenceNumber() {
        return 0;
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
        PostgresRawdataMessage that = (PostgresRawdataMessage) o;
        return ulid.equals(that.ulid) &&
                position.equals(that.position) &&
                this.data.keySet().equals(that.data.keySet()) &&
                this.data.keySet().stream().allMatch(key -> Arrays.equals(this.data.get(key), that.data.get(key)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(ulid, position);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageContent{" +
                "ulid=" + ulid +
                ", position='" + position + '\'' +
                ", data.keys=" + data.keySet() +
                '}';
    }

    static class Builder implements RawdataMessage.Builder {
        ULID.Value ulid;
        String position;
        Map<String, byte[]> data = new LinkedHashMap<>();

        @Override
        public RawdataMessage.Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public RawdataMessage.Builder position(String position) {
            this.position = position;
            return this;
        }

        @Override
        public RawdataMessage.Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
        }

        @Override
        public RawdataMessage build() {
            if (ulid == null) {
                throw new IllegalArgumentException("ulid cannot be null");
            }
            if (position == null) {
                throw new IllegalArgumentException("position cannot be null");
            }
            if (data == null) {
                throw new IllegalArgumentException("data cannot be null");
            }
            return new PostgresRawdataMessage(ulid, position, data);
        }
    }
}
