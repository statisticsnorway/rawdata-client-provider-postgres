package no.ssb.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Objects;

class PostgresRawdataMessageId {
    final String topic;
    final ULID.Value id;
    final String opaqueId;

    PostgresRawdataMessageId(String topic, ULID.Value id, String opaqueId) {
        this.topic = topic;
        this.id = id;
        this.opaqueId = opaqueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PostgresRawdataMessageId)) return false;
        PostgresRawdataMessageId that = (PostgresRawdataMessageId) o;
        return id == that.id &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, id);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageId{" +
                "topic='" + topic + '\'' +
                ", id=" + id +
                ", opaqueId='" + opaqueId + '\'' +
                '}';
    }
}
