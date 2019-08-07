package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataMessageId;

import java.util.Objects;

class PostgresRawdataMessageId implements RawdataMessageId {
    final String topic;
    final int index;

    PostgresRawdataMessageId(String topic, int index) {
        this.topic = topic;
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PostgresRawdataMessageId)) return false;
        PostgresRawdataMessageId that = (PostgresRawdataMessageId) o;
        return index == that.index &&
                Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, index);
    }

    @Override
    public String toString() {
        return "PostgresRawdataMessageId{" +
                "topic='" + topic + '\'' +
                ", index=" + index +
                '}';
    }
}
