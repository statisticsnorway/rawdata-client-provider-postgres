package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessageContent;
import no.ssb.rawdata.api.RawdataMessageId;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class PostgresRawdataProducer implements RawdataProducer {

    private final PostgresRawdataTopic topic;
    private final Map<String, PostgresRawdataMessageContent> buffer = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    PostgresRawdataProducer(PostgresRawdataTopic topic) {
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public String lastExternalId() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            PostgresRawdataMessageId lastMessageId = topic.lastMessageId();
            if (lastMessageId == null) {
                return null;
            }
            return topic.read(lastMessageId).content().externalId();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public RawdataMessageContent.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new RawdataMessageContent.Builder() {
            String externalId;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessageContent.Builder externalId(String externalId) {
                this.externalId = externalId;
                return this;
            }

            @Override
            public RawdataMessageContent.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public PostgresRawdataMessageContent build() {
                return new PostgresRawdataMessageContent(externalId, data);
            }
        };
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent.Builder builder) throws RawdataClosedException {
        return buffer(builder.build());
    }

    @Override
    public RawdataMessageContent buffer(RawdataMessageContent content) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(content.externalId(), (PostgresRawdataMessageContent) content);
        return content;
    }

    @Override
    public List<? extends RawdataMessageId> publish(List<String> externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        return publish(externalIds.toArray(new String[externalIds.size()]));
    }

    @Override
    public List<? extends RawdataMessageId> publish(String... externalIds) throws RawdataClosedException, RawdataContentNotBufferedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            List<PostgresRawdataMessageId> messageIds = new ArrayList<>();
            for (String externalId : externalIds) {
                PostgresRawdataMessageContent content = buffer.remove(externalId);
                if (content == null) {
                    throw new RawdataContentNotBufferedException(String.format("externalId %s has not been buffered", externalId));
                }
                PostgresRawdataMessageId messageId = topic.write(content);
                messageIds.add(messageId);
            }
            return messageIds;
        } finally {
            topic.unlock();
        }
    }

    @Override
    public List<CompletableFuture<? extends RawdataMessageId>> publishAsync(String... externalIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
    }
}
