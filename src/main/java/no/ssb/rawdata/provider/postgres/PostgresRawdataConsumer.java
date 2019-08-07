package no.ssb.rawdata.provider.postgres;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMessageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class PostgresRawdataConsumer implements RawdataConsumer {

    final PostgresRawdataTopic topic;
    final String subscription;
    final AtomicReference<PostgresRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    PostgresRawdataConsumer(PostgresRawdataTopic topic, String subscription, PostgresRawdataMessageId initialPosition) {
        this.topic = topic;
        this.subscription = subscription;
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            if (!topic.isLegalPosition(initialPosition)) {
                throw new IllegalArgumentException(String.format("the provided initial position %s is not legal in topic %s", initialPosition.index, topic.topic));
            }
        } finally {
            topic.unlock();
        }
        this.position.set(initialPosition);
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public String subscription() {
        return subscription;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit timeUnit) throws InterruptedException, RawdataClosedException {
        long expireTimeNano = System.nanoTime() + timeUnit.toNanos(timeout);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            while (!topic.hasNext(position.get())) {
                if (isClosed()) {
                    throw new RawdataClosedException();
                }
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                topic.awaitProduction(durationNano, TimeUnit.NANOSECONDS);
            }
            PostgresRawdataMessage message = topic.readNext(position.get());
            position.set(message.id()); // auto-acknowledge
            return message;
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<? extends RawdataMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        // do nothing due to auto-acknowledge feature of receive()
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        if (topic.tryLock()) {
            try {
                topic.signalProduction();
            } finally {
                topic.unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "PostgresRawdataConsumer{" +
                "topic=" + topic +
                ", subscription='" + subscription + '\'' +
                ", position=" + position +
                ", closed=" + closed +
                '}';
    }
}
