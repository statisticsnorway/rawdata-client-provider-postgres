package no.ssb.rawdata.provider.postgres;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionStatistics {
    final Map<String, AtomicLong> statistics = new ConcurrentHashMap<>();

    public TransactionStatistics add(String statistic, int increment) {
        statistics.computeIfAbsent(statistic, s -> new AtomicLong(0)).addAndGet(increment);
        return this;
    }

    public Map<String, Long> map() {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<String, AtomicLong> e : statistics.entrySet()) {
            result.put(e.getKey(), e.getValue().get());
        }
        return result;
    }

    @Override
    public String toString() {
        return "TransactionStatistics{" +
                "statistics=" + statistics +
                '}';
    }
}
