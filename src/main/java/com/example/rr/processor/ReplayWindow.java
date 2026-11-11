package com.example.rr.processor;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Offsets below the window's end were already in Kafka when the DB was found to be behind (after a failover),
 * so another database may already have applied them and answered OK. Those records are re-applied even if
 * their deadline has passed: dropping them would lose a write the caller was told succeeded.
 */
@Component
@Profile("processor")
public class ReplayWindow {

    private final Map<TopicPartition, Long> endExclusive = new ConcurrentHashMap<>();

    public void open(TopicPartition tp, long end) {
        endExclusive.merge(tp, end, Math::max);
    }

    public boolean isReplay(TopicPartition tp, long offset) {
        Long end = endExclusive.get(tp);
        if (end == null) {
            return false;
        }
        if (offset < end) {
            return true;
        }
        endExclusive.remove(tp, end);   // window passed
        return false;
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> out = new TreeMap<>();
        endExclusive.forEach((tp, end) -> out.put(tp.toString(), end));
        return out;
    }
}
