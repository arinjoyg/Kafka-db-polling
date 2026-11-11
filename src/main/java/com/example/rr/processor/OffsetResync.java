package com.example.rr.processor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.stereotype.Component;

/**
 * Makes the database, not Kafka, the source of truth for consumer progress.
 *
 * <p>On partition assignment: if the DB's stored offset is BEHIND Kafka's committed offset, the DB lost
 * commits (e.g. async replication lag at failover). Seek back to the DB offset and open a replay window up
 * to the current log end, so every record that might have been answered by the old DB is re-applied here.
 * If the DB is ahead, do nothing: the ledger de-duplicates and callers still get replies.
 *
 * <p>On an operator resync (after repointing to the shadow): seek every assigned partition to the DB offset.
 */
@Component
@Profile("processor")
public class OffsetResync {

    private static final Logger log = LoggerFactory.getLogger(OffsetResync.class);

    private final PaymentRepository repo;
    private final ReplayWindow window;
    private final KafkaAdmin kafkaAdmin;

    public OffsetResync(PaymentRepository repo, ReplayWindow window, KafkaAdmin kafkaAdmin) {
        this.repo = repo;
        this.window = window;
        this.kafkaAdmin = kafkaAdmin;
    }

    /** Called on the consumer thread with Kafka's committed positions for newly assigned partitions. */
    void onAssigned(Map<TopicPartition, Long> positions, ConsumerSeekCallback callback) {
        Map<TopicPartition, Long> seekTo = new HashMap<>();
        positions.forEach((tp, position) -> dbOffset(tp)
                .filter(dbNext -> dbNext < position)
                .ifPresent(dbNext -> seekTo.put(tp, dbNext)));
        if (seekTo.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> ends = endOffsets(seekTo.keySet());
        seekTo.forEach((tp, dbNext) -> {
            long end = ends.getOrDefault(tp, positions.get(tp));
            window.open(tp, end);
            callback.seek(tp.topic(), tp.partition(), dbNext);
            log.warn("DB behind Kafka for {}: kafka-committed={} db-next={} -> seek back, replay window until {}",
                    tp, positions.get(tp), dbNext, end);
        });
    }

    /** Operator-triggered: seek every assigned partition to what the (new) database says. */
    Map<String, String> resyncAll(Map<TopicPartition, ConsumerSeekCallback> callbacks) {
        Map<String, String> report = new LinkedHashMap<>();
        if (callbacks.isEmpty()) {
            report.put("result", "no partitions assigned to this instance");
            return report;
        }
        Map<TopicPartition, Long> ends = endOffsets(callbacks.keySet());
        callbacks.forEach((tp, callback) -> {
            Optional<Long> dbNext = dbOffset(tp);
            if (dbNext.isEmpty()) {
                report.put(tp.toString(), "no DB offset; left at current position");
                log.warn("Resync: no DB offset for {}; leaving consumer position unchanged", tp);
                return;
            }
            long end = ends.get(tp);
            if (dbNext.get() < end) {
                window.open(tp, end);
            }
            callback.seek(tp.topic(), tp.partition(), dbNext.get());
            report.put(tp.toString(), "seek " + dbNext.get() + ", replay until " + end);
        });
        log.warn("Resync from DB offsets: {}", report);
        return report;
    }

    private Optional<Long> dbOffset(TopicPartition tp) {
        return repo.findNextOffset(tp.topic(), tp.partition());
    }

    private Map<TopicPartition, Long> endOffsets(Set<TopicPartition> tps) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<TopicPartition, OffsetSpec> request =
                    tps.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
            return admin.listOffsets(request).all().get(10, TimeUnit.SECONDS).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted reading end offsets", e);
        } catch (Exception e) {
            throw new IllegalStateException("Could not read end offsets for " + tps, e);
        }
    }
}
