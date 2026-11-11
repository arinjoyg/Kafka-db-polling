package com.example.rr.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppProperties(
        /** Unique per pod; used for the reply consumer group and for tracing which instance handled what. */
        String instanceId,
        Topics topics,
        /** End-to-end budget the caller is promised. Sent as an absolute deadline header. */
        long slaMs,
        /** How long the requestor waits for a reply. Keep below slaMs to leave room for HTTP + serialization. */
        long replyTimeoutMs,
        /**
         * Dedicated reply partition for this requestor instance. Each requestor replica MUST use a
         * distinct partition (e.g. StatefulSet ordinal), and reply-partitions must be >= replica count.
         */
        int replyPartition,
        Processor processor,
        Db db,
        Audit audit) {

    public record Topics(String request, String reply, int requestPartitions, int replyPartitions, int replicationFactor) {}

    public record Processor(int concurrency) {}

    /** Server-side guards so a single slow statement or lock wait can't silently eat the whole SLA. */
    public record Db(int lockTimeoutMs, int statementTimeoutMs) {}

    /** When enabled the requestor records every outcome so timeouts can be reconciled against DB commits. */
    public record Audit(boolean enabled) {}
}
