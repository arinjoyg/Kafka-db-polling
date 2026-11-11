package com.example.rr.model;

/** Where a record sits in Kafka; {@code nextOffset} = record offset + 1 (the position to resume from). */
public record RecordPosition(String topic, int partition, long nextOffset) {}
