package com.example.rr.config;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public final class RrHeaders {

    /**
     * Absolute deadline (epoch millis, requestor clock). The processor drops anything already past
     * its deadline instead of writing and replying to a caller that has given up.
     * Requires NTP-synced clocks; skew eats directly into the budget.
     */
    public static final String DEADLINE = "x-deadline-epoch-ms";

    private RrHeaders() {}

    public static void putDeadline(Headers headers, long deadlineEpochMs) {
        headers.add(DEADLINE, Long.toString(deadlineEpochMs).getBytes(StandardCharsets.UTF_8));
    }

    /** @return the deadline, or -1 if the header is absent or malformed. */
    public static long deadline(Headers headers) {
        Header h = headers.lastHeader(DEADLINE);
        if (h == null || h.value() == null) {
            return -1L;
        }
        try {
            return Long.parseLong(new String(h.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return -1L;
        }
    }
}
