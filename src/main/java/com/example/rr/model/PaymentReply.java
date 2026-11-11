package com.example.rr.model;

/**
 * @param version   row version after this request was applied (for a duplicate: the version the
 *                  original application produced)
 * @param duplicate true when this requestId had already been applied; no second write happened
 */
public record PaymentReply(
        String requestId,
        String paymentId,
        String status,
        long version,
        boolean duplicate,
        String processorInstance,
        long processedAtEpochMs) {}
