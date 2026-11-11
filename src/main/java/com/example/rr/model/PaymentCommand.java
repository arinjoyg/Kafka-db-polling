package com.example.rr.model;

import java.math.BigDecimal;

/**
 * Request payload. {@code requestId} is the idempotency key (one logical attempt; a client retry of the
 * same attempt must reuse it). {@code paymentId} is the business key and the Kafka partition key, so all
 * updates to one payment are ordered on one partition. {@code memo} exists to vary payload size in tests.
 */
public record PaymentCommand(String requestId, String paymentId, BigDecimal amount, String currency, String memo) {

    public PaymentCommand withRequestId(String newRequestId) {
        return new PaymentCommand(newRequestId, paymentId, amount, currency, memo);
    }

    public void validate() {
        if (paymentId == null || paymentId.isBlank()) {
            throw new IllegalArgumentException("paymentId is required");
        }
        if (amount == null || amount.signum() < 0) {
            throw new IllegalArgumentException("amount must be >= 0");
        }
        if (currency == null || currency.length() != 3) {
            throw new IllegalArgumentException("currency must be ISO-4217 (3 chars)");
        }
    }
}
