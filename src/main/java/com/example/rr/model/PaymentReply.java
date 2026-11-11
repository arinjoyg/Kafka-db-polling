package com.example.rr.model;

/**
 * Response published to the response topic, keyed by requestId.
 *
 * @param status         {@code ACCEPTED} or {@code REJECTED_DUPLICATE}
 * @param version        row version this requestId produced (for a duplicate: the ORIGINAL request's version)
 * @param duplicate      true when this requestId had already been applied by an earlier request
 * @param reason         why it was rejected; null when accepted
 * @param originalStatus for a rejected duplicate: the outcome of the ORIGINAL request. Callers must read this:
 *                       a REJECTED_DUPLICATE with originalStatus=ACCEPTED means the payment DID go through.
 *                       Treating it as a plain failure and retrying with a new requestId would double-pay.
 */
public record PaymentReply(
        String requestId,
        String paymentId,
        String status,
        long version,
        boolean duplicate,
        String reason,
        String originalStatus,
        String processorInstance,
        long processedAtEpochMs) {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String REJECTED_DUPLICATE = "REJECTED_DUPLICATE";
}
