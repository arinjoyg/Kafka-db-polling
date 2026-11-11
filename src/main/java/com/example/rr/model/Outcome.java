package com.example.rr.model;

/** What the caller of the requestor sees. */
public record Outcome(String requestId, Kind kind, PaymentReply reply, long latencyMs, String error) {

    public enum Kind {
        /** Reply received within the timeout; write committed. */
        OK,
        /** Rejected: this requestId had already been processed (no second write). See reply.originalStatus. */
        DUPLICATE,
        /**
         * No reply within the timeout. The write MAY still commit later ("late write"), or may have been
         * dropped as expired. Callers must treat this as PENDING and query status, never as FAILED.
         */
        TIMEOUT,
        /** Send failed or reply unparseable. */
        ERROR
    }

    public static Outcome ok(String requestId, PaymentReply reply, long latencyMs) {
        return new Outcome(requestId, reply.duplicate() ? Kind.DUPLICATE : Kind.OK, reply, latencyMs, null);
    }

    public static Outcome timeout(String requestId, long latencyMs) {
        return new Outcome(requestId, Kind.TIMEOUT, null, latencyMs, null);
    }

    public static Outcome error(String requestId, long latencyMs, Throwable t) {
        return new Outcome(requestId, Kind.ERROR, null, latencyMs, String.valueOf(t));
    }
}
