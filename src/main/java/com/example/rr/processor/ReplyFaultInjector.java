package com.example.rr.processor;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Chaos hooks, inert unless a test arms them for a specific requestId:
 * <ul>
 *   <li>crash AFTER the response is sent but BEFORE the Kafka transaction (response + offset) commits;</li>
 *   <li>stall (DB already committed) BEFORE sending the response, long enough to exceed max.poll.interval.ms,
 *       so the consumer is evicted and its partition reassigned mid-request ("zombie" consumer).</li>
 * </ul>
 */
@Component
@Profile("processor")
public class ReplyFaultInjector {

    private final Set<String> armed = ConcurrentHashMap.newKeySet();
    private final AtomicInteger fired = new AtomicInteger();
    private final Map<String, Duration> stalls = new ConcurrentHashMap<>();
    private final AtomicInteger stallsStarted = new AtomicInteger();

    public void stallOnceBeforeReply(String requestId, Duration duration) {
        stalls.put(requestId, duration);
    }

    public int stallsStarted() {
        return stallsStarted.get();
    }

    void beforeReplySent(String requestId) {
        Duration stall = stalls.remove(requestId);
        if (stall != null) {
            stallsStarted.incrementAndGet();
            try {
                Thread.sleep(stall.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void failOnceAfterReply(String requestId) {
        armed.add(requestId);
    }

    public int fired() {
        return fired.get();
    }

    /** Flushes first, so the response is really on the broker before the "crash": the worst case, deterministically. */
    void afterReplySent(String requestId, Runnable flush) {
        if (armed.remove(requestId)) {
            flush.run();
            fired.incrementAndGet();
            throw new InjectedFault("injected crash after reply sent, before commit: " + requestId);
        }
    }

    public static class InjectedFault extends RuntimeException {
        InjectedFault(String message) {
            super(message);
        }
    }
}
