package com.example.rr.processor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Chaos hook: simulates a processor crash AFTER the response has been sent but BEFORE the Kafka transaction
 * (response + consumer offset) commits. Inert unless a test arms it for a specific requestId.
 */
@Component
@Profile("processor")
public class ReplyFaultInjector {

    private final Set<String> armed = ConcurrentHashMap.newKeySet();
    private final AtomicInteger fired = new AtomicInteger();

    public void failOnceAfterReply(String requestId) {
        armed.add(requestId);
    }

    public int fired() {
        return fired.get();
    }

    void afterReplySent(String requestId) {
        if (armed.remove(requestId)) {
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
