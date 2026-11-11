package com.example.rr.requestor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Closes the front door during a DB switchover: callers get 503 + Retry-After immediately instead of a request
 * that is queued past its 1s deadline and ends as PENDING. Per-pod; call every requestor pod.
 */
@Component
@Profile("requestor")
public class IntakeGate {

    public static final int RETRY_AFTER_SECONDS = 30;

    private final AtomicBoolean open = new AtomicBoolean(true);

    public boolean isOpen() {
        return open.get();
    }

    void set(boolean value) {
        open.set(value);
    }

    @RestController
    @RequestMapping("/admin/intake")
    @Profile("requestor")
    static class IntakeAdminController {

        private final IntakeGate gate;

        IntakeAdminController(IntakeGate gate) {
            this.gate = gate;
        }

        @PostMapping("/close")
        Map<String, Object> close() {
            gate.set(false);
            return Map.of("open", false);
        }

        @PostMapping("/open")
        Map<String, Object> reopen() {
            gate.set(true);
            return Map.of("open", true);
        }
    }
}
