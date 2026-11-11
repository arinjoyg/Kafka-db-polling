package com.example.rr.processor;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Switchover controls for the processor. Per-pod: call every processor pod (see scripts/db-switchover.sh).
 * Secure these endpoints (network policy / auth) before using them outside a test environment.
 */
@RestController
@RequestMapping("/admin/processing")
@Profile("processor")
public class ProcessingAdminController {

    private final KafkaListenerEndpointRegistry registry;
    private final PaymentRequestListener listener;
    private final ReplayWindow replayWindow;

    public ProcessingAdminController(KafkaListenerEndpointRegistry registry, PaymentRequestListener listener,
                                     ReplayWindow replayWindow) {
        this.registry = registry;
        this.listener = listener;
        this.replayWindow = replayWindow;
    }

    /** Stops fetching but keeps polling: no rebalance, no max.poll.interval.ms eviction. */
    @PostMapping("/pause")
    public Map<String, Object> pause() {
        container().pause();
        return status();
    }

    @PostMapping("/resume")
    public Map<String, Object> resume() {
        container().resume();
        return status();
    }

    /** After repointing to the new database: seek to its stored offsets and open replay windows. */
    @PostMapping("/resync")
    public Map<String, Object> resync() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("seeks", listener.resyncFromDatabase());
        out.putAll(status());
        return out;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        MessageListenerContainer c = container();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("running", c.isRunning());
        out.put("pauseRequested", c.isPauseRequested());
        out.put("paused", c.isContainerPaused());
        out.put("replayWindows", replayWindow.snapshot());
        return out;
    }

    private MessageListenerContainer container() {
        MessageListenerContainer c = registry.getListenerContainer(PaymentRequestListener.LISTENER_ID);
        if (c == null) {
            throw new IllegalStateException("listener container not found");
        }
        return c;
    }
}
