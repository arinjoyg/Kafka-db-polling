package com.example.rr.requestor;

import com.example.rr.model.Outcome;
import com.example.rr.model.PaymentCommand;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 200 = committed
 * 409 = REJECTED_DUPLICATE: requestId already processed; body.reply.originalStatus says how the original ended
 * 202 = PENDING: no reply within the budget; the write may still land. Never report this as a failure.
 * 503 = send failed, or intake closed for a DB switchover (with Retry-After)
 */
@RestController
@RequestMapping("/payments")
@Profile("requestor")
public class PaymentController {

    private final ReplyClient client;
    private final IntakeGate gate;

    public PaymentController(ReplyClient client, IntakeGate gate) {
        this.client = client;
        this.gate = gate;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<Outcome>> submit(@RequestBody PaymentCommand body) {
        if (!gate.isOpen()) {
            // Switchover in progress: fail fast and honestly rather than queue past the deadline.
            return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header("Retry-After", String.valueOf(IntakeGate.RETRY_AFTER_SECONDS))
                    .<Outcome>build());
        }
        body.validate();
        PaymentCommand cmd = (body.requestId() == null || body.requestId().isBlank())
                ? body.withRequestId(UUID.randomUUID().toString())
                : body;
        return client.submit(cmd).thenApply(o -> switch (o.kind()) {
            case OK -> ResponseEntity.ok(o);
            // Same requestId already processed: reject, but the body carries the original outcome.
            case DUPLICATE -> ResponseEntity.status(HttpStatus.CONFLICT).body(o);
            case TIMEOUT -> ResponseEntity.accepted().body(o);
            case ERROR -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(o);
        });
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<String> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
