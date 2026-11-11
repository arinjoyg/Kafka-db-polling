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
 * 200 = committed (OK or DUPLICATE)
 * 202 = PENDING: no reply within the budget; the write may still land. Never report this as a failure.
 * 503 = send failed
 */
@RestController
@RequestMapping("/payments")
@Profile("requestor")
public class PaymentController {

    private final ReplyClient client;

    public PaymentController(ReplyClient client) {
        this.client = client;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<Outcome>> submit(@RequestBody PaymentCommand body) {
        body.validate();
        PaymentCommand cmd = (body.requestId() == null || body.requestId().isBlank())
                ? body.withRequestId(UUID.randomUUID().toString())
                : body;
        return client.submit(cmd).thenApply(o -> switch (o.kind()) {
            case OK, DUPLICATE -> ResponseEntity.ok(o);
            case TIMEOUT -> ResponseEntity.accepted().body(o);
            case ERROR -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(o);
        });
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<String> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
