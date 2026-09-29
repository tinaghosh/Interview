package com.example.tokenvalidation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Validates OAuth tokens against a downstream authorization service.
 *
 * <p>Every downstream call runs on a bounded executor (bulkhead) and is abandoned after
 * {@code attemptTimeout}, so a slow dependency can never hold a request thread longer than that.
 *
 * <p>A {@link CircuitBreaker} guards every attempt. While it is open, requests fail fast with
 * {@code TEMPORARILY_UNAVAILABLE} without touching the downstream or the bulkhead. Each attempt
 * (including retries) takes its own permit and reports exactly one outcome, so a failing retry in
 * {@code HALF_OPEN} re-opens the circuit and the next attempt is short-circuited.
 */
public final class TokenValidationService implements AutoCloseable {
    private static final int MAX_ATTEMPTS = 3;

    /** ~3x the normal 100 ms downstream latency. */
    static final Duration DEFAULT_ATTEMPT_TIMEOUT = Duration.ofMillis(300);
    static final int DEFAULT_POOL_THREADS = 200;
    static final int DEFAULT_POOL_QUEUE = 100;
    static final int DEFAULT_FAILURE_THRESHOLD = 5;
    static final Duration DEFAULT_OPEN_DURATION = Duration.ofSeconds(30);
    static final int DEFAULT_HALF_OPEN_TRIALS = 1;

    private final DownstreamAuthorizationClient downstream;
    private final AuditLogger auditLogger;
    private final MetricsRecorder metrics;
    private final Clock clock;
    private final ExecutorService downstreamExecutor;
    private final Duration attemptTimeout;
    private final CircuitBreaker breaker;
    private final boolean ownsExecutor;

    /** Convenience constructor with production defaults; the service owns (and closes) its pool. */
    public TokenValidationService(
            DownstreamAuthorizationClient downstream,
            AuditLogger auditLogger,
            MetricsRecorder metrics,
            Clock clock) {
        this(downstream, auditLogger, metrics, clock,
                DownstreamExecutors.bounded("downstream-auth", DEFAULT_POOL_THREADS, DEFAULT_POOL_QUEUE),
                DEFAULT_ATTEMPT_TIMEOUT,
                null,
                true);
    }

    /** Caller owns the executor's lifecycle; uses the default circuit breaker. */
    public TokenValidationService(
            DownstreamAuthorizationClient downstream,
            AuditLogger auditLogger,
            MetricsRecorder metrics,
            Clock clock,
            ExecutorService downstreamExecutor,
            Duration attemptTimeout) {
        this(downstream, auditLogger, metrics, clock, downstreamExecutor, attemptTimeout, null, false);
    }

    /** Full constructor; the caller owns the executor and supplies the circuit breaker. */
    public TokenValidationService(
            DownstreamAuthorizationClient downstream,
            AuditLogger auditLogger,
            MetricsRecorder metrics,
            Clock clock,
            ExecutorService downstreamExecutor,
            Duration attemptTimeout,
            CircuitBreaker breaker) {
        this(downstream, auditLogger, metrics, clock, downstreamExecutor, attemptTimeout,
                Objects.requireNonNull(breaker, "breaker"), false);
    }

    private TokenValidationService(
            DownstreamAuthorizationClient downstream,
            AuditLogger auditLogger,
            MetricsRecorder metrics,
            Clock clock,
            ExecutorService downstreamExecutor,
            Duration attemptTimeout,
            CircuitBreaker breaker,
            boolean ownsExecutor) {
        this.downstream = Objects.requireNonNull(downstream);
        this.auditLogger = Objects.requireNonNull(auditLogger);
        this.metrics = Objects.requireNonNull(metrics);
        this.clock = Objects.requireNonNull(clock);
        this.downstreamExecutor = Objects.requireNonNull(downstreamExecutor);
        this.attemptTimeout = Objects.requireNonNull(attemptTimeout);
        if (attemptTimeout.isNegative() || attemptTimeout.isZero()) {
            throw new IllegalArgumentException("attemptTimeout must be positive");
        }
        this.breaker = breaker != null ? breaker : defaultBreaker(this.clock, this.metrics, this.auditLogger);
        this.ownsExecutor = ownsExecutor;
    }

    /** Production defaults; state changes are exported as a metric and an audit event. */
    static CircuitBreaker defaultBreaker(Clock clock, MetricsRecorder metrics, AuditLogger auditLogger) {
        return new CircuitBreaker(
                DEFAULT_FAILURE_THRESHOLD,
                DEFAULT_OPEN_DURATION,
                DEFAULT_HALF_OPEN_TRIALS,
                clock,
                (from, to) -> {
                    metrics.increment("circuit.state." + to.name().toLowerCase());
                    auditLogger.warn("circuit_state_changed", "from=" + from + ", to=" + to);
                });
    }

    /** Exposed for tests and health checks. */
    CircuitBreaker.State circuitState() {
        return breaker.state();
    }

    public ValidationResult validate(String token, String customerId) {
        if (token == null || token.isBlank()) {
            return ValidationResult.invalid("missing token");
        }

        Instant start = clock.instant();
        // Never log the token (a bearer credential) or the customer id (PII); neither is needed to
        // operate the service. Correlate via the request/trace id carried by the logging context.
        auditLogger.info("validation_started", "");

        try {
            for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
                Optional<CircuitBreaker.Permit> acquired = breaker.tryAcquire();
                if (acquired.isEmpty()) {
                    // Open (or half-open with its trial slots taken): don't touch the dependency.
                    metrics.increment("validation.circuit_open");
                    auditLogger.warn("circuit_open", "attempt=" + attempt);
                    return ValidationResult.unavailable();
                }
                CircuitBreaker.Permit permit = acquired.get();
                try {
                    Future<AuthorizationDecision> future;
                    try {
                        future = downstreamExecutor.submit(() -> downstream.validate(token));
                    } catch (RejectedExecutionException saturated) {
                        // Bulkhead full: the dependency is already slow. Fail fast, don't retry.
                        permit.recordFailure();
                        metrics.increment("validation.saturated");
                        auditLogger.warn("downstream_saturated", "attempt=" + attempt);
                        return ValidationResult.unavailable();
                    }

                    try {
                        AuthorizationDecision decision =
                                future.get(attemptTimeout.toMillis(), TimeUnit.MILLISECONDS);
                        // Any answer, including "inactive", means the dependency is healthy.
                        permit.recordSuccess();
                        metrics.increment("validation.success");

                        if (decision.active()) {
                            auditLogger.info("validation_succeeded", "attempt=" + attempt);
                            return ValidationResult.valid(decision.subject());
                        }

                        auditLogger.warn("validation_rejected", "attempt=" + attempt);
                        return ValidationResult.invalid(decision.reason());
                    } catch (TimeoutException timeout) {
                        // Abandon the call and interrupt the worker so the pool slot can be reclaimed.
                        // Not retried: a timeout means the dependency is slow, and retrying would
                        // multiply both latency and load on it.
                        future.cancel(true);
                        permit.recordFailure();
                        metrics.increment("validation.timeout");
                        auditLogger.warn("downstream_timeout",
                                "attempt=" + attempt + ", timeoutMs=" + attemptTimeout.toMillis());
                        return ValidationResult.unavailable();
                    } catch (InterruptedException interrupted) {
                        // Caller is being cancelled/shut down: says nothing about dependency health.
                        future.cancel(true);
                        permit.release();
                        Thread.currentThread().interrupt(); // preserve the caller's interrupt status
                        metrics.increment("validation.interrupted");
                        return ValidationResult.unavailable();
                    } catch (ExecutionException failure) {
                        Throwable cause = failure.getCause() != null ? failure.getCause() : failure;
                        if (cause instanceof InterruptedException) {
                            // Worker interrupted (e.g. executor shutdown): not a dependency failure,
                            // and retrying on a shutting-down pool is pointless.
                            permit.release();
                            metrics.increment("validation.interrupted");
                            return ValidationResult.unavailable();
                        }
                        permit.recordFailure();
                        metrics.increment("validation.retry");
                        auditLogger.warn(
                                "downstream_failure",
                                // Exception class only: downstream messages can echo the request (and token).
                                "attempt=" + attempt + ", errorType=" + cause.getClass().getSimpleName());
                    }
                } finally {
                    // Safety net for unexpected exceptions: never leak a half-open trial slot.
                    // No-op when an outcome was already recorded (permits are single-use).
                    permit.release();
                }
            }

            metrics.increment("validation.failure");
            return ValidationResult.unavailable();
        } finally {
            metrics.recordDuration("validation.duration", Duration.between(start, clock.instant()));
        }
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            downstreamExecutor.shutdownNow();
        }
    }
}
