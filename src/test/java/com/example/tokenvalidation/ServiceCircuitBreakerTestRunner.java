package com.example.tokenvalidation;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service-level tests for how {@link TokenValidationService} drives its {@link CircuitBreaker}.
 *
 * <p>Determinism: the breaker's clock is a {@link CircuitBreakerTestRunner.MutableClock} advanced by
 * hand, so no test waits for the open duration. Slow downstreams block on latches the test controls.
 */
public final class ServiceCircuitBreakerTestRunner {
    private static final String TOKEN = "header.payload.signature";
    private static final String CUSTOMER = "customer-456";
    private static final Duration OPEN_DURATION = Duration.ofSeconds(30);
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(50);
    private static final Duration LONG_TIMEOUT = Duration.ofSeconds(5);
    /**
     * Non-zero so back-to-back retries are never spuriously rejected while the previous worker is
     * still returning to the pool (a SynchronousQueue hand-off race). Saturation tests use 0 explicitly.
     */
    private static final int TEST_QUEUE = 8;

    private int passed;
    private int failed;

    public static void main(String[] args) {
        new ServiceCircuitBreakerTestRunner().run();
    }

    private void run() {
        execute("consecutive downstream failures open the circuit", this::failuresOpenCircuit);
        execute("open circuit fails fast without calling downstream", this::openCircuitShortCircuits);
        execute("inactive token does not trip the circuit", this::inactiveTokenIsHealthy);
        execute("timeout counts as a breaker failure", this::timeoutCountsAsFailure);
        execute("saturated bulkhead counts as a breaker failure", this::saturationCountsAsFailure);
        execute("OPEN stays OPEN until the open duration has fully elapsed", this::openUntilDurationElapses);
        execute("OPEN->HALF_OPEN happens lazily on the next request, not on the clock", this::halfOpenIsLazy);
        execute("OPEN->HALF_OPEN: trial call runs while the breaker is HALF_OPEN", this::trialRunsInHalfOpen);
        execute("HALF_OPEN admits one trial; concurrent requests fail fast", this::halfOpenAdmitsOneTrial);
        execute("HALF_OPEN trial slot is handed back when the caller is interrupted", this::halfOpenInterruptReleasesSlot);
        execute("default breaker reports OPEN->HALF_OPEN->CLOSED", this::defaultBreakerReportsRecovery);
        execute("half-open trial success closes the circuit", this::halfOpenSuccessCloses);
        execute("half-open trial failure re-opens and stops retries", this::halfOpenFailureReopens);
        execute("success resets the consecutive failure count", this::successResetsFailures);
        execute("caller interrupt does not count as a failure", this::callerInterruptIsNotFailure);
        execute("worker interrupt on shutdown is not a failure and not retried", this::workerInterruptNotRetried);
        execute("default breaker reports transitions without leaking secrets", this::defaultBreakerReportsTransitions);

        System.out.printf("%nService circuit-breaker result: %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------- tests

    private void failuresOpenCircuit() {
        Fixture f = new Fixture(3, new FailingDownstream());
        try {
            ValidationResult result = f.validate();

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "3 failed attempts should open the circuit");
            check(f.transitions.transitions.contains("CLOSED->OPEN"), "expected CLOSED->OPEN transition");
        } finally {
            f.close();
        }
    }

    private void openCircuitShortCircuits() {
        FailingDownstream downstream = new FailingDownstream();
        Fixture f = new Fixture(1, downstream);
        try {
            f.validate(); // attempt 1 fails -> OPEN; attempt 2 is short-circuited
            int callsWhenOpened = downstream.calls.get();

            ValidationResult result = f.validate();

            check(callsWhenOpened == 1, "retry after opening must not reach downstream, got " + callsWhenOpened);
            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(downstream.calls.get() == 1, "open circuit must not call downstream");
            check(f.metrics.count("validation.circuit_open") == 2, "expected circuit_open metric per rejected attempt");
            check(f.logger.entries.stream().anyMatch(e -> e.contains("circuit_open")), "expected circuit_open log");
        } finally {
            f.close();
        }
    }

    private void inactiveTokenIsHealthy() {
        AtomicInteger calls = new AtomicInteger();
        Fixture f = new Fixture(1, token -> {
            calls.incrementAndGet();
            return AuthorizationDecision.inactive("revoked");
        });
        try {
            for (int i = 0; i < 5; i++) {
                check(f.validate().status() == ValidationResult.Status.INVALID, "expected INVALID");
            }
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "rejected tokens must not open the circuit");
            check(calls.get() == 5, "every request should reach downstream");
        } finally {
            f.close();
        }
    }

    private void timeoutCountsAsFailure() {
        TimeoutTestRunner.BlockingDownstream slow = new TimeoutTestRunner.BlockingDownstream();
        Fixture f = new Fixture(1, slow, SHORT_TIMEOUT, 1);
        try {
            ValidationResult result = f.validate();

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "timeout should open a threshold-1 circuit");
        } finally {
            slow.release();
            f.close();
        }
    }

    private void saturationCountsAsFailure() {
        TimeoutTestRunner.BlockingDownstream slow = new TimeoutTestRunner.BlockingDownstream();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            pool.submit(() -> slow.validate(TOKEN)); // occupy the only worker
            CircuitBreaker breaker = breaker(1, new CircuitBreakerTestRunner.RecordingListener(),
                    new CircuitBreakerTestRunner.MutableClock());
            TokenValidationService service = new TokenValidationService(
                    slow, new TimeoutTestRunner.RecordingLogger(), new TimeoutTestRunner.RecordingMetrics(),
                    new CircuitBreakerTestRunner.MutableClock(), pool, LONG_TIMEOUT, breaker);

            ValidationResult result = service.validate(TOKEN, CUSTOMER);

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(breaker.state() == CircuitBreaker.State.OPEN, "saturation should open a threshold-1 circuit");
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void openUntilDurationElapses() {
        FailingDownstream downstream = new FailingDownstream();
        Fixture f = new Fixture(1, downstream);
        try {
            f.validate();
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "setup: expected OPEN");

            f.clock.advance(OPEN_DURATION.minusMillis(1));
            ValidationResult result = f.validate();

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(downstream.calls.get() == 1, "no trial call may happen before the open duration elapses");
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "expected still OPEN 1 ms early");
            check(!f.transitions.transitions.contains("OPEN->HALF_OPEN"), "must not go HALF_OPEN early");

            f.clock.advance(Duration.ofMillis(1)); // exactly openedAt + OPEN_DURATION
            f.validate();

            check(downstream.calls.get() == 2, "exactly one trial call expected at the boundary");
            check(f.transitions.transitions.contains("OPEN->HALF_OPEN"), "expected OPEN->HALF_OPEN at the boundary");
        } finally {
            f.close();
        }
    }

    private void halfOpenIsLazy() {
        Fixture f = new Fixture(1, new FailingDownstream());
        try {
            f.validate();
            f.clock.advance(OPEN_DURATION.multipliedBy(10));

            check(f.breaker.state() == CircuitBreaker.State.OPEN,
                    "time passing alone must not change state; the next request drives OPEN->HALF_OPEN");
            check(!f.transitions.transitions.contains("OPEN->HALF_OPEN"), "no transition without a request");
        } finally {
            f.close();
        }
    }

    private void trialRunsInHalfOpen() {
        GatedDownstream gated = new GatedDownstream();
        Fixture f = new Fixture(1, gated);
        try {
            gated.failNext.set(true);
            f.validate(); // fails -> OPEN
            f.clock.advance(OPEN_DURATION);

            CompletableFuture<ValidationResult> trial = CompletableFuture.supplyAsync(f::validate);
            check(await(gated.entered), "trial call did not reach downstream");

            check(f.breaker.state() == CircuitBreaker.State.HALF_OPEN, "expected HALF_OPEN while the trial is in flight");
            check(f.transitions.transitions.equals(java.util.List.of("CLOSED->OPEN", "OPEN->HALF_OPEN")),
                    "unexpected transitions " + f.transitions.transitions);

            gated.release.countDown();
            ValidationResult result = trial.get(LONG_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            check(result.status() == ValidationResult.Status.VALID, "trial should succeed");
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "expected CLOSED after trial success");
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        } finally {
            gated.release.countDown();
            f.close();
        }
    }

    private void halfOpenAdmitsOneTrial() {
        GatedDownstream gated = new GatedDownstream();
        Fixture f = new Fixture(1, gated, LONG_TIMEOUT, 4); // spare workers: only the breaker can block the 2nd call
        try {
            gated.failNext.set(true);
            f.validate();
            f.clock.advance(OPEN_DURATION);

            CompletableFuture<ValidationResult> trial = CompletableFuture.supplyAsync(f::validate);
            check(await(gated.entered), "trial call did not reach downstream");
            int callsDuringTrial = gated.calls.get();

            ValidationResult concurrent = f.validate();

            check(concurrent.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE,
                    "second request during the trial must fail fast");
            check(gated.calls.get() == callsDuringTrial, "second request must not reach downstream");
            check(f.metrics.count("validation.circuit_open") >= 1, "expected circuit_open metric");

            gated.release.countDown();
            check(trial.get(LONG_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).status() == ValidationResult.Status.VALID,
                    "trial should succeed");
            check(f.validate().status() == ValidationResult.Status.VALID, "traffic flows again once CLOSED");
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        } finally {
            gated.release.countDown();
            f.close();
        }
    }

    private void halfOpenInterruptReleasesSlot() {
        GatedDownstream gated = new GatedDownstream();
        Fixture f = new Fixture(1, gated);
        try {
            gated.failNext.set(true);
            f.validate();
            f.clock.advance(OPEN_DURATION);

            Thread.currentThread().interrupt();
            ValidationResult interrupted = f.validate(); // takes the only trial slot, then is interrupted
            Thread.interrupted();

            check(interrupted.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(f.breaker.state() == CircuitBreaker.State.HALF_OPEN,
                    "interrupt is not evidence of failure: must stay HALF_OPEN, not re-open");

            gated.release.countDown(); // let the next trial answer immediately
            ValidationResult next = f.validate();

            check(next.status() == ValidationResult.Status.VALID, "slot must be free for a new trial");
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "expected CLOSED after the new trial");
        } finally {
            Thread.interrupted();
            gated.release.countDown();
            f.close();
        }
    }

    private void defaultBreakerReportsRecovery() {
        TimeoutTestRunner.RecordingLogger logger = new TimeoutTestRunner.RecordingLogger();
        TimeoutTestRunner.RecordingMetrics metrics = new TimeoutTestRunner.RecordingMetrics();
        CircuitBreakerTestRunner.MutableClock clock = new CircuitBreakerTestRunner.MutableClock();
        AtomicBoolean healthy = new AtomicBoolean(false);
        ExecutorService pool = DownstreamExecutors.bounded("test", 2, TEST_QUEUE);
        try {
            TokenValidationService service = new TokenValidationService(token -> {
                if (!healthy.get()) {
                    throw new IllegalStateException("down");
                }
                return AuthorizationDecision.active("subject-123");
            }, logger, metrics, clock, pool, LONG_TIMEOUT);
            service.validate(TOKEN, CUSTOMER);
            service.validate(TOKEN, CUSTOMER);
            check(service.circuitState() == CircuitBreaker.State.OPEN, "setup: expected OPEN");

            healthy.set(true);
            clock.advance(TokenValidationService.DEFAULT_OPEN_DURATION);
            ValidationResult result = service.validate(TOKEN, CUSTOMER);

            check(result.status() == ValidationResult.Status.VALID, "trial should succeed");
            check(metrics.count("circuit.state.half_open") == 1, "expected circuit.state.half_open metric");
            check(metrics.count("circuit.state.closed") == 1, "expected circuit.state.closed metric");
            check(logger.entries.stream().anyMatch(e -> e.contains("from=OPEN, to=HALF_OPEN")),
                    "expected OPEN->HALF_OPEN audit event");
        } finally {
            pool.shutdownNow();
        }
    }

    private void halfOpenSuccessCloses() {
        AtomicBoolean healthy = new AtomicBoolean(false);
        AtomicInteger calls = new AtomicInteger();
        Fixture f = new Fixture(1, token -> {
            calls.incrementAndGet();
            if (!healthy.get()) {
                throw new IllegalStateException("down");
            }
            return AuthorizationDecision.active("subject-123");
        });
        try {
            f.validate();
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "setup: expected OPEN");

            healthy.set(true);
            f.clock.advance(OPEN_DURATION);
            int before = calls.get();
            ValidationResult result = f.validate();

            check(result.status() == ValidationResult.Status.VALID, "trial request should succeed");
            check(calls.get() - before == 1, "exactly one trial call expected");
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "trial success should close the circuit");
            check(f.transitions.transitions.contains("HALF_OPEN->CLOSED"), "expected HALF_OPEN->CLOSED");
        } finally {
            f.close();
        }
    }

    private void halfOpenFailureReopens() {
        FailingDownstream downstream = new FailingDownstream();
        Fixture f = new Fixture(1, downstream);
        try {
            f.validate();
            f.clock.advance(OPEN_DURATION);
            int before = downstream.calls.get();

            ValidationResult result = f.validate();

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(downstream.calls.get() - before == 1, "failed trial must not be retried against downstream");
            check(f.breaker.state() == CircuitBreaker.State.OPEN, "failed trial should re-open the circuit");
        } finally {
            f.close();
        }
    }

    private void successResetsFailures() {
        AtomicInteger calls = new AtomicInteger();
        // Pattern per request: fail, fail, succeed. Threshold 3 must never be reached.
        Fixture f = new Fixture(3, token -> {
            if (calls.incrementAndGet() % 3 != 0) {
                throw new IllegalStateException("flaky");
            }
            return AuthorizationDecision.active("subject-123");
        });
        try {
            for (int i = 0; i < 4; i++) {
                check(f.validate().status() == ValidationResult.Status.VALID, "expected eventual success");
            }
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "non-consecutive failures must not open");
        } finally {
            f.close();
        }
    }

    private void callerInterruptIsNotFailure() {
        TimeoutTestRunner.BlockingDownstream slow = new TimeoutTestRunner.BlockingDownstream();
        Fixture f = new Fixture(1, slow, LONG_TIMEOUT, 1);
        try {
            Thread.currentThread().interrupt();
            ValidationResult result = f.validate();
            boolean stillInterrupted = Thread.interrupted(); // also clears it for later tests

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(stillInterrupted, "caller interrupt status must be preserved");
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "caller interrupt must not open the circuit");
        } finally {
            Thread.interrupted();
            slow.release();
            f.close();
        }
    }

    private void workerInterruptNotRetried() {
        EnteringBlockingDownstream slow = new EnteringBlockingDownstream();
        Fixture f = new Fixture(1, slow, LONG_TIMEOUT, 2);
        try {
            CompletableFuture<ValidationResult> pending = CompletableFuture.supplyAsync(f::validate);
            check(await(slow.entered), "downstream call did not start");

            f.pool.shutdownNow(); // interrupts the worker, like TokenValidationService.close()

            ValidationResult result = pending.get(LONG_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected UNAVAILABLE");
            check(slow.calls.get() == 1, "worker interrupt must not be retried, got " + slow.calls.get());
            check(f.breaker.state() == CircuitBreaker.State.CLOSED, "shutdown must not open the circuit");
            check(f.metrics.count("validation.interrupted") == 1, "expected validation.interrupted metric");
            check(f.metrics.count("validation.saturated") == 0, "must not be misreported as saturation");
        } catch (Exception e) {
            throw new AssertionError("unexpected: " + e, e);
        } finally {
            f.close();
        }
    }

    private void defaultBreakerReportsTransitions() {
        TimeoutTestRunner.RecordingLogger logger = new TimeoutTestRunner.RecordingLogger();
        TimeoutTestRunner.RecordingMetrics metrics = new TimeoutTestRunner.RecordingMetrics();
        ExecutorService pool = DownstreamExecutors.bounded("test", 2, TEST_QUEUE);
        try {
            // 6-arg constructor -> default breaker (threshold 5). Two requests x 3 attempts = 6 failures.
            TokenValidationService service = new TokenValidationService(
                    new FailingDownstream(), logger, metrics, new CircuitBreakerTestRunner.MutableClock(),
                    pool, LONG_TIMEOUT);
            service.validate(TOKEN, CUSTOMER);
            service.validate(TOKEN, CUSTOMER);

            check(service.circuitState() == CircuitBreaker.State.OPEN, "default breaker should open after 5 failures");
            check(metrics.count("circuit.state.open") == 1, "expected circuit.state.open metric");
            check(logger.entries.stream().anyMatch(e -> e.contains("circuit_state_changed") && e.contains("to=OPEN")),
                    "expected circuit_state_changed log");
            for (String entry : logger.entries) {
                check(!entry.contains(TOKEN) && !entry.contains(CUSTOMER), "log leaked a secret: " + entry);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // -------------------------------------------------------------- helpers

    private static CircuitBreaker breaker(
            int threshold, CircuitBreaker.TransitionListener listener, CircuitBreakerTestRunner.MutableClock clock) {
        return new CircuitBreaker(threshold, OPEN_DURATION, 1, clock, listener);
    }

    /** Service wired to an injected breaker whose clock the test controls. */
    private static final class Fixture {
        final CircuitBreakerTestRunner.MutableClock clock = new CircuitBreakerTestRunner.MutableClock();
        final CircuitBreakerTestRunner.RecordingListener transitions = new CircuitBreakerTestRunner.RecordingListener();
        final TimeoutTestRunner.RecordingLogger logger = new TimeoutTestRunner.RecordingLogger();
        final TimeoutTestRunner.RecordingMetrics metrics = new TimeoutTestRunner.RecordingMetrics();
        final CircuitBreaker breaker;
        final ExecutorService pool;
        final TokenValidationService service;

        Fixture(int threshold, DownstreamAuthorizationClient downstream) {
            this(threshold, downstream, LONG_TIMEOUT, 2);
        }

        Fixture(int threshold, DownstreamAuthorizationClient downstream, Duration timeout, int threads) {
            this.breaker = breaker(threshold, transitions, clock);
            this.pool = DownstreamExecutors.bounded("test", threads, TEST_QUEUE);
            this.service = new TokenValidationService(downstream, logger, metrics, clock, pool, timeout, breaker);
        }

        ValidationResult validate() {
            return service.validate(TOKEN, CUSTOMER);
        }

        void close() {
            pool.shutdownNow();
        }
    }

    static final class FailingDownstream implements DownstreamAuthorizationClient {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public AuthorizationDecision validate(String token) {
            calls.incrementAndGet();
            throw new IllegalStateException("connection refused");
        }
    }

    /**
     * Fails when {@code failNext} is set (once); otherwise signals {@code entered} and waits for
     * {@code release} before answering "active".
     */
    static final class GatedDownstream implements DownstreamAuthorizationClient {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean failNext = new AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public AuthorizationDecision validate(String token) throws Exception {
            calls.incrementAndGet();
            if (failNext.getAndSet(false)) {
                throw new IllegalStateException("down");
            }
            entered.countDown();
            release.await();
            return AuthorizationDecision.active("subject-123");
        }
    }

    /** Blocks until interrupted and signals when the call has started. */
    static final class EnteringBlockingDownstream implements DownstreamAuthorizationClient {
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);

        @Override
        public AuthorizationDecision validate(String token) throws Exception {
            calls.incrementAndGet();
            entered.countDown();
            new CountDownLatch(1).await(); // until interrupted
            return AuthorizationDecision.active("unreachable");
        }
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(LONG_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void execute(String name, Runnable test) {
        try {
            test.run();
            passed++;
            System.out.println("PASS " + name);
        } catch (Throwable failure) {
            failed++;
            System.err.println("FAIL " + name + ": " + failure.getMessage());
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
