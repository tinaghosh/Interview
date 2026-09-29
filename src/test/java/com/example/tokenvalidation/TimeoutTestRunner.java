package com.example.tokenvalidation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the downstream timeout and bulkhead behaviour.
 *
 * <p>Determinism: the "slow" downstream never returns on its own; it blocks on a latch that the
 * test controls. The only real time involved is the short attempt timeout, and assertions use a
 * generous upper bound, so the tests do not depend on scheduler timing.
 */
public final class TimeoutTestRunner {
    private static final String TOKEN = "header.payload.signature";
    private static final String CUSTOMER = "customer-456";
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(50);
    private static final long UPPER_BOUND_MILLIS = 1_000;

    private int passed;
    private int failed;

    public static void main(String[] args) {
        new TimeoutTestRunner().run();
    }

    private void run() {
        execute("slow downstream times out and returns TEMPORARILY_UNAVAILABLE", this::slowDownstreamTimesOut);
        execute("timeout is not retried", this::timeoutIsNotRetried);
        execute("timed-out worker thread is interrupted", this::timedOutWorkerIsInterrupted);
        execute("timeout log does not contain token or customer id", this::timeoutLogIsRedacted);
        execute("fast downstream is unaffected by the timeout", this::fastDownstreamSucceeds);
        execute("saturated pool fails fast without calling downstream", this::saturatedPoolFailsFast);
        execute("caller interrupt is preserved and fails closed", this::callerInterruptPreserved);
        execute("duration is recorded on the timeout path", this::durationRecordedOnTimeout);
        execute("rejects non-positive timeout", this::rejectsNonPositiveTimeout);

        System.out.printf("%nTimeout result: %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------- tests

    private void slowDownstreamTimesOut() {
        BlockingDownstream slow = new BlockingDownstream();
        RecordingMetrics metrics = new RecordingMetrics();
        ExecutorService pool = DownstreamExecutors.bounded("test", 2, 0);
        try {
            TokenValidationService service = service(slow, new RecordingLogger(), metrics, pool);

            long startNanos = System.nanoTime();
            ValidationResult result = service.validate(TOKEN, CUSTOMER);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE,
                    "expected TEMPORARILY_UNAVAILABLE, got " + result.status());
            check(elapsedMillis < UPPER_BOUND_MILLIS,
                    "caller must be released quickly, took " + elapsedMillis + " ms");
            check(metrics.count("validation.timeout") == 1, "expected one timeout metric");
            check(metrics.count("validation.success") == 0, "timeout must not count as success");
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void timeoutIsNotRetried() {
        BlockingDownstream slow = new BlockingDownstream();
        ExecutorService pool = DownstreamExecutors.bounded("test", 4, 0);
        try {
            service(slow, new RecordingLogger(), new RecordingMetrics(), pool).validate(TOKEN, CUSTOMER);

            check(slow.calls.get() == 1, "expected exactly one downstream call, got " + slow.calls.get());
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void timedOutWorkerIsInterrupted() {
        BlockingDownstream slow = new BlockingDownstream();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            service(slow, new RecordingLogger(), new RecordingMetrics(), pool).validate(TOKEN, CUSTOMER);

            check(await(slow.interrupted), "worker should be interrupted so its pool slot is reclaimed");
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void timeoutLogIsRedacted() {
        BlockingDownstream slow = new BlockingDownstream();
        RecordingLogger logger = new RecordingLogger();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            service(slow, logger, new RecordingMetrics(), pool).validate(TOKEN, CUSTOMER);

            String timeoutEntry = logger.entries.stream()
                    .filter(e -> e.contains("downstream_timeout"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("expected a downstream_timeout log entry"));
            check(!timeoutEntry.contains(TOKEN), "timeout log leaked the token");
            check(!timeoutEntry.contains(CUSTOMER), "timeout log leaked the customer id");
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void fastDownstreamSucceeds() {
        RecordingMetrics metrics = new RecordingMetrics();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            // Generous timeout: this test is about the happy path, not a race against the clock.
            TokenValidationService service = new TokenValidationService(
                    token -> AuthorizationDecision.active("subject-123"),
                    new RecordingLogger(), metrics, fixedClock(), pool, Duration.ofSeconds(5));

            ValidationResult result = service.validate(TOKEN, CUSTOMER);

            check(result.status() == ValidationResult.Status.VALID, "expected VALID");
            check("subject-123".equals(result.subject()), "expected subject");
            check(metrics.count("validation.timeout") == 0, "no timeout expected");
        } finally {
            pool.shutdownNow();
        }
    }

    private void saturatedPoolFailsFast() {
        // One thread, no queue. Occupy the only thread before the service call.
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        CountDownLatch occupying = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        AtomicInteger downstreamCalls = new AtomicInteger();
        RecordingMetrics metrics = new RecordingMetrics();
        try {
            pool.submit(() -> {
                occupying.countDown();
                releaseBlocker.await();
                return null;
            });
            check(await(occupying), "setup: blocker did not start");

            // Long timeout: if the service waited instead of failing fast, the test would take 5 s.
            TokenValidationService service = new TokenValidationService(token -> {
                downstreamCalls.incrementAndGet();
                return AuthorizationDecision.active("never");
            }, new RecordingLogger(), metrics, fixedClock(), pool, Duration.ofSeconds(5));

            long startNanos = System.nanoTime();
            ValidationResult result = service.validate(TOKEN, CUSTOMER);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "expected unavailable");
            check(downstreamCalls.get() == 0, "downstream must not be called when saturated");
            check(metrics.count("validation.saturated") == 1, "expected saturated metric");
            check(metrics.count("validation.timeout") == 0, "saturation is not a timeout");
            check(elapsedMillis < UPPER_BOUND_MILLIS,
                    "must fail fast, not wait for the timeout; took " + elapsedMillis + " ms");
        } finally {
            releaseBlocker.countDown();
            pool.shutdownNow();
        }
    }

    private void callerInterruptPreserved() {
        BlockingDownstream slow = new BlockingDownstream();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            TokenValidationService service = service(slow, new RecordingLogger(), new RecordingMetrics(), pool);

            Thread.currentThread().interrupt(); // e.g. server shutting down
            ValidationResult result = service.validate(TOKEN, CUSTOMER);
            boolean stillInterrupted = Thread.interrupted(); // reads AND clears, so later tests are clean

            check(result.status() == ValidationResult.Status.TEMPORARILY_UNAVAILABLE, "must fail closed");
            check(stillInterrupted, "interrupt flag must be restored, not swallowed");
        } finally {
            Thread.interrupted();
            slow.release();
            pool.shutdownNow();
        }
    }

    private void durationRecordedOnTimeout() {
        BlockingDownstream slow = new BlockingDownstream();
        RecordingMetrics metrics = new RecordingMetrics();
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            service(slow, new RecordingLogger(), metrics, pool).validate(TOKEN, CUSTOMER);

            check(metrics.durations.getOrDefault("validation.duration", List.of()).size() == 1,
                    "slow requests must appear in the latency metric");
        } finally {
            slow.release();
            pool.shutdownNow();
        }
    }

    private void rejectsNonPositiveTimeout() {
        ExecutorService pool = DownstreamExecutors.bounded("test", 1, 0);
        try {
            for (Duration bad : List.of(Duration.ZERO, Duration.ofMillis(-1))) {
                try {
                    new TokenValidationService(token -> AuthorizationDecision.active("x"),
                            new RecordingLogger(), new RecordingMetrics(), fixedClock(), pool, bad);
                    throw new AssertionError("expected rejection of timeout " + bad);
                } catch (IllegalArgumentException expected) {
                    // ok
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    // -------------------------------------------------------------- helpers

    private static TokenValidationService service(
            DownstreamAuthorizationClient downstream,
            RecordingLogger logger,
            RecordingMetrics metrics,
            ExecutorService pool) {
        return new TokenValidationService(downstream, logger, metrics, fixedClock(), pool, SHORT_TIMEOUT);
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(UPPER_BOUND_MILLIS, TimeUnit.MILLISECONDS);
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

    /** Downstream that hangs until released (or interrupted), like the 10-second incident. */
    static final class BlockingDownstream implements DownstreamAuthorizationClient {
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch interrupted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public AuthorizationDecision validate(String token) throws Exception {
            calls.incrementAndGet();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
            return AuthorizationDecision.active("too-late");
        }

        void release() {
            release.countDown();
        }
    }

    static final class RecordingLogger implements AuditLogger {
        final List<String> entries = java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void info(String event, String details) {
            entries.add("INFO " + event + " " + details);
        }

        @Override
        public void warn(String event, String details) {
            entries.add("WARN " + event + " " + details);
        }
    }

    static final class RecordingMetrics implements MetricsRecorder {
        final Map<String, Integer> counters = new HashMap<>();
        final Map<String, List<Duration>> durations = new HashMap<>();

        @Override
        public synchronized void increment(String metricName) {
            counters.merge(metricName, 1, Integer::sum);
        }

        @Override
        public synchronized void recordDuration(String metricName, Duration duration) {
            durations.computeIfAbsent(metricName, k -> new ArrayList<>()).add(duration);
        }

        synchronized int count(String name) {
            return counters.getOrDefault(name, 0);
        }
    }
}
