package com.example.tokenvalidation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Dependency-free, deterministic tests for {@link CircuitBreaker}. */
public final class CircuitBreakerTestRunner {
    private static final Duration OPEN_DURATION = Duration.ofSeconds(30);

    private int passed;
    private int failed;

    public static void main(String[] args) {
        new CircuitBreakerTestRunner().run();
    }

    private void run() {
        execute("starts closed and permits calls", this::startsClosed);
        execute("opens after threshold consecutive failures", this::opensAfterThreshold);
        execute("success resets consecutive failure count", this::successResetsFailures);
        execute("rejects while open until open duration elapses", this::rejectsWhileOpen);
        execute("half-open admits only the configured trial calls", this::halfOpenLimitsTrials);
        execute("trial success closes the circuit", this::trialSuccessCloses);
        execute("trial failure re-opens and restarts the timer", this::trialFailureReopens);
        execute("stale result from before opening is ignored", this::staleResultIgnored);
        execute("release hands back a half-open slot", this::releaseFreesHalfOpenSlot);
        execute("permit is single-use", this::permitIsSingleUse);
        execute("listener failure does not break the breaker", this::listenerFailureIsContained);
        execute("rejects invalid configuration", this::rejectsInvalidConfig);
        execute("concurrent failures open the circuit exactly once", this::concurrentFailuresOpenOnce);
        execute("concurrent half-open callers get exactly N permits", this::concurrentHalfOpenPermits);

        System.out.printf("%nCircuitBreaker result: %d passed, %d failed%n", passed, failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ---------------------------------------------------------------- tests

    private void startsClosed() {
        CircuitBreaker breaker = breaker(3, 1, new MutableClock(), new RecordingListener());
        check(breaker.state() == CircuitBreaker.State.CLOSED, "expected CLOSED");
        check(breaker.tryAcquire().isPresent(), "expected permit");
    }

    private void opensAfterThreshold() {
        RecordingListener listener = new RecordingListener();
        CircuitBreaker breaker = breaker(3, 1, new MutableClock(), listener);

        fail(breaker);
        fail(breaker);
        check(breaker.state() == CircuitBreaker.State.CLOSED, "still CLOSED after 2 failures");
        fail(breaker);

        check(breaker.state() == CircuitBreaker.State.OPEN, "expected OPEN after 3 failures");
        check(listener.transitions.equals(List.of("CLOSED->OPEN")), "transitions: " + listener.transitions);
    }

    private void successResetsFailures() {
        CircuitBreaker breaker = breaker(3, 1, new MutableClock(), new RecordingListener());

        fail(breaker);
        fail(breaker);
        succeed(breaker);
        fail(breaker);
        fail(breaker);

        check(breaker.state() == CircuitBreaker.State.CLOSED, "non-consecutive failures must not open");
    }

    private void rejectsWhileOpen() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = openBreaker(clock, 1, new RecordingListener());

        check(breaker.tryAcquire().isEmpty(), "rejected immediately after opening");
        clock.advance(OPEN_DURATION.minusMillis(1));
        check(breaker.tryAcquire().isEmpty(), "rejected 1 ms before retry time");
        clock.advance(Duration.ofMillis(1));
        check(breaker.tryAcquire().isPresent(), "trial permitted once open duration elapsed");
        check(breaker.state() == CircuitBreaker.State.HALF_OPEN, "expected HALF_OPEN");
    }

    private void halfOpenLimitsTrials() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = openBreaker(clock, 2, new RecordingListener());
        clock.advance(OPEN_DURATION);

        check(breaker.tryAcquire().isPresent(), "trial 1 permitted");
        check(breaker.tryAcquire().isPresent(), "trial 2 permitted");
        check(breaker.tryAcquire().isEmpty(), "trial 3 rejected");
    }

    private void trialSuccessCloses() {
        MutableClock clock = new MutableClock();
        RecordingListener listener = new RecordingListener();
        CircuitBreaker breaker = openBreaker(clock, 1, listener);
        clock.advance(OPEN_DURATION);

        breaker.tryAcquire().orElseThrow().recordSuccess();

        check(breaker.state() == CircuitBreaker.State.CLOSED, "expected CLOSED");
        check(listener.transitions.equals(List.of("CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED")),
                "transitions: " + listener.transitions);
        check(breaker.tryAcquire().isPresent(), "normal traffic flows again");
    }

    private void trialFailureReopens() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = openBreaker(clock, 1, new RecordingListener());
        clock.advance(OPEN_DURATION);

        breaker.tryAcquire().orElseThrow().recordFailure();

        check(breaker.state() == CircuitBreaker.State.OPEN, "expected OPEN");
        clock.advance(OPEN_DURATION.minusMillis(1));
        check(breaker.tryAcquire().isEmpty(), "timer restarted from the trial failure");
        clock.advance(Duration.ofMillis(1));
        check(breaker.tryAcquire().isPresent(), "next trial after a full open duration");
    }

    private void staleResultIgnored() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = breaker(3, 1, clock, new RecordingListener());

        // A slow call starts while CLOSED...
        CircuitBreaker.Permit slowCall = breaker.tryAcquire().orElseThrow();
        // ...other calls fail and open the circuit, then it moves to HALF_OPEN.
        fail(breaker);
        fail(breaker);
        fail(breaker);
        clock.advance(OPEN_DURATION);
        CircuitBreaker.Permit trial = breaker.tryAcquire().orElseThrow();

        // The slow call finally succeeds; it must not close the circuit on the trial's behalf.
        slowCall.recordSuccess();
        check(breaker.state() == CircuitBreaker.State.HALF_OPEN, "stale success must be ignored");

        trial.recordFailure();
        check(breaker.state() == CircuitBreaker.State.OPEN, "trial outcome still decides");
    }

    private void releaseFreesHalfOpenSlot() {
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = openBreaker(clock, 1, new RecordingListener());
        clock.advance(OPEN_DURATION);

        CircuitBreaker.Permit trial = breaker.tryAcquire().orElseThrow();
        check(breaker.tryAcquire().isEmpty(), "slot taken");
        trial.release();

        check(breaker.state() == CircuitBreaker.State.HALF_OPEN, "release does not change state");
        check(breaker.tryAcquire().isPresent(), "slot available again after release");
    }

    private void permitIsSingleUse() {
        CircuitBreaker breaker = breaker(2, 1, new MutableClock(), new RecordingListener());
        CircuitBreaker.Permit permit = breaker.tryAcquire().orElseThrow();

        permit.recordFailure();
        permit.recordFailure(); // must be ignored

        check(breaker.state() == CircuitBreaker.State.CLOSED, "one permit counts as one failure");
    }

    private void listenerFailureIsContained() {
        CircuitBreaker breaker = breaker(1, 1, new MutableClock(), (from, to) -> {
            throw new IllegalStateException("metrics backend down");
        });

        fail(breaker);

        check(breaker.state() == CircuitBreaker.State.OPEN, "transition applied despite listener error");
    }

    private void rejectsInvalidConfig() {
        MutableClock clock = new MutableClock();
        expectIllegalArgument(() -> new CircuitBreaker(0, OPEN_DURATION, 1, clock, CircuitBreaker.TransitionListener.NO_OP));
        expectIllegalArgument(() -> new CircuitBreaker(1, Duration.ZERO, 1, clock, CircuitBreaker.TransitionListener.NO_OP));
        expectIllegalArgument(() -> new CircuitBreaker(1, OPEN_DURATION, 0, clock, CircuitBreaker.TransitionListener.NO_OP));
    }

    private void concurrentFailuresOpenOnce() {
        int threads = 64;
        RecordingListener listener = new RecordingListener();
        CircuitBreaker breaker = breaker(10, 1, new MutableClock(), listener);

        runConcurrently(threads, () -> breaker.tryAcquire().ifPresent(CircuitBreaker.Permit::recordFailure));

        check(breaker.state() == CircuitBreaker.State.OPEN, "expected OPEN");
        check(listener.transitions.equals(List.of("CLOSED->OPEN")),
                "must open exactly once, got " + listener.transitions);
    }

    private void concurrentHalfOpenPermits() {
        int threads = 64;
        int maxTrials = 3;
        MutableClock clock = new MutableClock();
        CircuitBreaker breaker = openBreaker(clock, maxTrials, new RecordingListener());
        clock.advance(OPEN_DURATION);
        AtomicInteger granted = new AtomicInteger();

        runConcurrently(threads, () -> {
            Optional<CircuitBreaker.Permit> permit = breaker.tryAcquire();
            if (permit.isPresent()) {
                granted.incrementAndGet(); // hold the permit: never report
            }
        });

        check(granted.get() == maxTrials, "expected " + maxTrials + " permits, got " + granted.get());
    }

    // -------------------------------------------------------------- helpers

    private static CircuitBreaker breaker(
            int threshold, int halfOpenMaxCalls, Clock clock, CircuitBreaker.TransitionListener listener) {
        return new CircuitBreaker(threshold, OPEN_DURATION, halfOpenMaxCalls, clock, listener);
    }

    private static CircuitBreaker openBreaker(
            MutableClock clock, int halfOpenMaxCalls, CircuitBreaker.TransitionListener listener) {
        CircuitBreaker breaker = breaker(1, halfOpenMaxCalls, clock, listener);
        fail(breaker);
        check(breaker.state() == CircuitBreaker.State.OPEN, "setup: expected OPEN");
        return breaker;
    }

    private static void fail(CircuitBreaker breaker) {
        breaker.tryAcquire().orElseThrow().recordFailure();
    }

    private static void succeed(CircuitBreaker breaker) {
        breaker.tryAcquire().orElseThrow().recordSuccess();
    }

    /** Releases all threads at once through a start gate to maximise contention. */
    private static void runConcurrently(int threads, Runnable task) {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    task.run();
                    return null;
                }));
            }
            check(ready.await(5, TimeUnit.SECONDS), "threads did not start");
            go.countDown();
            for (Future<?> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new AssertionError("concurrent run failed: " + e, e);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void expectIllegalArgument(Runnable action) {
        try {
            action.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("expected IllegalArgumentException");
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

    static final class RecordingListener implements CircuitBreaker.TransitionListener {
        final List<String> transitions = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onTransition(CircuitBreaker.State from, CircuitBreaker.State to) {
            transitions.add(from + "->" + to);
        }
    }

    /** Deterministic, manually advanced clock. */
    static final class MutableClock extends Clock {
        private volatile Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
