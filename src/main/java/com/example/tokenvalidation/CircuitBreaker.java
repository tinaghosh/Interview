package com.example.tokenvalidation;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free, dependency-free circuit breaker.
 *
 * <pre>
 *   CLOSED --(N consecutive failures)--> OPEN
 *   OPEN   --(openDuration elapsed, next caller)--> HALF_OPEN
 *   HALF_OPEN --(trial success)--> CLOSED
 *   HALF_OPEN --(trial failure)--> OPEN (timer restarts)
 * </pre>
 *
 * <p>Callers must obtain a {@link Permit} before calling the dependency and report exactly one
 * outcome on it: {@link Permit#recordSuccess()}, {@link Permit#recordFailure()}, or
 * {@link Permit#release()} when the outcome says nothing about dependency health (for example
 * the caller was interrupted). Each permit carries the breaker "generation" it was issued in, so
 * a slow call that started before a state change cannot close or re-open the circuit later.
 *
 * <p>All state lives in one immutable snapshot swapped with compare-and-set, so transitions are
 * atomic under concurrent callers and no thread ever blocks on the breaker.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    @FunctionalInterface
    public interface TransitionListener {
        void onTransition(State from, State to);

        TransitionListener NO_OP = (from, to) -> { };
    }

    private record Snapshot(
            State state,
            long generation,
            int consecutiveFailures,
            Instant openedAt,
            int halfOpenInFlight) {

        static Snapshot closed(long generation) {
            return new Snapshot(State.CLOSED, generation, 0, null, 0);
        }

        static Snapshot open(long generation, Instant openedAt) {
            return new Snapshot(State.OPEN, generation, 0, openedAt, 0);
        }

        static Snapshot halfOpen(long generation, Instant openedAt, int inFlight) {
            return new Snapshot(State.HALF_OPEN, generation, 0, openedAt, inFlight);
        }
    }

    private final int failureThreshold;
    private final Duration openDuration;
    private final int halfOpenMaxCalls;
    private final Clock clock;
    private final TransitionListener listener;
    private final AtomicReference<Snapshot> current = new AtomicReference<>(Snapshot.closed(0));

    public CircuitBreaker(
            int failureThreshold,
            Duration openDuration,
            int halfOpenMaxCalls,
            Clock clock,
            TransitionListener listener) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        if (halfOpenMaxCalls < 1) {
            throw new IllegalArgumentException("halfOpenMaxCalls must be >= 1");
        }
        Objects.requireNonNull(openDuration, "openDuration");
        if (openDuration.isNegative() || openDuration.isZero()) {
            throw new IllegalArgumentException("openDuration must be positive");
        }
        this.failureThreshold = failureThreshold;
        this.openDuration = openDuration;
        this.halfOpenMaxCalls = halfOpenMaxCalls;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    public State state() {
        return current.get().state();
    }

    /**
     * Returns a permit if the dependency may be called now, or empty if the caller must fail fast.
     */
    public Optional<Permit> tryAcquire() {
        while (true) {
            Snapshot snap = current.get();
            switch (snap.state()) {
                case CLOSED:
                    return Optional.of(new Permit(snap.generation()));

                case OPEN: {
                    Instant retryAt = snap.openedAt().plus(openDuration);
                    if (clock.instant().isBefore(retryAt)) {
                        return Optional.empty();
                    }
                    Snapshot next = Snapshot.halfOpen(snap.generation() + 1, snap.openedAt(), 1);
                    if (current.compareAndSet(snap, next)) {
                        notify(State.OPEN, State.HALF_OPEN);
                        return Optional.of(new Permit(next.generation()));
                    }
                    break; // lost the race; re-read and retry
                }

                case HALF_OPEN: {
                    if (snap.halfOpenInFlight() >= halfOpenMaxCalls) {
                        return Optional.empty();
                    }
                    Snapshot next = Snapshot.halfOpen(
                            snap.generation(), snap.openedAt(), snap.halfOpenInFlight() + 1);
                    if (current.compareAndSet(snap, next)) {
                        return Optional.of(new Permit(next.generation()));
                    }
                    break;
                }

                default:
                    throw new IllegalStateException("unknown state " + snap.state());
            }
        }
    }

    private void onSuccess(long generation) {
        while (true) {
            Snapshot snap = current.get();
            if (snap.generation() != generation) {
                return; // stale result from an earlier state; ignore
            }
            Snapshot next;
            switch (snap.state()) {
                case CLOSED:
                    if (snap.consecutiveFailures() == 0) {
                        return;
                    }
                    next = Snapshot.closed(snap.generation());
                    break;
                case HALF_OPEN:
                    next = Snapshot.closed(snap.generation() + 1);
                    break;
                default:
                    return; // OPEN never issues permits for its own generation
            }
            if (current.compareAndSet(snap, next)) {
                if (snap.state() != next.state()) {
                    notify(snap.state(), next.state());
                }
                return;
            }
        }
    }

    private void onFailure(long generation) {
        while (true) {
            Snapshot snap = current.get();
            if (snap.generation() != generation) {
                return;
            }
            Snapshot next;
            switch (snap.state()) {
                case CLOSED: {
                    int failures = snap.consecutiveFailures() + 1;
                    next = failures >= failureThreshold
                            ? Snapshot.open(snap.generation() + 1, clock.instant())
                            : new Snapshot(State.CLOSED, snap.generation(), failures, null, 0);
                    break;
                }
                case HALF_OPEN:
                    next = Snapshot.open(snap.generation() + 1, clock.instant());
                    break;
                default:
                    return;
            }
            if (current.compareAndSet(snap, next)) {
                if (snap.state() != next.state()) {
                    notify(snap.state(), next.state());
                }
                return;
            }
        }
    }

    private void onRelease(long generation) {
        while (true) {
            Snapshot snap = current.get();
            if (snap.generation() != generation || snap.state() != State.HALF_OPEN) {
                return; // only half-open slots need to be handed back
            }
            Snapshot next = Snapshot.halfOpen(
                    snap.generation(), snap.openedAt(), Math.max(0, snap.halfOpenInFlight() - 1));
            if (current.compareAndSet(snap, next)) {
                return;
            }
        }
    }

    private void notify(State from, State to) {
        try {
            listener.onTransition(from, to);
        } catch (RuntimeException ignored) {
            // A broken metrics/log listener must never break request handling.
        }
    }

    /** Single-use ticket; only the first outcome reported on it has any effect. */
    public final class Permit {
        private final long generation;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Permit(long generation) {
            this.generation = generation;
        }

        /** The dependency answered (an inactive-token answer is still a healthy dependency). */
        public void recordSuccess() {
            if (completed.compareAndSet(false, true)) {
                onSuccess(generation);
            }
        }

        /** Timeout, connection error, 5xx, saturated bulkhead, malformed response. */
        public void recordFailure() {
            if (completed.compareAndSet(false, true)) {
                onFailure(generation);
            }
        }

        /** Outcome is not evidence about dependency health (e.g. caller interrupted). */
        public void release() {
            if (completed.compareAndSet(false, true)) {
                onRelease(generation);
            }
        }
    }
}
