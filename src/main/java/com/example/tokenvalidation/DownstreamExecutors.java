package com.example.tokenvalidation;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory for the bounded "bulkhead" pool that isolates downstream calls from request threads. */
public final class DownstreamExecutors {
    private DownstreamExecutors() {
    }

    /**
     * Fixed-size pool with a bounded queue that rejects (never blocks, never grows) when full.
     * A rejection is surfaced to the caller as "temporarily unavailable" immediately.
     */
    public static ExecutorService bounded(String name, int threads, int queueCapacity) {
        if (threads < 1) {
            throw new IllegalArgumentException("threads must be >= 1");
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("queueCapacity must be >= 0");
        }
        BlockingQueue<Runnable> queue = queueCapacity == 0
                ? new SynchronousQueue<>()
                : new ArrayBlockingQueue<>(queueCapacity);
        return new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                queue,
                namedDaemonThreads(name),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static ThreadFactory namedDaemonThreads(String name) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, name + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
