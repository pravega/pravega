/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.shared.common.util.Retry;
import com.emc.pravega.controller.retryable.RetryableException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

class TaskStepsRetryHelper {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(5).toMillis();
    private static final Retry.RetryAndThrowConditionally<RuntimeException> RETRY = Retry
            .withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
            .retryWhen(RetryableException::isRetryable)
            .throwingOn(RuntimeException.class);

    static <U> CompletableFuture<U> withRetries(Supplier<CompletableFuture<U>> futureSupplier, ScheduledExecutorService executor) {
        return RETRY.runAsync(futureSupplier, executor);
    }
}
