/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

class TaskStepsRetryHelper {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(10).toMillis();
    private static final Retry.RetryAndThrowExceptionally<RetryableException, RuntimeException> RETRY = Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
            .retryingOn(RetryableException.class)
            .throwingOn(RuntimeException.class);

    static <U> CompletableFuture<U> withRetries(Supplier<CompletableFuture<U>> future, ScheduledExecutorService executor) {
        return RETRY.runAsync(future, executor);
    }

    static <U> CompletableFuture<U> withWireCommandHandling(CompletableFuture<U> future) {
        return future.handle((res, ex) -> {
            if (ex != null) {
                Throwable cause = ExceptionHelpers.getRealException(ex);
                if (cause instanceof WireCommandFailedException) {
                    throw (WireCommandFailedException) cause;
                } else {
                    throw new RuntimeException(ex);
                }
            }
            return res;
        });
    }
}
