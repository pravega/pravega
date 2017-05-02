/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.util;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.util.Retry;
import io.pravega.server.controller.service.retryable.RetryableException;
import io.pravega.server.controller.service.store.checkpoint.CheckpointStoreException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RetryHelper {

    public static final Predicate<Throwable> RETRYABLE_PREDICATE = e -> {
        Throwable t = ExceptionHelpers.getRealException(e);
        return RetryableException.isRetryable(t) || (t instanceof CheckpointStoreException &&
                ((CheckpointStoreException) t).getType().equals(CheckpointStoreException.Type.Connectivity));
    };

    public static final Predicate<Throwable> UNCONDITIONAL_PREDICATE = e -> true;

    public static <U> U withRetries(Supplier<U> supplier, Predicate<Throwable> predicate, int numOfTries) {
        return Retry.withExpBackoff(100, 2, numOfTries, 1000)
                .retryWhen(predicate)
                .throwingOn(RuntimeException.class)
                .run(supplier::get);
    }

    public static <U> CompletableFuture<U> withRetriesAsync(Supplier<CompletableFuture<U>> futureSupplier, Predicate<Throwable> predicate, int numOfTries,
                                                            ScheduledExecutorService executor) {
        return Retry
                .withExpBackoff(100, 2, numOfTries, 10000)
                .retryWhen(predicate)
                .throwingOn(RuntimeException.class)
                .runAsync(futureSupplier, executor);
    }
}