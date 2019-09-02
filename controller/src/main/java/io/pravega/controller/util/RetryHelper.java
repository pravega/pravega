/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.util;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RetryHelper {

    public static final Predicate<Throwable> RETRYABLE_PREDICATE = e -> {
        Throwable t = Exceptions.unwrap(e);
        return RetryableException.isRetryable(t) || (t instanceof CheckpointStoreException &&
                ((CheckpointStoreException) t).getType().equals(CheckpointStoreException.Type.Connectivity));
    };

    public static final Predicate<Throwable> UNCONDITIONAL_PREDICATE = e -> true;

    public static <U> U withRetries(Supplier<U> supplier, Predicate<Throwable> predicate, int numOfTries) {
        return Retry.withExpBackoff(100, 2, numOfTries, 1000)
                .retryWhen(predicate)
                .run(supplier::get);
    }

    public static <U> CompletableFuture<U> withRetriesAsync(Supplier<CompletableFuture<U>> futureSupplier, Predicate<Throwable> predicate, int numOfTries,
                                                            ScheduledExecutorService executor) {
        return Retry
                .withExpBackoff(100, 2, numOfTries, 10000)
                .retryWhen(predicate)
                .runAsync(futureSupplier, executor);
    }

    public static <U> CompletableFuture<U> withIndefiniteRetriesAsync(Supplier<CompletableFuture<U>> futureSupplier,
                                                                      Consumer<Throwable> exceptionConsumer,
                                                                      ScheduledExecutorService executor) {
        return Retry
                .indefinitelyWithExpBackoff(100, 2, 10000, exceptionConsumer)
                .runAsync(futureSupplier, executor);
    }

    public static CompletableFuture<Void> loopWithDelay(Supplier<Boolean> condition, Supplier<CompletableFuture<Void>> loopBody, long delay,
                                                         ScheduledExecutorService executor) {
        return Futures.loop(condition, () -> Futures.delayedFuture(loopBody, delay, executor), executor);
    }
}
