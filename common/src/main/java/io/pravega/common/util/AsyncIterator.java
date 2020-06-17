/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.SequentialProcessor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Defines an Iterator for which every invocation results in an async call with a delayed response.
 *
 * @param <T> Element type.
 */
public interface AsyncIterator<T> {
    /**
     * Attempts to get the next element in the iteration.
     *
     * Note: since this is an async call, it is possible to invoke this method before the previous call to it completed;
     * in this case the behavior is undefined and, depending on the actual implementation, the internal state of the
     * {@link AsyncIterator} may get corrupted. Consider invoking {@link #asSequential(Executor)} which will provide a
     * thin wrapper on top of this instance that serializes calls to this method.
     *
     * @return A CompletableFuture that, when completed, will contain the next element in the iteration. If the iteration
     * has reached its end, this will complete with null. If an exception occurred, this will be completed exceptionally
     * with the causing exception, and the iteration will end.
     */
    CompletableFuture<T> getNext();

    /**
     * Processes the remaining elements in the AsyncIterator.
     *
     * @param consumer A Consumer that will be invoked for each remaining element. The consumer will be invoked using the
     *                 given Executor, but any new invocation will wait for the previous invocation to complete.
     * @param executor An Executor to run async tasks on.
     * @return A CompletableFuture that, when completed, will indicate that the processing is complete.
     */
    default CompletableFuture<Void> forEachRemaining(Consumer<? super T> consumer, Executor executor) {
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return Futures.loop(
                canContinue::get,
                this::getNext,
                e -> {
                    if (e == null) {
                        canContinue.set(false);
                    } else {
                        consumer.accept(e);
                    }
                }, executor);
    }

    /**
     * Returns a new {@link AsyncIterator} that wraps this instance which serializes the execution of all calls to
     * {@link #getNext()} (no two executions of {@link #getNext()} will ever execute at the same time; they will be
     * run in the order of invocation, but only after the previous one completes).
     *
     * @param executor An Executor to run async tasks on.
     * @return A new {@link AsyncIterator}.
     */
    default AsyncIterator<T> asSequential(Executor executor) {
        SequentialProcessor processor = new SequentialProcessor(executor);
        return () -> {
            CompletableFuture<T> result = processor.add(AsyncIterator.this::getNext);
            result.thenAccept(r -> {
                if (r == null) {
                    processor.close();
                }
            });
            return result;
        };
    }

    /**
     * Processes the remaining elements in the AsyncIterator until the specified {@link Predicate} returns false.
     *
     * @param collector A {@link Predicate} that decides if iterating over the collection can continue.
     * @return A CompletableFuture that, when completed, will indicate that the processing is complete.
     */
    default CompletableFuture<Void> collectRemaining(Predicate<? super T> collector) {
        return getNext().thenCompose(e -> {
            boolean canContinue = e != null && collector.test(e);
            if (canContinue) {
                return collectRemaining(collector);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }
}
