/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.concurrent.Futures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Defines an Iterator for which every invocation results in an async call with a delayed response.
 *
 * @param <T> Element type.
 */
public interface AsyncIterator<T> {
    /**
     * Attempts to get the next element in the iteration.
     *
     * Note: since this is an async call, it is possible to invoke getNext() before the previous call completed; in this
     * case the expected behavior is that the second call will be rejected with an {@link IllegalStateException}.
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
     * Returns a new AsyncIterator instance wrapping this one that will have a different element type.
     *
     * @param converter A Function that will convert {@link T} to {@link V}, which will be invoked after every call to
     *                  {@link #getNext()} to transform the original input into the desired type.
     * @param <V>       New Element type.
     * @return A new AsyncIterator with the converter applied.
     */
    default <V> AsyncIterator<V> apply(Function<T, V> converter) {
        return () -> getNext().thenApply(input -> input == null ? null : converter.apply(input));
    }
}