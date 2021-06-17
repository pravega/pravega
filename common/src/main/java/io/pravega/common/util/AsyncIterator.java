/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.util;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.SequentialProcessor;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.val;

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

    /**
     * Returns a new {@link AsyncIterator} that wraps this instance and converts all items from this one into items of a
     * new type.
     *
     * @param converter A {@link Function} that will convert T to U.
     * @param <U>       New type.
     * @return A new {@link AsyncIterator}.
     */
    default <U> AsyncIterator<U> thenApply(@NonNull Function<? super T, ? extends U> converter) {
        return () -> AsyncIterator.this.getNext().thenApply(item -> item == null ? null : converter.apply(item));
    }

    /**
     * Returns a new {@link AsyncIterator} that wraps this instance and converts all items from this one into items of a
     * new type using an async call.
     *
     * @param converter A {@link Function} that will convert T to U.
     * @param <U>       New type.
     * @return A new {@link AsyncIterator}.
     */
    default <U> AsyncIterator<U> thenCompose(@NonNull Function<? super T, CompletableFuture<U>> converter) {
        return () -> AsyncIterator.this.getNext()
                .thenCompose(item -> item == null ? CompletableFuture.completedFuture(null) : converter.apply(item));
    }

    /**
     * Returns an {@link Iterator} that wraps this instance.
     *
     * @return A new {@link BlockingAsyncIterator} wrapping this instance.
     */
    default Iterator<T> asIterator() {
        return new BlockingAsyncIterator<>(this);
    }

    /**
     * Returns an {@link AsyncIterator} with exactly one item.
     * @param item The Item to return
     * @param <T> Item type.
     * @return A singleton {@link AsyncIterator}.
     */
    static <T> AsyncIterator<T> singleton(T item) {
        val returned = new AtomicBoolean(false);
        return () -> CompletableFuture.completedFuture(returned.getAndSet(true) ? null : item);
    }
}
