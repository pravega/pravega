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
package io.pravega.common.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * An async processor that executes items after a predetermined (yet fixed) delay of time.
 *
 * Items are added to the processor using {@link DelayedProcessor#process}. The "clock" starts ticking from the moment
 * they were added and the processor will guarantee that the item will not be processed before the preconfigured delay
 * time (note that it may be executed some time after the delay expired, mostly due to other items in front of it that
 * need execution as well).
 *
 * Each item must implement {@link Item} and hence provide a unique {@link Item#key()}. The Key is used to differentiate
 * between different instances of {@link Item} that refer to the same task (i.e., the same segment). If an {@link Item}
 * with a same Key is already queued up for processing (but has not yet finished executing), a call to {@link #process}
 * with it as argument will not add it again.
 * <p>
 * Items may be dequeued (cancelled) using {@link #cancel}. See that method for more details.
 *
 * @param <T> Type of item to process.
 */
@Slf4j
public class DelayedProcessor<T extends DelayedProcessor.Item> {
    //region Private

    private final Function<T, CompletableFuture<Void>> itemProcessor;
    private final Duration itemDelay;
    private final ScheduledExecutorService executor;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final AbstractTimer timer;
    private final DelayedQueue queue;
    private final CompletableFuture<Void> runTask;
    private volatile CompletableFuture<Void> currentIterationDelayTask;
    private final AtomicBoolean closed;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link DelayedProcessor} class.
     *
     * @param itemProcessor A Function that, when given an item of type T, returns a CompletableFuture which will be
     *                      completed when that item has finished processing. The {@link DelayedProcessor} does not
     *                      care if the CompletableFuture completes normally or exceptionally - it will consider the
     *                      item as processed when the CompletableFuture completes (one way or another).
     * @param itemDelay     A {@link Duration} indicating the minimum amount of time that must elapse between when an item
     *                      is first added using {@link #process} and when it is actually processed. Note that if multiple
     *                      items with the same {@link Item#key()} are added, only the time from the first invocation of
     *                      {@link #process} is counted.
     * @param executor      An Executor for async operations.
     * @param traceObjectId An identifier for logging purposes.
     */
    public DelayedProcessor(Function<T, CompletableFuture<Void>> itemProcessor, Duration itemDelay,
                            ScheduledExecutorService executor, String traceObjectId) {
        this.itemProcessor = itemProcessor;
        this.itemDelay = itemDelay;
        this.executor = executor;
        this.traceObjectId = traceObjectId;
        this.closed = new AtomicBoolean(false);
        this.timer = new Timer();
        this.queue = new DelayedQueue();
        this.runTask = start();
    }

    //endregion

    /**
     * Shuts down the Delay processor. No further items will be processed.
     * Outstanding items are returned. 
     * 
     * @return A list of unprocessed items.
     */
    public List<T> shutdown() {
        if (!this.closed.getAndSet(true)) {
            val delayTask = this.currentIterationDelayTask;
            if (delayTask != null) {
                delayTask.cancel(true);
            }
            if (!executor.isShutdown()) {
                Futures.await(runTask, 10000);
            }
            log.info("{}: Closed.", this.traceObjectId);
            return this.queue.drain();
        }
        return ImmutableList.of();
    }

    //endregion

    //region Operations

    /**
     * Processes a new {@link Item}. The item will NOT be queued up if there exists another {@link Item}with the same
     * {@link Item#key()} already in the queue.
     *
     * The {@link Item} will be processed using the handler passed in via this class' constructor at the earliest after
     * the delay specified in this class' constructor.
     *
     * @param item The Item to process.
     */
    public void process(@NonNull T item) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.queue.add(item);
    }

    /**
     * Cancels the {@link Item} with given key, if any. If the given {@link Item} hasn't yet begun executing, it will
     * be removed from the queue. If it is in the process of executing, the execution cannot be stopped and this call
     * will have no effect.
     *
     * @param key The Item's key.
     */
    public void cancel(@NonNull String key) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.queue.remove(key);
    }

    /**
     * Gets a value indicating the number of {@link Item}s currently pending (including currently executing) in the processor.
     *
     * @return The number of pending items.
     */
    @VisibleForTesting
    int size() {
        return this.queue.size();
    }

    private CompletableFuture<Void> start() {
        log.info("{}: Started. Iteration Delay = {} ms.", this.traceObjectId, this.itemDelay.toMillis());
        return Futures.loop(
                () -> !this.closed.get(),
                () -> delay().thenComposeAsync(v -> runOneIteration()),
                this.executor);
    }

    private CompletableFuture<Void> runOneIteration() {
        if (this.closed.get()) {
            log.debug("{}: Not running iteration due to shutting down.", this.traceObjectId);
            return CompletableFuture.completedFuture(null);
        }

        val firstItem = this.queue.peekFirst();
        if (firstItem == null || firstItem.getRemainingMillis() > 0) {
            log.debug("{}: Not running iteration due premature wake-up.", this.traceObjectId);
            return CompletableFuture.completedFuture(null);
        }

        return this.itemProcessor.apply(firstItem.getWrappedItem())
                .handle((r, ex) -> {
                    if (ex != null) {
                        log.error("{}: Unable to process {}.", this.traceObjectId, firstItem, ex);
                    }
                    this.queue.removeFirstIf(firstItem);
                    return null;
                });
    }

    private CompletableFuture<Void> delay() {
        val delay = calculateDelay();
        log.debug("{}: Iteration delay = {} ms. Queue size = {}.", this.traceObjectId, delay.toMillis(), this.queue.size());
        val result = createDelayedFuture(delay)
                .whenCompleteAsync((r, ex) -> this.currentIterationDelayTask = null, this.executor);
        this.currentIterationDelayTask = result;
        return result;
    }

    @VisibleForTesting
    protected CompletableFuture<Void> createDelayedFuture(Duration delay) {
        return Futures.delayedFuture(delay, this.executor);
    }

    /**
     * Calculates the iteration delay as the minimum of {@link #itemDelay} and the remaining time of the first item in
     * the {@link DelayedQueue} ({@link QueueItem#getRemainingMillis()}.
     *
     * @return The Iteration delay.
     */
    private Duration calculateDelay() {
        val firstItem = this.queue.peekFirst();
        val firstItemDuration = firstItem == null ? this.itemDelay : Duration.ofMillis(firstItem.getRemainingMillis());
        return firstItemDuration.compareTo(this.itemDelay) < 0 ? firstItemDuration : this.itemDelay;
    }

    //endregion

    //region Helper Classes

    /**
     * Internal queue for the {@link DelayedProcessor}.
     */
    private class DelayedQueue {
        @GuardedBy("this")
        private final Set<String> keys = new HashSet<>();
        @GuardedBy("this")
        private final Deque<QueueItem> queue = new ArrayDeque<>();

        synchronized List<T> drain() {
            this.keys.clear();
            val result = new ArrayList<T>();
            for (DelayedProcessor<T>.QueueItem item : queue) {
                result.add(item.getWrappedItem());
            }
            this.queue.clear();
            return result;
        }

        synchronized int size() {
            return this.queue.size();
        }

        synchronized void add(T item) {
            if (this.keys.add(item.key())) {
                this.queue.add(new QueueItem(item));
            }
        }

        synchronized void remove(String key) {
            this.keys.remove(key);
            this.queue.removeIf(i -> i.getWrappedItem().key().equals(key));
        }

        synchronized QueueItem peekFirst() {
            return this.queue.peekFirst();
        }

        synchronized void removeFirstIf(QueueItem firstItem) {
            QueueItem item = this.queue.pollFirst();
            if (item != firstItem) {
                // Optimistic removal. Most of the time the first item will match what we expect, but in case the item
                // has been cancelled while we're processing it, the first item may not match. If so, we'll want to add
                // it back where it belongs.
                this.queue.addFirst(item);
                return;
            }

            if (item != null) {
                this.keys.remove(item.getWrappedItem().key());
            }
        }
    }

    @Getter
    @ToString
    private class QueueItem {
        private final T wrappedItem;
        private final long addedTime; // No practical use, but useful for debugging (in heap dumps, for example).
        private final long expirationTime;

        QueueItem(T wrappedItem) {
            this.wrappedItem = wrappedItem;
            this.addedTime = getTimer().getElapsedMillis();
            this.expirationTime = this.addedTime + itemDelay.toMillis();
        }

        long getRemainingMillis() {
            return Math.max(0, this.expirationTime - getTimer().getElapsedMillis());
        }
    }

    /**
     * Defines a {@link DelayedProcessor} item to process..
     */
    public interface Item {
        /**
         * Item Key. Uniquely defines the Item within the processor.
         *
         * @return The key.
         */
        String key();
    }

    //endregion
}
