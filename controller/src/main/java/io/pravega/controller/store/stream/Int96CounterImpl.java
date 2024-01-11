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

package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.SequentialProcessor;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.common.tracing.TagLogger;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
  * Int96CounterImpl implements getNextCounter using the Int96Counter interface.
  */
class Int96CounterImpl implements Int96Counter {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(Int96CounterImpl.class));

    @VisibleForTesting
    final BiFunction<CounterInfo, BiConsumer<Int96, Int96>, CompletableFuture<Void>> function;

    private final Object lock;
    @GuardedBy("lock")
    private final AtomicInt96 limit;
    @GuardedBy("lock")
    private final AtomicInt96 counter;
    private final SequentialProcessor sequentialProcessor;


    public Int96CounterImpl(final BiFunction<CounterInfo, BiConsumer<Int96, Int96>, CompletableFuture<Void>> function,
                            Executor executor) {
        this.lock = new Object();
        this.counter = new AtomicInt96();
        this.limit = new AtomicInt96();
        this.function = function;
        this.sequentialProcessor = new SequentialProcessor(executor);
    }

    @Override
    public CompletableFuture<Int96> getNextCounter(OperationContext context) {
        CompletableFuture<Int96> future;
        Int96 next;
        Int96 currentLimit;
        synchronized (lock) {
            next = counter.incrementAndGet();
            currentLimit = limit.get();
        }
        if (next.compareTo(currentLimit) > 0) {
            // ignore the counter value and after refreshing call getNextCounter
            future = refreshRangeIfNeeded(context).thenCompose(x -> getNextCounter(context));
        } else {
            future = CompletableFuture.completedFuture(next);
        }
        return future;
    }

     @VisibleForTesting
     void reset(Int96 previous, Int96 nextLimit) {
        // Received new range, we should reset the counter and limit under the lock
        // and then reset refreshfutureref to null
        synchronized (lock) {
            // Note: counter is set to previous range's highest value. Always get the
            // next counter by calling counter.incrementAndGet otherwise there will
            // be a collision with counter used by someone else.
            counter.set(previous.getMsb(), previous.getLsb());
            limit.set(nextLimit.getMsb(), nextLimit.getLsb());
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> refreshRangeIfNeeded(final OperationContext context) {
        return sequentialProcessor.add(() -> {
            Int96 currentCounter;
            Int96 currentLimit;
            synchronized (lock) {
                currentCounter = counter.get();
                currentLimit = limit.get();
            }
            // no ongoing refresh, check if refresh is still needed
            if (currentCounter.compareTo(currentLimit) >= 0) {
                log.info("Refreshing counter range. Current counter is {}. Current limit is {}", counter.get(), limit.get());
                return function.apply(new CounterInfo(context, COUNTER_RANGE), this::reset)
                        .thenAccept(result -> {
                            synchronized (lock) {
                                log.info(context.getRequestId(), "Refreshed counter range. Current counter is {}. Current limit is {}",
                                        counter.get(), limit.get());
                            }
                        })
                        .exceptionally(e -> {
                            log.warn("Exception thrown while trying to refresh transaction counter range", e);
                            throw new CompletionException(e);
                        });
            } else {
                // nothing to do
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    // region getters and setters for testing
    @VisibleForTesting
    void setCounterAndLimitForTesting(int counterMsb, long counterLsb, int limitMsb, long limitLsb) {
        synchronized (lock) {
            limit.set(limitMsb, limitLsb);
            counter.set(counterMsb, counterLsb);
        }
    }

    @VisibleForTesting
    Int96 getLimitForTesting() {
        synchronized (lock) {
            return limit.get();
        }
    }

    @VisibleForTesting
    Int96 getCounterForTesting() {
        synchronized (lock) {
            return counter.get();
        }
    }

    @Data
    @AllArgsConstructor
    static class CounterInfo {
        private final OperationContext operationContext;
        private final int range;
    }

}
