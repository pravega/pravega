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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.VersionedMetadata;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import static io.pravega.shared.NameUtils.TRANSACTION_ID_COUNTER_TABLE;

/**
 * A Pravega tables based int 96 counter. At bootstrap, and upon each refresh, it retrieves a unique starting counter from the
 * Int96 space. It does this by getting and updating an Int96 value stored in Pravega Table. It updates the table value
 * with the next highest value that others could access.
 * So upon each retrieval from Pravega table, it has a starting counter value and a ceiling on counters it can assign until a refresh
 * is triggered. Any caller requesting for nextCounter will get a unique value from the range owned by this counter.
 * Once it exhausts the range, it refreshes the range by contacting Pravega Tables and repeating the steps described above.
 */

public class PravegaTablesInt96Counter implements Int96Counter {
    static final String COUNTER_KEY = "counter";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesInt96Counter.class));
    private final Object lock;
    @GuardedBy("lock")
    private final AtomicInt96 limit;
    @GuardedBy("lock")
    private final AtomicInt96 counter;
    @GuardedBy("lock")
    private CompletableFuture<Void> refreshFutureRef;
    private final PravegaTablesStoreHelper storeHelper;

    public PravegaTablesInt96Counter(final PravegaTablesStoreHelper storeHelper) {
        this.lock = new Object();
        this.counter = new AtomicInt96();
        this.limit = new AtomicInt96();
        this.refreshFutureRef = null;
        this.storeHelper = storeHelper;
    }

    @Override
    public CompletableFuture<Int96> getNextCounter(final OperationContext context) {
        CompletableFuture<Int96> future;
        synchronized (lock) {
            Int96 next = counter.incrementAndGet();
            if (next.compareTo(limit.get()) > 0) {
                // ignore the counter value and after refreshing call getNextCounter
                future = refreshRangeIfNeeded(context).thenCompose(x -> getNextCounter(context));
            } else {
                future = CompletableFuture.completedFuture(next);
            }
        }
        return future;
    }

    @VisibleForTesting
    CompletableFuture<Void> refreshRangeIfNeeded(final OperationContext context) {
        CompletableFuture<Void> refreshFuture;
        synchronized (lock) {
            // Ensure that only one background refresh is happening. For this we will reference the future in refreshFutureRef
            // If reference future ref is not null, we will return the reference to that future.
            // It is set to null when refresh completes.
            refreshFuture = this.refreshFutureRef;
            if (this.refreshFutureRef == null) {
                // no ongoing refresh, check if refresh is still needed
                if (counter.get().compareTo(limit.get()) >= 0) {
                    log.info("Refreshing counter range. Current counter is {}. Current limit is {}", counter.get(), limit.get());

                    // Need to refresh counter and limit. Start a new refresh future. We are under lock so no other
                    // concurrent thread can start the refresh future.
                    refreshFutureRef = getRefreshFuture(context)
                            .exceptionally(e -> {
                                // if any exception is thrown here, we would want to reset refresh future so that it can be retried.
                                synchronized (lock) {
                                    refreshFutureRef = null;
                                }
                                log.warn("Exception thrown while trying to refresh transaction counter range", e);
                                throw new CompletionException(e);
                            });
                    // Note: refreshFutureRef is reset to null under the lock, and since we have the lock in this thread
                    // until we release it, refresh future ref cannot be reset to null. So we will always return a non-null
                    // future from here.
                    refreshFuture = refreshFutureRef;
                } else {
                    // nothing to do
                    refreshFuture = CompletableFuture.completedFuture(null);
                }
            }
        }
        return refreshFuture;
    }

    @VisibleForTesting
    CompletableFuture<Void> getRefreshFuture(final OperationContext context) {
        return getCounterFromTable(context).thenCompose(data -> {
            Int96 previous = data.getObject();
            Int96 nextLimit = previous.add(COUNTER_RANGE);
            return storeHelper.updateEntry(TRANSACTION_ID_COUNTER_TABLE, COUNTER_KEY, nextLimit,
                            Int96::toBytes, data.getVersion(), context.getRequestId())
                    .thenAccept(x -> {
                        // Received new range, we should reset the counter and limit under the lock
                        // and then reset refreshfutureref to null
                        synchronized (lock) {
                            // Note: counter is set to previous range's highest value. Always get the
                            // next counter by calling counter.incrementAndGet otherwise there will
                            // be a collision with counter used by someone else.
                            counter.set(previous.getMsb(), previous.getLsb());
                            limit.set(nextLimit.getMsb(), nextLimit.getLsb());
                            refreshFutureRef = null;
                            log.info(context.getRequestId(), "Refreshed counter range. Current counter is {}. Current limit is {}",
                                    counter.get(), limit.get());
                        }
                    });
        });
    }

    private CompletableFuture<VersionedMetadata<Int96>> getCounterFromTable(final OperationContext context) {
        return Futures.exceptionallyComposeExpecting(storeHelper.getEntry(TRANSACTION_ID_COUNTER_TABLE,
                        COUNTER_KEY, Int96::fromBytes, context.getRequestId()),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException,
                () -> storeHelper.createTable(TRANSACTION_ID_COUNTER_TABLE, context.getRequestId())
                        .thenAccept(v -> log.info(context.getRequestId(), "batches root table {} created", TRANSACTION_ID_COUNTER_TABLE))
                        .thenCompose(v -> storeHelper.addNewEntryIfAbsent(TRANSACTION_ID_COUNTER_TABLE,
                                COUNTER_KEY, Int96.ZERO, Int96::toBytes, context.getRequestId()))
                        .thenCompose(v -> storeHelper.getEntry(TRANSACTION_ID_COUNTER_TABLE,
                                COUNTER_KEY, Int96::fromBytes, context.getRequestId())));
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
}