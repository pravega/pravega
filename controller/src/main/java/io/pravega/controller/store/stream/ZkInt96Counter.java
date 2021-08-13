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
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.controller.store.ZKStoreHelper;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A zookeeper based int 96 counter. At bootstrap, and upon each refresh, it retrieves a unique starting counter from the 
 * Int96 space. It does this by getting and updating an Int96 value stored in zookeeper. It updates zookeeper node with the 
 * next highest value that others could access. 
 * So upon each retrieval from zookeeper, it has a starting counter value and a ceiling on counters it can assign until a refresh
 * is triggered. Any caller requesting for nextCounter will get a unique value from the range owned by this counter. 
 * Once it exhausts the range, it refreshes the range by contacting zookeeper and repeating the steps described above. 
 */
@Slf4j
public class ZkInt96Counter {
    /**
     * This constant defines the size of the block of counter values that will be used by this controller instance.
     * The controller will try to get current counter value from zookeeper. It then tries to update the value in store
     * by incrementing it by COUNTER_RANGE. If it is able to update the new value successfully, then this controller
     * can safely use the block `previous-value-in-store + 1` to `previous-value-in-store + COUNTER_RANGE` No other controller
     * will use this range for transaction id generation as it will be unique assigned to current controller.
     * If controller crashes, all unused values go to waste. In worst case we may lose COUNTER_RANGE worth of values everytime
     * a controller crashes.
     */
    @VisibleForTesting
    static final int COUNTER_RANGE = 10000;
    @VisibleForTesting
    static final String COUNTER_PATH = "/counter";

    private final Object lock;
    @GuardedBy("lock")
    private final AtomicInt96 limit;
    @GuardedBy("lock")
    private final AtomicInt96 counter;
    @GuardedBy("lock")
    private CompletableFuture<Void> refreshFutureRef;
    private ZKStoreHelper storeHelper;

    public ZkInt96Counter(ZKStoreHelper storeHelper) {
        this.lock = new Object();
        this.counter = new AtomicInt96();
        this.limit = new AtomicInt96();
        this.refreshFutureRef = null;
        this.storeHelper = storeHelper;
    }

    CompletableFuture<Int96> getNextCounter() {
        CompletableFuture<Int96> future;
        synchronized (lock) {
            Int96 next = counter.incrementAndGet();
            if (next.compareTo(limit.get()) > 0) {
                // ignore the counter value and after refreshing call getNextCounter
                future = refreshRangeIfNeeded().thenCompose(x -> getNextCounter());
            } else {
                future = CompletableFuture.completedFuture(next);
            }
        }
        return future;
    }

    @VisibleForTesting
    CompletableFuture<Void> refreshRangeIfNeeded() {
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
                    refreshFutureRef = getRefreshFuture()
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
    CompletableFuture<Void> getRefreshFuture() {
        return storeHelper
                .createZNodeIfNotExist(COUNTER_PATH, Int96.ZERO.toBytes())
                .thenCompose(v -> storeHelper.getData(COUNTER_PATH, Int96::fromBytes)
                           .thenCompose(data -> {
                               Int96 previous = data.getObject();
                               Int96 nextLimit = previous.add(COUNTER_RANGE);
                               return storeHelper.setData(COUNTER_PATH, nextLimit.toBytes(), data.getVersion())
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
                                             log.info("Refreshed counter range. Current counter is {}. Current limit is {}",
                                                     counter.get(), limit.get());
                                         }
                                     });
                           }));
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
