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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.util.RetryHelper;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

/**
 * Garbage Collector class performs two functions periodically.
 * 1. It takes a garbage collection lambda and periodically executes while performing a loose coordination with other distributed
 * instances that exactly one execution of lambda per batch interval is performed.
 * 2. This also implements the Batching logic and every batch interval, before executing the lambda, it also increments the
 * batch id. What the user of this batch id does is outside the purview of this class but typically users may make use of the
 * `batch-id` to group its data into batches.
 *
 * Users of this class can request the latest batch id from it anytime.
 *
 * The current batch is identified by a znode. All controller instances register a watch on this znode. And whenever batch
 * is updated, all watchers receive the latest update.
 */
@SuppressWarnings("deprecation")
@Slf4j
class ZKGarbageCollector extends AbstractService {
    private static final String GC_ROOT = "/garbagecollection/%s";
    private static final String GUARD_PATH = GC_ROOT + "/guard";

    private final ZKStoreHelper zkStoreHelper;
    private final AtomicReference<CompletableFuture<Void>> gcLoop;
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private final Supplier<CompletableFuture<Void>> gcProcessingSupplier;
    private final String gcName;
    private final String guardPath;
    private final AtomicReference<NodeCache> watch;
    // dedicated thread executor for GC which will not interfere with rest of the processing.
    private final ScheduledExecutorService gcExecutor;
    private final long periodInMillis;
    private final AtomicInteger currentBatch;
    private final AtomicInteger latestVersion;

    ZKGarbageCollector(String gcName, ZKStoreHelper zkStoreHelper, Supplier<CompletableFuture<Void>> gcProcessingSupplier, Duration gcPeriod) {
        Preconditions.checkNotNull(zkStoreHelper);
        Preconditions.checkNotNull(gcProcessingSupplier);
        Preconditions.checkArgument(gcPeriod != null && !gcPeriod.isNegative());

        this.gcName = gcName;
        this.guardPath = String.format(GUARD_PATH, gcName);
        this.watch = new AtomicReference<>();
        this.zkStoreHelper = zkStoreHelper;
        this.gcProcessingSupplier = gcProcessingSupplier;
        this.periodInMillis = gcPeriod.toMillis();
        this.gcExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "gc");
        this.gcLoop = new AtomicReference<>();
        this.currentBatch = new AtomicInteger(0);
        this.latestVersion = new AtomicInteger(0);
    }

    @Override
    protected void doStart() {
        // Create znode for storing latest batch id. If the batch id exists, get the value from the store.
        // We will later register watch on the path and keep receiving any changes to its value.
        RetryHelper.withRetriesAsync(() -> zkStoreHelper.createZNodeIfNotExist(guardPath)
                        .thenCompose(v -> fetchVersion().thenAccept(r -> currentBatch.set(latestVersion.get())))
                        .thenAccept(v -> watch.compareAndSet(null, registerWatch(guardPath))),
                RetryHelper.RETRYABLE_PREDICATE, 5, gcExecutor)
                .whenComplete((r, e) -> {
                    if (e == null) {
                        notifyStarted();
                        gcLoop.set(Futures.loop(this::isRunning, () -> Futures.delayedFuture(this::process, periodInMillis, gcExecutor), gcExecutor));
                    } else {
                        notifyFailed(e);
                    }
                    latch.complete(null);
                });
    }

    @Override
    protected void doStop() {
        latch.thenAccept(v -> {
            CompletableFuture<Void> gcLoopFuture = gcLoop.updateAndGet(x -> {
                if (x != null) {
                    x.cancel(true);
                    x.whenComplete((r, e) -> {
                        if (e != null && !(Exceptions.unwrap(e) instanceof CancellationException)) {
                            log.warn("Exception while trying to stop GC {}", gcName, e);
                            notifyFailed(e);
                        } else {
                            notifyStopped();
                        }
                    });
                }
                return x;
            });

            if (gcLoopFuture == null) {
                notifyStopped();
            }
        });
        
        watch.getAndUpdate(x -> {
            if (x != null) {
                try {
                    x.close();
                } catch (IOException e) {
                    throw Exceptions.sneakyThrow(e);
                }
            }
            return x;
        });

        gcExecutor.shutdownNow();
    }

    int getLatestBatch() {
        return currentBatch.get();
    }

    @VisibleForTesting
    CompletableFuture<Void> process() {
        return zkStoreHelper.setData(guardPath, new byte[0], new Version.IntVersion(latestVersion.get()))
                .thenComposeAsync(r -> {
                    // If we reach here, we were able to update the guard and have loose exclusive rights on batch update.
                    // Note: Each change of guard will guarantee at least one batch internal cycle before another instance is able
                    // to update the guard.
                    // It is important to understand that since we are not doing strict leader election, if the GC lambda
                    // execution takes longer than one batch cycle, there may be two controller instances running the GC
                    // lambda concurrently.
                    // Hence this is loose rights guarantee as opposed to a leader election. But practically we will have
                    // large GC intervals and small execution for GC will mean exclusion.
                    log.info("Acquired guard, starting GC iteration for {}", gcName);
                    // Also, the update to the version increases the batch id. All those watching, including this process,
                    // will get and update the batch id eventually.

                    // Execute GC work supplied by the creator. We will log and ignore any exceptions in GC. The GC will be reattempted in
                    // next cycle.
                    return gcProcessingSupplier.get();
                }, gcExecutor)
                // fetch the version and update it.
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof StoreException.WriteConflictException) {
                        log.debug("Unable to acquire guard. Will try in next cycle.");
                    } else {
                        // if GC failed, it will be tried again in the next cycle. So log and ignore.
                        if (unwrap instanceof StoreException.StoreConnectionException) {
                            log.warn("StoreConnectionException thrown during Garbage Collection iteration for {}.", gcName);
                        } else {
                            log.warn("Exception thrown during Garbage Collection iteration for {}. Log and ignore.", gcName, unwrap);
                        }
                    }
                    return null;
                }).thenCompose(v -> fetchVersion());

    }

    @VisibleForTesting
    CompletableFuture<Void> fetchVersion() {
        return zkStoreHelper.getData(guardPath, x -> x)
                .thenAccept(data -> latestVersion.set(data.getVersion().asIntVersion().getIntValue()));
    }
    
    @VisibleForTesting
    void setVersion(int newVersion) {
        latestVersion.set(newVersion);
    }

    @VisibleForTesting
    int getVersion() {
        return latestVersion.get();
    }

    @SneakyThrows(Exception.class)
    private NodeCache registerWatch(String watchPath) {
        NodeCache nodeCache = new NodeCache(zkStoreHelper.getClient(), watchPath);
        NodeCacheListener watchListener = () -> {
            currentBatch.set(nodeCache.getCurrentData().getStat().getVersion());
            log.debug("Current batch for {} changed to {}", gcName, currentBatch.get());
        };

        nodeCache.getListenable().addListener(watchListener);

        nodeCache.start();
        return nodeCache;
    }

}
