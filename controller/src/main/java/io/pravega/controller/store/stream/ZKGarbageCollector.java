/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.Data;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Garbage Collector class which periodically executes garbage collection over some data in zookeeper.
 * This is a generic class which takes a garbage collection Identifier (gcName) and a garbage collection lambda and periodically
 * executes the lambda.
 * This also implements the Batching logic and the leader keeps generating new long based batch ids in
 * each GC cycle. It is responsibility of the user to make use of the `batch-id` to group its data into these batches.
 * Upon each periodic cycle, this class checks if a new batch should be generated, and then it executes the supplied gc lambda.
 *
 * All controller instances compete to become leader for GC workflow. The leader is responsible for maintaining the batch id
 * and execute gc workflow.
 * Users of this class can request the latest batch id from it anytime.
 *
 * The current batch is identified by a znode. All controller instances register a watch on this znode. And whenever leader changes
 * the current batch, they all receive the latest update.
 */
@Slf4j
class ZKGarbageCollector extends AbstractService implements AutoCloseable {
    private static final String GC_ROOT = "/gc/%s";
    private static final String BATCH_PATH = GC_ROOT + "/batch";
    private static final String LEADER_PATH = GC_ROOT + "/leader";

    private final ZKStoreHelper zkStoreHelper;
    private final AtomicLong currentBatch;
    private final AtomicReference<CompletableFuture<Void>> gcLoop;
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private final Supplier<CompletableFuture<Void>> gcProcessing;
    private final String gcName;
    private final String leaderPath;
    private final String batchPath;
    private final AtomicReference<NodeCache> watch;
    private final AtomicReference<LeaderLatch> leaderLatch;
    // dedicated thread executor for GC which will not interfere with rest of the processing.
    private final ScheduledExecutorService gcExecutor;
    private final long periodInMillis;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final CompletableFuture<Void> acquiredLeadership = new CompletableFuture<>();
    
    ZKGarbageCollector(String gcName, ZKStoreHelper zkStoreHelper, Supplier<CompletableFuture<Void>> gcProcessing, Duration gcPeriod) {
        Preconditions.checkNotNull(zkStoreHelper);
        Preconditions.checkNotNull(gcProcessing);
        Preconditions.checkArgument(gcPeriod != null && !gcPeriod.isNegative());

        this.currentBatch = new AtomicLong();
        this.gcName = gcName;
        this.leaderLatch = new AtomicReference<>();
        this.leaderPath = String.format(LEADER_PATH, gcName);
        this.watch = new AtomicReference<>();
        this.batchPath = String.format(BATCH_PATH, gcName);
        this.zkStoreHelper = zkStoreHelper;
        this.gcProcessing = gcProcessing;
        this.periodInMillis = gcPeriod.toMillis();
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor();
        this.gcLoop = new AtomicReference<>();
    }

    @Override
    protected void doStart() {
        long quantized = computeNewBatch();
        byte[] b = getBytes(quantized);
        // 1. create znode for storing latest batch id. If the batch id exists, get the value from the store.
        // We will later register watch on the path and keep receiving any changes to its value.
        zkStoreHelper.createZNodeIfNotExist(batchPath, b)
                .thenCompose(x -> zkStoreHelper.getData(batchPath))
                .thenAccept(data -> {
                    currentBatch.set(BitConverter.readLong(data.getData(), 0));
                })
                .thenRun(this::initialize)
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                    latch.complete(null);
                });
    }

    @Override
    protected void doStop() {
        latch.thenAccept(v -> {
            if (gcLoop.get() != null) {
                gcLoop.get().cancel(true);
                gcLoop.get().whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStopped();
                    }
                });
            } else {
                notifyStopped();
            }
        });
    }

    public Long getLatestBatch() {
        return currentBatch.get();
    }

    @SneakyThrows(Exception.class)
    private void initialize() {
        // 1. register watch on batch path
        watch.set(registerWatch(batchPath));

        // 2. attempt to acquire leadership
        leaderLatch.set(electLeader(leaderPath, acquiredLeadership));

        // 3. if this acquires leadership, then schedule periodic garbage collection.
        // Note: This will attempt first GC only after one period elapses. This will ensure that even if there were 
        // back to back leadership changes with each having their own clock skews, a completed transaction record
        // will at least be present for one GC period. 
        // The downside is, if there are controller failures before GC period, then GC keeps getting delayed perpetually. 
        gcLoop.set(acquiredLeadership.thenCompose(x -> Futures.loop(this::isRunning, () -> Futures.delayedFuture(this::process,
                periodInMillis, gcExecutor), gcExecutor)));

        log.info("GC {} initialized", gcName);
    }

    private byte[] getBytes(long quantized) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, quantized);
        return b;
    }

    private long getLong(byte[] data) {
        return BitConverter.readLong(data, 0);
    }

    private long computeNewBatch() {
        return System.currentTimeMillis() / periodInMillis;
    }

    @VisibleForTesting
    CompletableFuture<Void> process() {
        log.info("Starting GC {}", gcName);
        
        // This method is called from periodic GC thread and can only be called if this is the leader.
        // Compute new batch id.
        long quantized = computeNewBatch();
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        if (quantized > currentBatch.get()) {
            // Update current group in the store. We will get the update from the watch and update current batch locally there. 
            future = future.thenCompose(x -> zkStoreHelper.setData(batchPath, new Data<>(getBytes(quantized), null)));
        }

        // Execute GC work supplied by the creator. We will log and ignore any exceptions in GC. The GC will be reattempted in
        // next cycle.
        return future.thenComposeAsync(x -> gcProcessing.get(), gcExecutor)
                .exceptionally(e -> {
                    // if GC failed, it will be tried again in the next cycle. So log and ignore.
                    log.error("Garbage collection for {} failed", gcName, e);
                    return null;
                });
    }

    @SneakyThrows(Exception.class)
    private LeaderLatch electLeader(String leaderPath, CompletableFuture<Void> acquireLeadershipLatch) {
        LeaderLatch leaderLatch = new LeaderLatch(zkStoreHelper.getClient(), leaderPath);
        LeaderLatchListener leaderListener = new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("GC {} acquired leadership.", gcName);
                acquireLeadershipLatch.complete(null);
            }

            @Override
            public void notLeader() {
                // this can happen if only zk session expires.
                log.warn("GC {} lost leadership.", gcName);
            }
        };

        leaderLatch.addListener(leaderListener);
        leaderLatch.start();

        return leaderLatch;
    }

    @SneakyThrows(Exception.class)
    private NodeCache registerWatch(String watchPath) {
        NodeCache nodeCache = new NodeCache(zkStoreHelper.getClient(), watchPath);
        NodeCacheListener watchListener = () -> {
            currentBatch.set(getLong(nodeCache.getCurrentData().getData()));
            log.debug("Current batch for {} changed to {}", gcName, currentBatch.get());
        };

        nodeCache.getListenable().addListener(watchListener);

        nodeCache.start();
        return nodeCache;
    }

    @SneakyThrows(Exception.class)
    @Override
    public void close() {
        if (watch.get() != null) {
            watch.get().close();
        }

        if (leaderLatch.get() != null) {
            leaderLatch.get().close();
        }

        gcExecutor.shutdown();
    }
}
