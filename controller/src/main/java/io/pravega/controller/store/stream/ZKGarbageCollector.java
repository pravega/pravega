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
import io.pravega.controller.util.Config;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Garbage Collector class which periodically executes garbage collection over some data in zookeeper.
 * This is a generic class which takes a garbage collection Identifier (gcName) and a garbage collection lambda and periodically
 * executes the lambda.
 * This also implements the Batcher interface and keeps generating batch ids in
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
class ZKGarbageCollector extends AbstractService implements Batcher, AutoCloseable {
    private static final String GC_ROOT = "/gc/%s";
    private static final String BATCH_PATH = GC_ROOT + "/batch";
    private static final String LEADER_PATH = GC_ROOT + "/leader";

    private final ZKStoreHelper zkStoreHelper;
    private final AtomicLong currentBatch;
    private CompletableFuture<Void> gcLoop;
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private final Supplier<CompletableFuture<Void>> gcProcessing;
    private final String gcName;
    private final String leaderPath;
    private final String batchPath;
    private PathChildrenCache watch;
    private LeaderLatch leaderLatch;
    // dedicated thread executor for GC which will not interfere with rest of the processing.
    private final ScheduledExecutorService gcExecutor;
    private final long periodInMillis;

    ZKGarbageCollector(String gcName, ZKStoreHelper zkStoreHelper, Supplier<CompletableFuture<Void>> gcProcessing) {
        this(gcName, zkStoreHelper, gcProcessing, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }

    @VisibleForTesting
    ZKGarbageCollector(String gcName, ZKStoreHelper zkStoreHelper, Supplier<CompletableFuture<Void>> gcProcessing, Duration gcPeriod) {
        Preconditions.checkNotNull(zkStoreHelper);
        Preconditions.checkNotNull(gcProcessing);
        Preconditions.checkArgument(gcPeriod != null && !gcPeriod.isNegative());

        this.currentBatch = new AtomicLong();
        this.gcName = gcName;
        this.leaderPath = String.format(LEADER_PATH, gcName);
        this.batchPath = String.format(BATCH_PATH, gcName);
        this.zkStoreHelper = zkStoreHelper;
        this.gcProcessing = gcProcessing;
        this.periodInMillis = gcPeriod.toMillis();
        this.gcExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    protected void doStart() {
        long quantized = computeNewBatch();
        byte[] b = getBytes(quantized);
        // 1. create znode for storing latest batch id. If the batch id exists, get the value from the store.
        // We will later register watch on the path and keep receiving any changes to its value.
        zkStoreHelper.createZNodeIfNotExist(BATCH_PATH, b)
                .thenCompose(x -> zkStoreHelper.getData(BATCH_PATH))
                .thenAccept(data -> currentBatch.set(BitConverter.readLong(data.getData(), 0)))
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
            if (gcLoop != null) {
                gcLoop.cancel(true);
                gcLoop.whenComplete((r, e) -> {
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

    @Override
    public long getLatestBatch() {
        return currentBatch.get();
    }

    @SneakyThrows
    private void initialize() {
        // 1. register watch on batch path
        watch = watch(batchPath);

        // 2. attempt to acquire leadership
        CompletableFuture<Void> acquiredLeadership = new CompletableFuture<>();
        leaderLatch = electLeader(leaderPath, acquiredLeadership);

        // 3. if this acquires leadership, then schedule periodic garbage collection.
        gcLoop = acquiredLeadership.thenCompose(x -> Futures.loop(this::isRunning, () -> Futures.delayedFuture(this::process,
                periodInMillis, gcExecutor), gcExecutor));

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
        return System.currentTimeMillis() / Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS).toMillis();
    }

    private CompletableFuture<Void> process() {
        log.info("Starting GC {}", gcName);

        // This method is called from periodic GC thread and can only be called if this is the leader.
        // Compute new batch id.
        long quantized = computeNewBatch();
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (quantized > currentBatch.get()) {
            // update current group in the store
            future = future.thenCompose(x -> zkStoreHelper.setData(BATCH_PATH, new Data<>(getBytes(quantized), null)))
                    .thenAccept(v -> currentBatch.set(quantized));
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

    @SneakyThrows
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

    @SneakyThrows
    private PathChildrenCache watch(String watchPath) {
        PathChildrenCacheListener watchListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_UPDATED:
                    long newValue = getLong(event.getData().getData());
                    log.debug("GC batch updated with new value {}", newValue);
                    currentBatch.set(newValue);
                    break;
                case CONNECTION_LOST:
                    break;
                default:
                    log.warn("Received event {}", event.getType());
            }
        };

        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkStoreHelper.getClient(), watchPath, true);
        pathChildrenCache.getListenable().addListener(watchListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        return pathChildrenCache;
    }

    @SneakyThrows
    @Override
    public void close() {
        if (watch != null) {
            watch.close();
        }

        if (leaderLatch != null) {
            leaderLatch.close();
        }

        gcExecutor.shutdown();
    }
}
