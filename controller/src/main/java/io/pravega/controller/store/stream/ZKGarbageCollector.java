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

import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.retention.BucketChangeListener;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.controller.util.Config;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 */
@Slf4j
class ZKGarbageCollector extends AbstractService {

    private static final String GC_ROOT = "/gc";
    private static final String GROUP_PATH = GC_ROOT + "/group";
    // 1. elect leader
    // 2. owner of the root path. everyone else watch the root path.

    // bootstrap:
    // check if root path exists. if not, create one.
    // elect leader
    // if you are the leader, update the root-path
    // else watch the root path.

    // follower:
    // if watch says current root has changed, fetch the new value. update the local variable.
    // getPath method will return the updated value henceforth.

    // leader:
    // schedule GC periodically.

    // GC thread:
    // check if current root needs to be updated.
    // if yes, update it on rootPath.
    // clean up all but one older paths. We know current root. We will fetch all older paths. remove latest root path from this list. and sort and remove all but the largest number.
    // backward compatibility -- as we re

    private final ZKStoreHelper zkStoreHelper;
    private final AtomicLong currentGroup;
    private CompletableFuture<Void> gcLoop;
    private final CompletableFuture<Void> latch = new CompletableFuture<>();
    private final Supplier<CompletableFuture<Void>> gcProcessing;

    // take a supplier which will perform actual GC.

    ZKGarbageCollector(ZKStoreHelper zkStoreHelper, Supplier<CompletableFuture<Void>> gcProcessing) {
        this.currentGroup = new AtomicLong();
        this.zkStoreHelper = zkStoreHelper;
        this.gcProcessing = gcProcessing;
    }

    long getLatestGroup() {
        awaitRunning();
        return currentGroup.get();
    }

    CompletableFuture<Boolean> electLeader() {
        zkStoreHelper.leaderElection("LEADER_PATH");
    }

    @Override
    protected void doStart() {
        long quantized = getQuantized();
        byte[] b = getBytes(quantized);
        zkStoreHelper.createZNodeIfNotExist(GROUP_PATH, b)
                .thenCompose(x -> zkStoreHelper.getData(GROUP_PATH))
                .thenAccept(data -> currentGroup.set(BitConverter.readLong(data.getData(), 0)))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                        registerWatch();
                    }
                    latch.complete(null);
                });
    }

    @SneakyThrows
    private void registerWatch() {
        PathChildrenCacheListener listener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // TODO: shivesh
                    log.warn("we should never reach here");
                    break;
                case CHILD_REMOVED:
                    // TODO: shivesh
                    log.warn("We should never reach here");
                    break;
                case CHILD_UPDATED:
                case CONNECTION_LOST:
                    break;
                default:
                    // what to do?
                    log.warn("Received unknown event {}", event.getType());
            }
        };

        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkStoreHelper.getClient(), GROUP_PATH, true);
        pathChildrenCache.getListenable().addListener(listener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);

        // TODO: shivesh.. executor !!
        electLeader().thenApply(isLeader -> {
            if (isLeader) {
                gcLoop = Futures.loop(this::isRunning, this::myProcess, null);
            } else {
                // register watch
            }
        });
    }

    private byte[] getBytes(long quantized) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, quantized);
        return b;
    }

    private long getQuantized() {
        return (System.currentTimeMillis() / Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS).toMillis());
    }

    private CompletableFuture<Void> myProcess() {
        // this can only be called if we are the leader
        long quantized = getQuantized();
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (quantized > currentGroup.get()) {
            // need to update limit
            future = future.thenRun(() -> zkStoreHelper.setData(GROUP_PATH, new Data<>(getBytes(quantized), null)))
                    .thenAccept(v -> currentGroup.set(quantized));
        }
        return future.thenRun(gcProcessing::get);
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
}
