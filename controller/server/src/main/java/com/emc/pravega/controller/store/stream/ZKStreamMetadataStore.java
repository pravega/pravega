/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * ZK stream metadata store.
 */
@Slf4j
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private final ZKStoreHelper storeHelper;

    public ZKStreamMetadataStore(ScheduledExecutorService executor) {
        this(ZKUtils.getCuratorClient(), executor);
    }

    @VisibleForTesting
    public ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        initialize();
        storeHelper = new ZKStoreHelper(client, executor);
    }

    private void initialize() {
        METRICS_PROVIDER.start();
    }

    @Override
    ZKStream newStream(final String scope, final String name) {
        return new ZKStream(scope, name, storeHelper);
    }

    @Override
    ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return storeHelper.checkExists(String.format("/store/%s", scopeName))
                .thenApply(scopeExists -> {
                    if (scopeExists) {
                        return scopeName;
                    } else {
                        FutureHelpers.failedFuture(StoreException.create(StoreException.Type.NODE_NOT_FOUND, "/store/%s"));
                        return null;
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }

    @Override
    public CompletableFuture<Void> checkpoint(final String readerGroup, final String readerId, final ByteBuffer checkpointBlob) {
        return storeHelper.checkPoint(readerGroup, readerId, checkpointBlob.array());
    }

    @Override
    public CompletableFuture<ByteBuffer> readCheckpoint(final String readerGroup, final String readerId) {
        return storeHelper.readCheckPoint(readerGroup, readerId);
    }

}
