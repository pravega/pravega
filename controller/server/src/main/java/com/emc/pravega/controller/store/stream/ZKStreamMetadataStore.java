/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

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
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }
}
