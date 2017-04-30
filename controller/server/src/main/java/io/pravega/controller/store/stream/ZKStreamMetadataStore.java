/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * ZK stream metadata store.
 */
@Slf4j
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private final ZKStoreHelper storeHelper;

    ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
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
                        throw StoreException.create(StoreException.Type.NODE_NOT_FOUND, "/store/%s");
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }
}
