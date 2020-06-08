/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.ZKScope;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * ZK kvtable metadata store.
 */
@Slf4j
public class ZookeeperKVTMetadataStore extends AbstractKVTableMetadataStore implements AutoCloseable {
    @VisibleForTesting
    static final String SCOPE_ROOT_PATH = "/store";
    static final String DELETED_KVTABLES_PATH = "/lastActiveKVTableSegment/%s";

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ZKStoreHelper storeHelper;
    private final Executor executor;

    @VisibleForTesting
    public ZookeeperKVTMetadataStore(CuratorFramework client, Executor executor) {
        super(new ZKHostIndex(client, "/hostKVTRequestIndex", executor));
        this.storeHelper = new ZKStoreHelper(client, executor);
        this.executor = executor;
    }

    @Override
    ZookeeperKVTable newKeyValueTable(final String scope, final String name) {
        return new ZookeeperKVTable(scope, name, storeHelper, executor);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        String scopePath = ZKPaths.makePath(SCOPE_ROOT_PATH, scope);
        return storeHelper.checkExists(scopePath);
    }

    @Override
    public CompletableFuture<Void> createEntryForKVTable(String scopeName, String kvtName, byte[] id,  Executor executor) {
        return Futures.completeOn(
                CompletableFuture.completedFuture((ZKScope) getScope(scopeName))
                 .thenCompose(scope -> {
                     scope.getKVTableInScopeZNodePath(kvtName)
                    .thenCompose(path -> scope.addKVTableToScope(path, id));
                     return CompletableFuture.completedFuture(null);
                 }), executor);
    }

    @Override
    public ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(String scope, String name, KeyValueTableConfiguration configuration,
                                                                long createTimestamp, KVTOperationContext context, Executor executor) {
        return super.createKeyValueTable(scope, name, configuration, createTimestamp, context, executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return storeHelper.getData(String.format(DELETED_KVTABLES_PATH, getScopedKVTName(scopeName, streamName)), x -> BitConverter.readInt(x, 0))
                          .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedKVTName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }

    @Override
    public void close() {
    }
    // endregion
}
