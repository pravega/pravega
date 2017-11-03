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

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.tables.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * ZK stream metadata store.
 */
@Slf4j
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private final ZKStoreHelper storeHelper;

    ZKStreamMetadataStore(CuratorFramework client, Executor executor) {
        super(new ZKHostIndex(client, "/hostTxnIndex", executor));
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
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }

    @Override
    public void registerBucketListener(int bucket, BucketListener listener) {
        // TODO: add watch on children nodes of the bucket
        listeners.putIfAbsent(bucket, listener);
    }

    @Override
    public void unregisterBucketListener(int bucket) {
        // TODO: remove watch on children nodes of the bucket
        listeners.remove(bucket);
    }

    @Override
    public CompletableFuture<List<String>> getStreamsForBucket(int bucket, Executor executor) {
        return storeHelper.getChildren(String.format(ZKStoreHelper.BUCKET_PATH, bucket))
                .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addUpdateStreamForAutoRetention(final String scope, final String stream, final RetentionPolicy retentionPolicy,
                                                                   final OperationContext context, final Executor executor) {
        int bucket = getBucket(scope, stream);
        String retentionPath = String.format(ZKStoreHelper.RETENTION_PATH, bucket, encodedScopedStreamName(scope, stream));
        byte[] serialize = SerializationUtils.serialize(retentionPolicy);

        return storeHelper.getData(retentionPath)
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                }).thenCompose(data -> {
                    if (data == null) {
                        return storeHelper.createZNodeIfNotExist(retentionPath, serialize);
                    } else {
                        return storeHelper.setData(retentionPath, new Data<>(serialize, data.getVersion()));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteStreamFromAutoRetention(final String scope, final String stream,
                                                                 final OperationContext context, final Executor executor) {
        int bucket = getBucket(scope, stream);
        String retentionPath = String.format(ZKStoreHelper.RETENTION_PATH, bucket, encodedScopedStreamName(scope, stream));

        return storeHelper.deleteNode(retentionPath)
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                });
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return Base64.getEncoder().encodeToString(scopedStreamName.getBytes());
    }

    private String decodedScopedStreamName(String encodedScopedStreamName) {
        return new String(Base64.getDecoder().decode(encodedScopedStreamName));
    }
}
