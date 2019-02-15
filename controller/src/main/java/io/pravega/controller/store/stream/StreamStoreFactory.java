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

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.util.Config;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;

public class StreamStoreFactory {
    public static StreamMetadataStore createStore(final StoreClient storeClient, SegmentHelper segmentHelper, final Executor executor) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryStreamMetadataStore(executor);
            case Zookeeper:
                return new ZKStreamMetadataStore((CuratorFramework) storeClient.getClient(), executor);
            case PravegaTable:
                return new PravegaTablesStreamMetadataStore(segmentHelper, (CuratorFramework) storeClient.getClient(), executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    @VisibleForTesting
    public static StreamMetadataStore createPravegaTablesStore(final SegmentHelper segmentHelper, 
                                                               final CuratorFramework client, final Executor executor) {
        return new PravegaTablesStreamMetadataStore(segmentHelper, client, executor);
    }
    
    @VisibleForTesting
    public static StreamMetadataStore createZKStore(final CuratorFramework client, final Executor executor) {
        return new ZKStreamMetadataStore(client, executor);
    }
    
    @VisibleForTesting
    public static StreamMetadataStore createInMemoryStore(final Executor executor) {
        return new InMemoryStreamMetadataStore(executor);
    }

    public static BucketStore createBucketStore(final StoreClient storeClient, final Executor executor) {
        switch (storeClient.getType()) {
            case InMemory: 
                return createInMemoryBucketStore();
            case Zookeeper: 
            case PravegaTable:
                return createZKBucketStore((CuratorFramework) storeClient.getClient(), executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }
    
    @VisibleForTesting
    public static BucketStore createZKBucketStore(final CuratorFramework client, final Executor executor) {
        return createZKBucketStore(Config.BUCKET_COUNT, client, executor);
    }

    @VisibleForTesting
    public static BucketStore createZKBucketStore(final int bucketCount, final CuratorFramework client, final Executor executor) {
        return new ZookeeperBucketStore(bucketCount, client, executor);
    }
    
    @VisibleForTesting
    public static BucketStore createInMemoryBucketStore() {
        return createInMemoryBucketStore(Config.BUCKET_COUNT);
    }

    @VisibleForTesting
    public static BucketStore createInMemoryBucketStore(int bucketCount) {
        return new InMemoryBucketStore(bucketCount);
    }
}
