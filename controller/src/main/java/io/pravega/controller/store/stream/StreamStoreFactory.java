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

import io.pravega.controller.store.client.StoreClient;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.util.Config;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executor;

public class StreamStoreFactory {
    public static StreamMetadataStore createStore(final StoreClient storeClient, final Executor executor) {
        return createStore(storeClient, Config.BUCKET_COUNT, executor);
    }

    public static StreamMetadataStore createStore(final StoreClient storeClient, final int bucketCount, final Executor executor) {
        switch (storeClient.getType()) {
            case InMemory:
                return new InMemoryStreamMetadataStore(bucketCount, executor);
            case Zookeeper:
                return new ZKStreamMetadataStore((CuratorFramework) storeClient.getClient(), bucketCount, executor);
            default:
                throw new NotImplementedException(storeClient.getType().toString());
        }
    }

    @VisibleForTesting
    public static StreamMetadataStore createZKStore(final CuratorFramework client,
                                                    final Executor executor) {
        return createZKStore(client, Config.BUCKET_COUNT, executor);
    }

    @VisibleForTesting
    public static StreamMetadataStore createZKStore(final CuratorFramework client,
                                                    final int bucketCount,
                                                    final Executor executor) {
        return new ZKStreamMetadataStore(client, bucketCount, executor);
    }

    @VisibleForTesting
    public static StreamMetadataStore createInMemoryStore(final Executor executor) {
        return createInMemoryStore(Config.BUCKET_COUNT, executor);
    }

    @VisibleForTesting
    public static StreamMetadataStore createInMemoryStore(final int bucketCount, final Executor executor) {
        return new InMemoryStreamMetadataStore(bucketCount, executor);
    }
}
