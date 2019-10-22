/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;

import java.util.concurrent.ScheduledExecutorService;

public class InMemoryStoreBucketServiceTest extends BucketServiceTest {

    @Override
    public StreamMetadataStore createStreamStore(ScheduledExecutorService executor) {
        return StreamStoreFactory.createInMemoryStore(executor);
    }

    @Override
    public BucketStore createBucketStore(int bucketCount) {
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, bucketCount,
                BucketStore.ServiceType.WatermarkingService, bucketCount);

        return StreamStoreFactory.createInMemoryBucketStore(map);
    }
}
