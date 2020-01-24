/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.InMemoryBucketStore;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class InMemoryBucketService extends BucketService {
    private final InMemoryBucketStore bucketStore;

    InMemoryBucketService(BucketStore.ServiceType serviceType, int bucketId, InMemoryBucketStore bucketStore,
                          ScheduledExecutorService executor, int maxConcurrentExecutions, Duration executionDuration, BucketWork bucketWork) {
        super(serviceType, bucketId, executor, maxConcurrentExecutions, executionDuration, bucketWork);
        this.bucketStore = bucketStore;
    }

    @Override
    public void startBucketChangeListener() {
        bucketStore.registerBucketChangeListener(getServiceType(), getBucketId(), (x, y, z) -> {
            if (z) {
                notify(new StreamNotification(x, y, NotificationType.StreamAdded));
            } else {
                notify(new StreamNotification(x, y, NotificationType.StreamRemoved));
            }
        });
    }
    
    @Override
    public void stopBucketChangeListener() {
        bucketStore.unregisterBucketChangeListener(getServiceType(), getBucketId());
    }
}
