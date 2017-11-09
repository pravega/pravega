/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.retention;

import com.google.common.collect.Lists;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AutoRetentionTest {
    StreamMetadataStore streamMetadataStore;
    AutoRetentionService service;
    ScheduledExecutorService executor;
    StreamMetadataTasks streamMetadataTasks;
    private ConnectionFactoryImpl connectionFactory;
    private String hostId;

    @Before
    public void setup() throws Exception {
        executor = Executors.newScheduledThreadPool(10);
        hostId = UUID.randomUUID().toString();

        streamMetadataStore = createStore(executor);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);

        streamMetadataTasks = new StreamMetadataTasks(streamMetadataStore, hostStore, taskMetadataStore, segmentHelper, executor, hostId, connectionFactory);
        service = new AutoRetentionService(3, hostId, streamMetadataStore, streamMetadataTasks, executor);
        service.startAsync();
        service.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        service.stopAsync();
        service.awaitTerminated();
        connectionFactory.close();
        executor.shutdown();
    }

    protected abstract StreamMetadataStore createStore(Executor executor);

    @Test(timeout = 10000)
    public void testRetentionService() {
        List<AutoRetentionBucketService> bucketServices = Lists.newArrayList(service.getBuckets());

        assertNotNull(bucketServices);
        assertTrue(bucketServices.size() == 3);
        assertTrue(streamMetadataStore.takeBucketOwnership(0, hostId, executor).join());
        assertTrue(streamMetadataStore.takeBucketOwnership(1, hostId, executor).join());
        assertTrue(streamMetadataStore.takeBucketOwnership(2, hostId, executor).join());
        AssertExtensions.assertThrows("", () -> streamMetadataStore.takeBucketOwnership(3, hostId, executor).join(),
                e -> e instanceof IllegalArgumentException);
        service.notify(new BucketOwnershipListener.BucketNotification(0, BucketOwnershipListener.BucketNotification.NotificationType.BucketAvailable));

        String scope = "scope";
        String streamName = "stream";
        Stream stream = new StreamImpl(scope, streamName);
        streamMetadataStore.addUpdateStreamForAutoRetention(scope, streamName, RetentionPolicy.builder().build(), null, executor).join();

        // verify that at least one of the buckets got the notification
        int bucketId = stream.getScopedName().hashCode() % 3;
        AutoRetentionBucketService bucketService = bucketServices.stream().filter(x -> x.getBucketId() == bucketId).findAny().get();
        AtomicBoolean added = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !added.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> added.set(bucketService.getRetentionFutureMap().size() > 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getRetentionFutureMap().containsKey(stream));

        streamMetadataStore.removeStreamFromAutoRetention(scope, streamName, null, executor).join();
        AtomicBoolean removed = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !removed.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> removed.set(bucketService.getRetentionFutureMap().size() == 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getRetentionFutureMap().size() == 0);
    }
}
