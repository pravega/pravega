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
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.test.common.TestingServerStarter;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZkStoreRetentionTest extends StreamCutServiceTest {
    private TestingServer zkServer;
    private CuratorFramework zkClient;

    @Override
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();
        super.setup();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        zkClient.close();
        zkServer.close();
    }

    @Override
    protected StreamMetadataStore createStore(int bucketCount, Executor executor) {
        return StreamStoreFactory.createZKStore(zkClient, bucketCount, executor);
    }

    @Test(timeout = 10000)
    public void testBucketOwnership() throws Exception {
        // verify that ownership is not taken up by another host
        assertFalse(streamMetadataStore.takeBucketOwnership(0, "", executor).join());

        // Introduce connection failure error
        zkClient.getZookeeperClient().close();

        // restart
        CuratorFramework zkClient2 = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);
        zkClient2.start();
        StreamMetadataStore streamMetadataStore2 = StreamStoreFactory.createZKStore(zkClient2, executor);
        String scope = "scope1";
        String streamName = "stream1";
        streamMetadataStore2.addUpdateStreamForAutoStreamCut(scope, streamName, RetentionPolicy.builder().build(), null, executor).join();
        zkClient2.close();

        zkClient.getZookeeperClient().start();

        Stream stream = new StreamImpl(scope, streamName);

        // verify that at least one of the buckets got the notification
        List<StreamCutBucketService> bucketServices = Lists.newArrayList(service.getBuckets());

        int bucketId = stream.getScopedName().hashCode() % 3;
        StreamCutBucketService bucketService = bucketServices.stream().filter(x -> x.getBucketId() == bucketId).findAny().get();
        AtomicBoolean added = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !added.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> added.set(bucketService.getRetentionFutureMap().size() > 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getRetentionFutureMap().containsKey(stream));
    }

    @Test(timeout = 10000)
    public void testOwnershipOfExistingBucket() throws Exception {
        RequestTracker requestTracker = new RequestTracker(true);
        TestingServer zkServer2 = new TestingServerStarter().start();
        zkServer2.start();
        CuratorFramework zkClient2 = CuratorFrameworkFactory.newClient(zkServer2.getConnectString(), 10000, 1000,
                (r, e, s) -> false);
        zkClient2.start();

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor2 = Executors.newScheduledThreadPool(10);
        String hostId = UUID.randomUUID().toString();

        StreamMetadataStore streamMetadataStore2 = StreamStoreFactory.createZKStore(zkClient2, 1, executor2);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor2);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        StreamMetadataTasks streamMetadataTasks2 = new StreamMetadataTasks(streamMetadataStore2, hostStore, taskMetadataStore,
                segmentHelper, executor2, hostId, connectionFactory, AuthHelper.getDisabledAuthHelper(), requestTracker);

        String scope = "scope1";
        String streamName = "stream1";
        streamMetadataStore2.addUpdateStreamForAutoStreamCut(scope, streamName, RetentionPolicy.builder().build(), null, executor2).join();

        String scope2 = "scope2";
        String streamName2 = "stream2";
        streamMetadataStore2.addUpdateStreamForAutoStreamCut(scope2, streamName2, RetentionPolicy.builder().build(), null, executor2).join();

        StreamCutService service2 = new StreamCutService(1, hostId, streamMetadataStore2, streamMetadataTasks2,
                executor2, requestTracker);
        service2.startAsync();
        service2.awaitRunning();

        assertTrue(service2.getBuckets().stream().allMatch(x -> x.getRetentionFutureMap().size() == 2));

        service2.stopAsync();
        service2.awaitTerminated();
        zkClient2.close();
        zkServer2.close();
        streamMetadataTasks2.close();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executor2);
    }
}
