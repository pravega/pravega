/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.util.Retry;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.util.Config;
import io.pravega.test.common.TestingServerStarter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class IntermittentCnxnFailureTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService controllerService;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataStore streamStore;
    private BucketStore bucketStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private SegmentHelper segmentHelperMock;
    private RequestTracker requestTracker = new RequestTracker(true);
    private ConnectionFactory connectionFactory;
    
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        segmentHelperMock = spy(new SegmentHelper());

        doReturn(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(Config.SERVICE_PORT).build()).when(segmentHelperMock).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, hostStore, taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory, AuthHelper.getDisabledAuthHelper(), requestTracker);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, segmentHelperMock, executor, "host", connectionFactory, AuthHelper.getDisabledAuthHelper());

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null);

        controllerService.createScope(SCOPE).get();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void createStreamTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // start stream creation in background/asynchronously.
        // the connection to server will fail and should be retried
        controllerService.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis());

        // Stream should not have been created and while trying to access any stream metadata
        // we should get illegalStateException
        try {
            Retry.withExpBackoff(10, 10, 4)
                    .retryingOn(StoreException.DataNotFoundException.class)
                    .throwingOn(IllegalStateException.class)
                    .run(() -> {
                        Futures.getAndHandleExceptions(streamStore.getConfiguration(SCOPE, stream1, null, executor),
                                CompletionException::new);
                        return null;
                    });
        } catch (CompletionException ex) {
            Assert.assertEquals(Exceptions.unwrap(ex).getMessage(), "stream state unknown");
            assertEquals(Exceptions.unwrap(ex).getClass(), IllegalStateException.class);
        }

        // Mock createSegment to return success.
        doReturn(CompletableFuture.completedFuture(true)).when(segmentHelperMock).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any(), any(), anyLong());

        AtomicBoolean result = new AtomicBoolean(false);
        Retry.withExpBackoff(10, 10, 4)
                .retryingOn(IllegalStateException.class)
                .throwingOn(RuntimeException.class)
                .run(() -> {
                    Futures.getAndHandleExceptions(
                            streamStore.getConfiguration(SCOPE, stream1, null, executor)
                                    .thenAccept(configuration -> result.set(configuration.equals(configuration1))),
                            CompletionException::new);
                    return null;
                });

        assertTrue(result.get());
    }
}
