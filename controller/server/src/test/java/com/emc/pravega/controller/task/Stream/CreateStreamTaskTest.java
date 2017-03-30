/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.emc.pravega.controller.util.Config.SERVICE_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class CreateStreamTaskTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService controllerService;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataStore streamStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private TimeoutService timeoutService;

    private SegmentHelper segmentHelperMock;

    @Before
    public void initialize() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        segmentHelperMock = spy(new SegmentHelper());

        doReturn(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(segmentHelperMock).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelperMock, executor, "host", connectionFactory);
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelperMock, executor);

        controllerService.createScope(SCOPE).get();
    }

    @After
    public void tearDown() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
    }

    @Test
    public void createStreamTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();

        // start stream creation in background/asynchronously.
        // the connection to server will fail and should be retried
        controllerService.createStream(configuration1, System.currentTimeMillis());

        // stall for a while before validating that the stream creation is incomplete.
        Thread.sleep(1000);

        // Stream should not have been created and while trying to access any stream metadata, we should get illegalStateException

        streamStore.getConfiguration(SCOPE, stream1, null, executor)
                .handle((res, ex) -> {
                    assertNotEquals(ex, null);
                    assertEquals(ExceptionHelpers.getRealException(ex).getMessage(), "stream state unknown");
                    assertEquals(ExceptionHelpers.getRealException(ex).getClass(), IllegalStateException.class);
                    return null;
                }).get();

        // Mock createSegment to return success.
        doReturn(CompletableFuture.completedFuture(true)).when(segmentHelperMock).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        // Stall for a while to ensure that the mock is invoked in the subsequent retry attempt
        Thread.sleep(1000);

        streamStore.getConfiguration(SCOPE, stream1, null, executor)
                .handle((res, ex) -> {
                    assertEquals(ex, null);
                    assertEquals(res, configuration1);
                    return null;
                }).get();
    }
}
