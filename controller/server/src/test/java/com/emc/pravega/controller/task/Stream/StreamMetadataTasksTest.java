/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class StreamMetadataTasksTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService consumer;

    private TestingServer zkServer;

    private StreamMetadataStore streamStorePartialMock;
    private StreamMetadataTasks streamMetadataTasks;

    @Before
    public void initialize() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType
                .InMemory, executor);
        streamStorePartialMock = spy(streamStore); //create a partial mock.
        doReturn(CompletableFuture.completedFuture(false)).when(streamStorePartialMock).isTransactionOngoing(
                anyString(), anyString(), any(), any()); //mock only isTransactionOngoing call.

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(zkClient), executor);
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStorePartialMock, hostStore,
                taskMetadataStore, segmentHelperMock,
                executor, "host");

        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStorePartialMock, hostStore, taskMetadataStore, segmentHelperMock, executor, "host");
        TimeoutService timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks, 100000, 100000);

        consumer = new ControllerService(streamStorePartialMock, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelperMock, executor);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        streamStorePartialMock.createScope(SCOPE);

        streamStorePartialMock.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis(), null, executor);

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStorePartialMock.scale(SCOPE, stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20, null, executor);
    }

    @After
    public void tearDown() throws IOException {
        zkServer.close();
    }

    @Test
    public void sealStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());

        //seal a stream.
        UpdateStreamStatus sealOperationResult = streamMetadataTasks.sealStreamBody(SCOPE, stream1, null).get();
        assertEquals(UpdateStreamStatus.SUCCESS, sealOperationResult);

        //a sealed stream should have zero active/current segments
        assertEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());

        //reseal a sealed stream.
        assertEquals(UpdateStreamStatus.SUCCESS, streamMetadataTasks.sealStreamBody(SCOPE, stream1, null).get());

        //scale operation on the sealed stream.
        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.2);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.3, 0.4);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.4, 0.5);

        ScaleResponse scaleOpResult = streamMetadataTasks.scaleBody(SCOPE, stream1, Collections
                        .singletonList(0),
                Arrays.asList(segment3, segment4, segment5), 30, null).get();

        //scaling operation fails once a stream is sealed.
        assertEquals(ScaleStreamStatus.PRECONDITION_FAILED, scaleOpResult.getStatus());
    }
}
