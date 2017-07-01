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

import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.ScaleEventStreamWriterMock;
import io.pravega.controller.store.stream.StartScaleResponse;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.store.stream.ScaleOperationExceptions;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.ScaleStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

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

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataStore streamStorePartialMock;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private ConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(executor);
        streamStorePartialMock = spy(streamStore); //create a partial mock.
        doReturn(CompletableFuture.completedFuture(false)).when(streamStorePartialMock).isTransactionOngoing(
                anyString(), anyString(), any(), any()); //mock only isTransactionOngoing call.

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStorePartialMock, hostStore,
                taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStorePartialMock, hostStore, segmentHelperMock, executor, "host", connectionFactory);
        streamTransactionMetadataTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(),
                "abortStream", new EventStreamWriterMock<>());

        consumer = new ControllerService(streamStorePartialMock, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        streamStorePartialMock.createScope(SCOPE);

        long start = System.currentTimeMillis();
        streamStorePartialMock.createStream(SCOPE, stream1, configuration1, start, null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments = Collections.singletonList(1);
        StartScaleResponse response = streamStorePartialMock.startScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), start + 20, false, null, executor).get();
        List<Segment> segmentsCreated = response.getSegmentsCreated();
        streamStorePartialMock.scaleNewSegmentsCreated(SCOPE, stream1, sealedSegments, segmentsCreated, response.getActiveEpoch(), start + 20, null, executor).get();
        streamStorePartialMock.scaleSegmentsSealed(SCOPE, stream1, sealedSegments, segmentsCreated, response.getActiveEpoch(), start + 20, null, executor).get();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void sealStreamTest() throws Exception {
        streamMetadataTasks.setRequestEventWriter(new ScaleEventStreamWriterMock(streamMetadataTasks, executor));

        List<Controller.SegmentRange> segmentRanges = consumer.getCurrentSegments(SCOPE, stream1).get();
        assertNotEquals(0, segmentRanges.size());

        //seal a stream.
        ScaleResponse scaleResponse = streamMetadataTasks.manualScale(SCOPE, stream1,
                segmentRanges.stream()
                        .map(range -> range.getSegmentId().getSegmentNumber())
                        .collect(Collectors.toList()),
                Collections.emptyList(), System.currentTimeMillis(), null).get();
        assertEquals(ScaleStreamStatus.SUCCESS, scaleResponse.getStatus());

        //a sealed stream should have zero active/current segments
        assertEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());

        //Scaling a sealed stream should return a pre-condition failure.
        assertEquals(ScaleStreamStatus.PRECONDITION_FAILED, streamMetadataTasks.manualScale(SCOPE, stream1,
                segmentRanges.stream()
                        .map(range -> range.getSegmentId().getSegmentNumber())
                        .collect(Collectors.toList()),
                Collections.emptyList(), System.currentTimeMillis(), null).get().getStatus());

        // Updating config of a sealed stream should fail.
        final StreamConfiguration newConfig = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(4))
                .build();
        assertEquals(UpdateStreamStatus.Status.FAILURE,
                streamMetadataTasks.updateStream(SCOPE, stream1, newConfig, null).join());

        // Creating transactions on a sealed stream should fail.
        AssertExtensions.assertThrows("Create txn on sealed stream",
                streamTransactionMetadataTasks.createTxn(SCOPE, stream1, 10000, 10000, 10000, null),
                ex -> ex instanceof StoreException.OperationNotAllowedException);
    }

    @Test
    public void eventWriterInitializationTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        AssertExtensions.assertThrows("", () -> streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                Arrays.asList(), 30, null).get(), e -> e instanceof ScaleOperationExceptions.ScaleRequestNotEnabledException);

        streamMetadataTasks.setRequestEventWriter(new ScaleEventStreamWriterMock(streamMetadataTasks, executor));
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                newRanges, 30, null).get();

        // scaling operation fails once a stream is sealed.
        assertEquals(ScaleStreamStatus.SUCCESS, scaleOpResult.getStatus());
    }
}
