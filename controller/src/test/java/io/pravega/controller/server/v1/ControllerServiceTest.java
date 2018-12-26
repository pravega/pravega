/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.v1;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.TestingServerStarter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Controller service implementation test.
 */
public class ControllerServiceTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(executor);

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final ConnectionFactoryImpl connectionFactory;
    private final ControllerService consumer;

    private final CuratorFramework zkClient;
    private final TestingServer zkServer;

    private long startTs;
    private long scaleTs;

    private RequestTracker requestTracker = new RequestTracker(true);

    public ControllerServiceTest() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, hostStore, taskMetadataStore, segmentHelper, executor,
                "host", connectionFactory, AuthHelper.getDisabledAuthHelper(), requestTracker);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, segmentHelper, executor, "host", connectionFactory, AuthHelper.getDisabledAuthHelper());

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                new SegmentHelper(), executor, null);
    }

    @Before
    public void setup() throws ExecutionException, InterruptedException {

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(policy2).build();

        // createScope
        streamStore.createScope(SCOPE).get();

        // region createStream
        startTs = System.currentTimeMillis();
        OperationContext context = streamStore.createContext(SCOPE, stream1);
        streamStore.createStream(SCOPE, stream1, configuration1, startTs, context, executor).get();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, context, executor);

        OperationContext context2 = streamStore.createContext(SCOPE, stream2);
        streamStore.createStream(SCOPE, stream2, configuration2, startTs, context2, executor).get();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, context2, executor);

        // endregion

        // region scaleSegments

        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);
        scaleTs = System.currentTimeMillis();
        VersionedMetadata<EpochTransitionRecord> record = streamStore.submitScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), startTs,
                null, null, executor).get();
        VersionedMetadata<State> state = streamStore.getVersionedState(SCOPE, stream1, null, executor).get();
        state = streamStore.updateVersionedState(SCOPE, stream1, State.SCALING, state, null, executor).get();
        record = streamStore.startScale(SCOPE, stream1, false, record, state, null, executor).get();
        streamStore.scaleCreateNewEpochs(SCOPE, stream1, record, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream1, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), record,
                null, executor).get();
        streamStore.completeScale(SCOPE, stream1, record, null, executor).get();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        sealedSegments = Arrays.asList(0L, 1L, 2L);
        record = streamStore.submitScale(SCOPE, stream2, sealedSegments, Arrays.asList(segment3, segment4, segment5),
                scaleTs, null, null, executor).get();
        state = streamStore.getVersionedState(SCOPE, stream2, null, executor).get();
        state = streamStore.updateVersionedState(SCOPE, stream2, State.SCALING, state, null, executor).get();
        record = streamStore.startScale(SCOPE, stream2, false, record, state, null, executor).get();
        streamStore.scaleCreateNewEpochs(SCOPE, stream2, record, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream2, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), record,
                null, executor).get();
        streamStore.completeScale(SCOPE, stream2, record, null, executor).get();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, null, executor).get();

        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamTransactionMetadataTasks.close();
        streamMetadataTasks.close();
        connectionFactory.close();
        zkClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        Map<SegmentId, Long> segments;

        segments = consumer.getSegmentsAtHead(SCOPE, stream1).get();
        assertEquals(2, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 1)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream1).get();
        assertEquals(2, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 1)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream2).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 1)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 2)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream2).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 1)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 2)));
    }
}
