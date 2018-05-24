/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.TestingServerStarter;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for ControllerService With ZK Stream Store
 */
@Slf4j
public class ControllerServiceWithZKStreamTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService consumer;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;
    private StreamRequestHandler streamRequestHandler;

    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private ConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() {
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            log.error("Error starting ZK server", e);
        }
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                  .controllerURI(URI.create("tcp://localhost"))
                                                                  .build());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory, false, "");
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelperMock, executor, "host", connectionFactory, false, "");
        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                executor);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executor));
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null);
    }

    @After
    public void teardown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 5000)
    public void getSegmentsImmediatelyFollowingTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create stream and scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());
        Controller.CreateStreamStatus streamStatus = consumer.createStream(configuration1, start).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        List<Controller.SegmentRange> currentSegments = consumer.getCurrentSegments(SCOPE, STREAM).get();
        assertEquals(2, currentSegments.size());

        //scale segment 1 which has key range from 0.5 to 1.0 at time: start+20
        Map<Double, Double> keyRanges = new HashMap<>(2);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);

        assertFalse(consumer.checkScale(SCOPE, STREAM, 0).get().getStatus().equals(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS));
        Controller.ScaleResponse scaleStatus = consumer.scale(SCOPE, STREAM, Arrays.asList(1L), keyRanges, start + 20)
                .get();
        assertEquals(Controller.ScaleResponse.ScaleStreamStatus.STARTED, scaleStatus.getStatus());
        AtomicBoolean done = new AtomicBoolean(false);
        Futures.loop(() -> !done.get(), () -> consumer.checkScale(SCOPE, STREAM, scaleStatus.getEpoch())
                                                      .thenAccept(x -> done.set(x.getStatus().equals(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS))), executor).get();
        //After scale the current number of segments is 3;
        List<Controller.SegmentRange> currentSegmentsAfterScale = consumer.getCurrentSegments(SCOPE, STREAM).get();
        assertEquals(3, currentSegmentsAfterScale.size());

        Map<Controller.SegmentRange, List<Long>> successorsOfSeg1 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM, 1)).get();
        assertEquals(2, successorsOfSeg1.size()); //two segments follow segment 1

        Map<Controller.SegmentRange, List<Long>> successorsOfSeg0 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM, 0)).get();
        assertEquals(0, successorsOfSeg0.size()); //no segments follow segment 0
    }
}
