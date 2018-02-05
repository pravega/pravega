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

import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StartScaleResponse;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamCutRecord;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamProperty;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.ScaleStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import lombok.Data;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
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
    private StreamRequestHandler streamRequestHandler;
    private ConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(1, executor);
        streamStorePartialMock = spy(streamStore); //create a partial mock.
        doReturn(CompletableFuture.completedFuture(false)).when(streamStorePartialMock).isTransactionOngoing(
                anyString(), anyString(), any(), any()); //mock only isTransactionOngoing call.

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = spy(new StreamMetadataTasks(streamStorePartialMock, hostStore,
                taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory));

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStorePartialMock, hostStore, segmentHelperMock, executor, "host", connectionFactory);

        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStorePartialMock, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                new SealStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                executor);
        consumer = new ControllerService(streamStorePartialMock, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        streamStorePartialMock.createScope(SCOPE).join();

        long start = System.currentTimeMillis();
        streamStorePartialMock.createStream(SCOPE, stream1, configuration1, start, null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments = Collections.singletonList(1);
        StartScaleResponse response = streamStorePartialMock.startScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), start + 20, false, null, executor).get();
        List<Segment> segmentsCreated = response.getSegmentsCreated();
        streamStorePartialMock.setState(SCOPE, stream1, State.SCALING, null, executor).get();
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

    @Test(timeout = 30000)
    public void updateStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(5)).build();

        StreamProperty<StreamConfiguration> configProp = streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor).join();
        assertFalse(configProp.isUpdating());
        // 1. happy day test
        // update.. should succeed
        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture = streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture.join());

        configProp = streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor).join();
        assertTrue(configProp.getProperty().equals(streamConfiguration));

        streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(6)).build();

        // 2. change state to scaling
        streamStorePartialMock.setState(SCOPE, stream1, State.SCALING, null, executor).get();
        // call update should fail without posting the event
        streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration, null);

        AtomicBoolean loop = new AtomicBoolean(false);
        Futures.loop(() -> !loop.get(),
                () -> streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor)
                        .thenApply(StreamProperty::isUpdating)
                        .thenAccept(loop::set), executor).join();

        // event posted, first step performed. now pick the event for processing
        UpdateStreamTask updateStreamTask = new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, executor);
        UpdateStreamEvent taken = (UpdateStreamEvent) requestEventWriter.eventQueue.take();
        AssertExtensions.assertThrows("", updateStreamTask.execute(taken),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        // now with state = active, process the same event. it should succeed now.
        assertTrue(Futures.await(updateStreamTask.execute(taken)));

        // 3. multiple back to back updates.
        StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.byEventRate(1, 1, 2)).build();

        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture1 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration1, null);

        // ensure that previous updatestream has posted the event and set status to updating,
        // only then call second updateStream
        AtomicBoolean loop2 = new AtomicBoolean(false);
        Futures.loop(() -> !loop2.get(),
                () -> streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor)
                        .thenApply(StreamProperty::isUpdating)
                        .thenAccept(loop2::set), executor).join();

        configProp = streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor).join();
        assertTrue(configProp.getProperty().equals(streamConfiguration1) && configProp.isUpdating());

        StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(7)).build();

        // post the second update request. This should fail here itself as previous one has started.
        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture2 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration2, null);
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateOperationFuture2.join());

        // process event
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        // verify that first request for update also completes with success.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture1.join());

        configProp = streamStorePartialMock.getConfigurationProperty(SCOPE, stream1, true, null, executor).join();
        assertTrue(configProp.getProperty().equals(streamConfiguration1) && !configProp.isUpdating());
    }

    @Test(timeout = 30000)
    public void truncateStreamTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, "test").get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 0.75));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.75, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(1),
                newRanges, 30, null).get();
        assertTrue(scaleOpResult.getStatus().equals(ScaleStreamStatus.STARTED));

        ScaleOperationTask scaleTask = new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor);
        assertTrue(Futures.await(scaleTask.execute((ScaleOpEvent) requestEventWriter.eventQueue.take())));

        // start truncation
        StreamProperty<StreamTruncationRecord> truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test",
                true, null, executor).join();
        assertFalse(truncProp.isUpdating());
        // 1. happy day test
        // update.. should succeed
        Map<Integer, Long> streamCut = new HashMap<>();
        streamCut.put(0, 1L);
        streamCut.put(1, 11L);
        CompletableFuture<UpdateStreamStatus.Status> truncateFuture = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        assertEquals(UpdateStreamStatus.Status.SUCCESS, truncateFuture.join());

        truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).join();
        assertTrue(truncProp.getProperty().getStreamCut().equals(streamCut));
        assertTrue(truncProp.getProperty().getStreamCut().equals(streamCut));

        // 2. change state to scaling
        streamStorePartialMock.setState(SCOPE, "test", State.SCALING, null, executor).get();
        // call update should fail without posting the event
        Map<Integer, Long> streamCut2 = new HashMap<>();
        streamCut2.put(0, 1L);
        streamCut2.put(2, 1L);
        streamCut2.put(3, 1L);

        streamMetadataTasks.truncateStream(SCOPE, "test", streamCut2, null);

        AtomicBoolean loop = new AtomicBoolean(false);
        Futures.loop(() -> !loop.get(),
                () -> streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor)
                        .thenApply(StreamProperty::isUpdating)
                        .thenAccept(loop::set), executor).join();

        // event posted, first step performed. now pick the event for processing
        TruncateStreamTask truncateStreamTask = new TruncateStreamTask(streamMetadataTasks, streamStorePartialMock, executor);
        TruncateStreamEvent taken = (TruncateStreamEvent) requestEventWriter.eventQueue.take();
        AssertExtensions.assertThrows("", truncateStreamTask.execute(taken),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        // now with state = active, process the same event. it should succeed now.
        assertTrue(Futures.await(truncateStreamTask.execute(taken)));

        // 3. multiple back to back updates.

        Map<Integer, Long> streamCut3 = new HashMap<>();
        streamCut3.put(0, 12L);
        streamCut3.put(2, 12L);
        streamCut3.put(3, 12L);
        CompletableFuture<UpdateStreamStatus.Status> truncateOp1 = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut3, null);

        // ensure that previous updatestream has posted the event and set status to updating,
        // only then call second updateStream
        AtomicBoolean loop2 = new AtomicBoolean(false);
        Futures.loop(() -> !loop2.get(),
                () -> streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor)
                        .thenApply(StreamProperty::isUpdating)
                        .thenAccept(loop2::set), executor).join();

        truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).join();
        assertTrue(truncProp.getProperty().getStreamCut().equals(streamCut3) && truncProp.isUpdating());

        // post the second update request. This should fail here itself as previous one has started.
        Map<Integer, Long> streamCut4 = new HashMap<>();
        streamCut4.put(0, 14L);
        streamCut4.put(2, 14L);
        streamCut4.put(3, 14L);
        CompletableFuture<UpdateStreamStatus.Status> truncateOpFuture2 = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut4, null);
        assertEquals(UpdateStreamStatus.Status.FAILURE, truncateOpFuture2.join());

        // process event
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        // verify that first request for update also completes with success.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, truncateOp1.join());

        truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).join();
        assertTrue(truncProp.getProperty().getStreamCut().equals(streamCut3) && !truncProp.isUpdating());
    }

    @Test(timeout = 30000)
    public void retentionStreamTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofMinutes(60).toMillis())
                .build();

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, "test").get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        long recordingTime1 = System.currentTimeMillis();
        Map<Integer, Long> map1 = new HashMap<>();
        map1.put(0, 1L);
        map1.put(1, 1L);
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime1, Long.MIN_VALUE, map1);

        doReturn(CompletableFuture.completedFuture(streamCut1)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any()); //mock only isTransactionOngoing call.

        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime1, null).get();
        // verify that one streamCut is generated and added.

        List<StreamCutRecord> list = streamStorePartialMock.getStreamCutsFromRetentionSet(SCOPE, "test", null, executor).get();
        assertTrue(list.contains(streamCut1));

        Map<Integer, Long> map2 = new HashMap<>();
        map2.put(0, 10L);
        map2.put(1, 10L);
        long recordingTime2 = recordingTime1 + Duration.ofMinutes(5).toMillis();

        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime2, Long.MIN_VALUE, map2);
        doReturn(CompletableFuture.completedFuture(streamCut2)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any()); //mock only isTransactionOngoing call.

        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime2, null).get();
        list = streamStorePartialMock.getStreamCutsFromRetentionSet(SCOPE, "test", null, executor).get();
        StreamProperty<StreamTruncationRecord> truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).get();
        // verify that only one stream cut is in retention set. streamCut2 is not added
        // verify that truncation did not happen
        assertTrue(list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(!truncProp.isUpdating());

        Map<Integer, Long> map3 = new HashMap<>();
        map3.put(0, 20L);
        map3.put(1, 20L);
        long recordingTime3 = recordingTime1 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime3, Long.MIN_VALUE, map3);
        doReturn(CompletableFuture.completedFuture(streamCut3)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any()); //mock only isTransactionOngoing call.

        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime3, null).get();
        // verify two stream cuts are in retention set. Cut 1 and 3.
        // verify that Truncation not not happened.
        list = streamStorePartialMock.getStreamCutsFromRetentionSet(SCOPE, "test", null, executor).get();
        truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).get();

        assertTrue(list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(!truncProp.isUpdating());

        Map<Integer, Long> map4 = new HashMap<>();
        map4.put(0, 20L);
        map4.put(1, 20L);
        long recordingTime4 = recordingTime1 + retentionPolicy.getRetentionParam() + 2;
        StreamCutRecord streamCut4 = new StreamCutRecord(recordingTime4, Long.MIN_VALUE, map4);
        doReturn(CompletableFuture.completedFuture(streamCut4)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any());

        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime4, null).get();
        // verify that only two stream cut are in retention set. streamcut 3 and 4
        // verify that truncation has started. verify that streamCut1 is removed from retention set as that has been used for truncation
        list = streamStorePartialMock.getStreamCutsFromRetentionSet(SCOPE, "test", null, executor).get();
        truncProp = streamStorePartialMock.getTruncationProperty(SCOPE, "test", true, null, executor).get();

        assertTrue(!list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertTrue(truncProp.isUpdating());
        assertTrue(truncProp.getProperty().getStreamCut().get(0) == 1L && truncProp.getProperty().getStreamCut().get(1) == 1L);
    }

    @Test(timeout = 30000)
    public void retentionPolicyUpdateTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);

        String stream = "test";
        final StreamConfiguration noRetentionConfig = StreamConfiguration.builder().scope(SCOPE).streamName(stream).scalingPolicy(policy).build();

        // add stream without retention policy
        streamMetadataTasks.createStreamBody(SCOPE, stream, noRetentionConfig, System.currentTimeMillis()).join();
        String scopedStreamName = String.format("%s/%s", SCOPE, stream);

        // verify that stream is not added to bucket
        assertTrue(!streamStorePartialMock.getStreamsForBucket(0, executor).join().contains(scopedStreamName));

        UpdateStreamTask task = new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, executor);

        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofMinutes(60).toMillis())
                .build();

        final StreamConfiguration withRetentionConfig = StreamConfiguration.builder().scope(SCOPE).streamName(stream).scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        // now update stream with a retention policy
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream, withRetentionConfig, null, executor).join();
        UpdateStreamEvent update = new UpdateStreamEvent(SCOPE, stream);
        task.execute(update).join();

        // verify that bucket has the stream.
        assertTrue(streamStorePartialMock.getStreamsForBucket(0, executor).join().contains(scopedStreamName));

        // update stream such that stream is updated with null retention policy
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream, noRetentionConfig, null, executor).join();
        task.execute(update).join();

        // verify that the stream is no longer present in the bucket
        assertTrue(!streamStorePartialMock.getStreamsForBucket(0, executor).join().contains(scopedStreamName));
    }

    @Test(timeout = 30000)
    public void sealStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        //seal a stream.
        CompletableFuture<UpdateStreamStatus.Status> sealOperationResult = streamMetadataTasks.sealStream(SCOPE, stream1, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealOperationResult.get());

        //a sealed stream should have zero active/current segments
        assertEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());

        //reseal a sealed stream.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, streamMetadataTasks.sealStream(SCOPE, stream1, null).get());

        //scale operation on the sealed stream.
        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.2);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.3, 0.4);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.4, 0.5);

        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, stream1, Collections.singletonList(0),
                Arrays.asList(segment3, segment4, segment5), 30, null).get();

        // scaling operation fails once a stream is sealed.
        assertEquals(ScaleStreamStatus.FAILURE, scaleOpResult.getStatus());
    }

    @Test(timeout = 30000)
    public void deleteStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        // delete before seal
        Controller.DeleteStreamStatus.Status deleteStatus = streamMetadataTasks.deleteStream(SCOPE, stream1, null).get();
        assertEquals(Controller.DeleteStreamStatus.Status.STREAM_NOT_SEALED, deleteStatus);
        assertNull(requestEventWriter.getEventQueue().peek());

        //seal stream.
        CompletableFuture<UpdateStreamStatus.Status> sealOperationResult = streamMetadataTasks.sealStream(SCOPE, stream1, null);

        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());
        Futures.await(sealOperationResult);
        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealOperationResult.get());

        // delete after seal
        CompletableFuture<Controller.DeleteStreamStatus.Status> future = streamMetadataTasks.deleteStream(SCOPE, stream1, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, future.get());

        assertFalse(streamStorePartialMock.checkStreamExists(SCOPE, stream1).join());
    }

    @Test
    public void eventWriterInitializationTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        AssertExtensions.assertThrows("", () -> streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                Arrays.asList(), 30, null).get(), e -> e instanceof TaskExceptions.ProcessingDisabledException);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executor));
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                newRanges, 30, null).get();

        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());

        Controller.ScaleStatusResponse scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, "UNKNOWN", 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());

        scaleStatusResult = streamMetadataTasks.checkScale("UNKNOWN", "test", 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, "test", 5, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());
    }

    @Test(timeout = 30000)
    public void manualScaleTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                newRanges, 30, null).get();

        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());
        OperationContext context = streamStorePartialMock.createContext(SCOPE, "test");
        assertEquals(streamStorePartialMock.getState(SCOPE, "test", false, context, executor).get(), State.ACTIVE);

        // Now when startScale runs even after that we should get the state as active.
        StartScaleResponse response = streamStorePartialMock.startScale(SCOPE, "test", Collections.singletonList(0), newRanges, 30, true, null, executor).get();
        assertEquals(response.getActiveEpoch(), 0);
        assertEquals(streamStorePartialMock.getState(SCOPE, "test", true, context, executor).get(), State.ACTIVE);

        AssertExtensions.assertThrows("", () -> streamStorePartialMock.scaleNewSegmentsCreated(SCOPE, "test",
                Collections.singletonList(0), response.getSegmentsCreated(),
                response.getActiveEpoch(), 30, context, executor).get(),
                ex -> Exceptions.unwrap(ex) instanceof StoreException.IllegalStateException);

        List<Segment> segments = streamMetadataTasks.startScale((ScaleOpEvent) requestEventWriter.getEventQueue().take(), true, context).get();

        assertTrue(segments.stream().anyMatch(x -> x.getNumber() == 1 && x.getKeyStart() == 0.0 && x.getKeyEnd() == 0.5));
        assertTrue(segments.stream().anyMatch(x -> x.getNumber() == 2 && x.getKeyStart() == 0.5 && x.getKeyEnd() == 1.0));
    }

    private CompletableFuture<Void> processEvent(WriterMock requestEventWriter) throws InterruptedException {
        return Retry.withExpBackoff(100, 10, 5, 1000)
                .retryingOn(TaskExceptions.StartException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> {
                    ControllerEvent event;
                    try {
                        event = requestEventWriter.getEventQueue().take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return streamRequestHandler.processEvent(event)
                            .exceptionally(e -> {
                                requestEventWriter.getEventQueue().add(event);
                                throw new CompletionException(e);
                            });
                }, executor);
    }

    @Data
    public class WriterMock implements EventStreamWriter<ControllerEvent> {
        private final StreamMetadataTasks streamMetadataTasks;
        private final ScheduledExecutorService executor;
        @Getter
        private LinkedBlockingQueue<ControllerEvent> eventQueue = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Void> writeEvent(ControllerEvent event) {
            this.eventQueue.add(event);

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> writeEvent(String routingKey, ControllerEvent event) {
            return writeEvent(event);
        }

        @Override
        public Transaction<ControllerEvent> beginTxn() {
            return null;
        }

        @Override
        public Transaction<ControllerEvent> getTxn(UUID transactionId) {
            return null;
        }

        @Override
        public EventWriterConfig getConfig() {
            return null;
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }
    }
}
