/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
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
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamMetadataStoreTestHelper;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.ScaleStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.AddSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doCallRealMethod;

public abstract class StreamMetadataTasksTest {

    private static final String SCOPE = "scope";
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected boolean authEnabled = false;
    protected CuratorFramework zkClient;
    private final String stream1 = "stream1";

    private ControllerService consumer;

    private TestingServer zkServer;

    private StreamMetadataStore streamStorePartialMock;
    private BucketStore bucketStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private StreamRequestHandler streamRequestHandler;
    private ConnectionFactory connectionFactory;

    private RequestTracker requestTracker = new RequestTracker(true);
    private EventStreamWriterMock<CommitEvent> commitWriter;
    private EventStreamWriterMock<AbortEvent> abortWriter;
    @Mock
    private KVTableMetadataStore kvtStore;
    @Mock
    private TableMetadataTasks kvtMetadataTasks;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        StreamMetadataStore streamStore = getStore();
        streamStorePartialMock = spy(streamStore); //create a partial mock.
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 1,
                BucketStore.ServiceType.WatermarkingService, 1);

        bucketStore = StreamStoreFactory.createInMemoryBucketStore(map);
        
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        EventHelper helper = EventHelperMock.getEventHelperMock(executor, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = spy(new StreamMetadataTasks(streamStorePartialMock, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", new GrpcAuthHelper(authEnabled, "key", 300), requestTracker, helper));

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStorePartialMock, segmentHelperMock, executor, "host", 
                new GrpcAuthHelper(authEnabled, "key", 300));

        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStorePartialMock, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStorePartialMock, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStorePartialMock, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                streamStorePartialMock,
                executor);
        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStorePartialMock, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null);
        commitWriter = new EventStreamWriterMock<>();
        abortWriter = new EventStreamWriterMock<>();
        streamTransactionMetadataTasks.initializeStreamWriters(commitWriter, abortWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        streamStorePartialMock.createScope(SCOPE).join();

        long start = System.currentTimeMillis();
        streamStorePartialMock.createStream(SCOPE, stream1, configuration1, start, null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);
        VersionedMetadata<EpochTransitionRecord> response = streamStorePartialMock.submitScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), start + 20, null, null, executor).get();
        VersionedMetadata<State> state = streamStorePartialMock.getVersionedState(SCOPE, stream1, null, executor).join();
        state = streamStorePartialMock.updateVersionedState(SCOPE, stream1, State.SCALING, state, null, executor).join();
        streamStorePartialMock.startScale(SCOPE, stream1, false, response, state, null, executor).join();
        streamStorePartialMock.scaleCreateNewEpochs(SCOPE, stream1, response, null, executor).get();
        streamStorePartialMock.scaleSegmentsSealed(SCOPE, stream1, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), response,
                null, executor).get();
        streamStorePartialMock.completeScale(SCOPE, stream1, response, null, executor).join();
        streamStorePartialMock.updateVersionedState(SCOPE, stream1, State.ACTIVE, state, null, executor).get();
    }

    abstract StreamMetadataStore getStore();

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStorePartialMock.close();
        streamStorePartialMock.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void updateStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(5)).build();

        StreamConfigurationRecord configProp = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join().getObject();
        assertFalse(configProp.isUpdating());
        // 1. happy day test
        // update.. should succeed
        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture = streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture.join());

        configProp = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join().getObject();
        assertTrue(configProp.getStreamConfiguration().equals(streamConfiguration));

        streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(6)).build();

        // 2. change state to scaling
        streamStorePartialMock.setState(SCOPE, stream1, State.SCALING, null, executor).get();
        // call update should fail without posting the event
        streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration, null);

        AtomicBoolean loop = new AtomicBoolean(false);
        Futures.loop(() -> !loop.get(),
                () -> streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor)
                        .thenApply(x -> x.getObject().isUpdating())
                        .thenAccept(loop::set), executor).join();

        // event posted, first step performed. now pick the event for processing
        UpdateStreamTask updateStreamTask = new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, bucketStore, executor);
        UpdateStreamEvent taken = (UpdateStreamEvent) requestEventWriter.eventQueue.take();
        AssertExtensions.assertFutureThrows("", updateStreamTask.execute(taken),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        // now with state = active, process the same event. it should succeed now.
        assertTrue(Futures.await(updateStreamTask.execute(taken)));

        // 3. multiple back to back updates.
        StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(1, 1, 2)).build();

        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture1 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration1, null);

        // ensure that previous updatestream has posted the event and set status to updating,
        // only then call second updateStream
        AtomicBoolean loop2 = new AtomicBoolean(false);
        Futures.loop(() -> !loop2.get(),
                () -> streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor)
                        .thenApply(x -> x.getObject().isUpdating())
                        .thenAccept(loop2::set), executor).join();

        configProp = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join().getObject();
        assertTrue(configProp.getStreamConfiguration().equals(streamConfiguration1) && configProp.isUpdating());

        StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(7)).build();

        // post the second update request. This should fail here itself as previous one has started.
        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture2 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration2, null);
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateOperationFuture2.join());

        // process event
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        // verify that first request for update also completes with success.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture1.join());

        configProp = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join().getObject();
        assertTrue(configProp.getStreamConfiguration().equals(streamConfiguration1) && !configProp.isUpdating());

        streamStorePartialMock.setState(SCOPE, stream1, State.UPDATING, null, executor).join();
        UpdateStreamEvent event = new UpdateStreamEvent(SCOPE, stream1, System.nanoTime());
        assertTrue(Futures.await(updateStreamTask.execute(event)));
        // execute the event again. It should complete without doing anything. 
        updateStreamTask.execute(event).join();
        assertEquals(State.ACTIVE, streamStorePartialMock.getState(SCOPE, stream1, true, null, executor).join());
    }

    @Test(timeout = 30000)
    public void addSubscriberTest() throws InterruptedException, ExecutionException {
        // add a new subscriber - positive case
        String subscriber1 = "subscriber1";
        Controller.AddSubscriberStatus.Status addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        List<String> allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(1, allSubscribers.size());

        String subscriber2 = "subscriber2";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber3 = "subscriber3";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber3, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(3, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));
        assertTrue(allSubscribers.contains(subscriber3));

        // Add subscriber with same name, next generation idempotent operation
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 1L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        // Add subscriber with same name, old generation
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        // Add subscriber when stream/scope does not exist
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, "nostream", "subscriber4", 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.STREAM_NOT_FOUND, addStatus);
    }

    @Test(timeout = 30000)
    public void removeSubscriberTest() throws InterruptedException, ExecutionException {
        // add a new subscriber - positive case
        String subscriber1 = "subscriber1";
        AddSubscriberStatus.Status addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 1L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber2 = "subscriber2";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber3 = "subscriber3";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber3, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        List<String> allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(3, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));
        assertTrue(allSubscribers.contains(subscriber3));

        // Remove subscriber
        DeleteSubscriberStatus.Status removeStatus = streamMetadataTasks.deleteSubscriber(SCOPE, stream1, subscriber2, 1L, null).get();
        assertEquals(DeleteSubscriberStatus.Status.SUCCESS, removeStatus);

        // Remove subscriber, old generation
        removeStatus = streamMetadataTasks.deleteSubscriber(SCOPE, stream1, subscriber1, 0L, null).get();
        assertEquals(DeleteSubscriberStatus.Status.SUCCESS, removeStatus);

        allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(2, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber3));

        // Remove subscriber from non-existing stream
        removeStatus = streamMetadataTasks.deleteSubscriber(SCOPE, "nostream", subscriber3, 2L, null).get();
        assertEquals(DeleteSubscriberStatus.Status.STREAM_NOT_FOUND, removeStatus);

        // Remove non-existing subscriber from stream
        removeStatus = streamMetadataTasks.deleteSubscriber(SCOPE, stream1, "subscriber4", 2L, null).get();
        assertEquals(DeleteSubscriberStatus.Status.SUCCESS, removeStatus);
    }

    @Test(timeout = 30000)
    public void getSubscribersForStreamTest() throws InterruptedException, ExecutionException {
        // subscribers for non-existing stream
        List<String> allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, "stream2", null).get().getSubscribersList();
        assertEquals(0, allSubscribers.size());

        // no subscribers found for existing Stream
        allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(0, allSubscribers.size());

        // add a new subscribers - positive case
        String subscriber1 = "subscriber1";
        AddSubscriberStatus.Status addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber2 = "subscriber2";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber3 = "subscriber3";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber3, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(3, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));
        assertTrue(allSubscribers.contains(subscriber3));
    }

    @Test(timeout = 30000)
    public void updateSubscriberStreamCutTest() throws InterruptedException, ExecutionException {
        String subscriber1 = "subscriber1";
        AddSubscriberStatus.Status addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        String subscriber2 = "subscriber2";
        addStatus = streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).get();
        assertEquals(Controller.AddSubscriberStatus.Status.SUCCESS, addStatus);

        List<String> allSubscribers = streamMetadataTasks.listSubscribers(SCOPE, stream1, null).get().getSubscribersList();
        assertEquals(2, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));

        ImmutableMap<Long, Long> streamCut1 = ImmutableMap.of(0L, 10L, 1L, 10L);
        UpdateSubscriberStatus.Status updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1,
                                                                                            streamCut1, null).get();
        assertEquals(UpdateSubscriberStatus.Status.SUCCESS, updateStatus);

        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, streamCut1, null).get();
        assertEquals(UpdateSubscriberStatus.Status.SUCCESS, updateStatus);

        ImmutableMap<Long, Long> streamCut2 = ImmutableMap.of(0L, 20L, 1L, 30L);
        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, streamCut2, null).get();
        assertEquals(UpdateSubscriberStatus.Status.SUCCESS, updateStatus);

        ImmutableMap<Long, Long> streamCut3 = ImmutableMap.of(0L, 20L, 1L, 1L);
        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, streamCut3, null).get();
        assertEquals(UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID, updateStatus);

        ImmutableMap<Long, Long> streamCut4 = ImmutableMap.of(0L, 25L);
        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, streamCut4, null).get();
        assertEquals(UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID, updateStatus);

        // update non-existing stream
        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, "nostream", subscriber2, streamCut1, null).get();
        assertEquals(UpdateSubscriberStatus.Status.STREAM_NOT_FOUND, updateStatus);

        // update non-existing subscriber
        updateStatus = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, "nosubscriber", streamCut1, null).get();
        assertEquals(UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND, updateStatus);
    }

    @Test(timeout = 30000)
    public void truncateStreamTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, "test").get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 0.75));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.75, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(1L),
                newRanges, 30, null).get();
        assertTrue(scaleOpResult.getStatus().equals(ScaleStreamStatus.STARTED));

        ScaleOperationTask scaleTask = new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor);
        assertTrue(Futures.await(scaleTask.execute((ScaleOpEvent) requestEventWriter.eventQueue.take())));

        // start truncation
        StreamTruncationRecord truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test",
                null, executor).join().getObject();
        assertFalse(truncProp.isUpdating());
        // 1. happy day test
        // update.. should succeed
        Map<Long, Long> streamCut = new HashMap<>();
        streamCut.put(0L, 1L);
        streamCut.put(1L, 11L);
        CompletableFuture<UpdateStreamStatus.Status> truncateFuture = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        assertEquals(UpdateStreamStatus.Status.SUCCESS, truncateFuture.join());

        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).join().getObject();
        assertTrue(truncProp.getStreamCut().equals(streamCut));
        assertTrue(truncProp.getStreamCut().equals(streamCut));

        // 2. change state to scaling
        streamStorePartialMock.setState(SCOPE, "test", State.SCALING, null, executor).get();
        // call update should fail without posting the event
        long two = NameUtils.computeSegmentId(2, 1);
        long three = NameUtils.computeSegmentId(3, 1);
        Map<Long, Long> streamCut2 = new HashMap<>();
        streamCut2.put(0L, 1L);
        streamCut2.put(two, 1L);
        streamCut2.put(three, 1L);

        streamMetadataTasks.truncateStream(SCOPE, "test", streamCut2, null);

        AtomicBoolean loop = new AtomicBoolean(false);
        Futures.loop(() -> !loop.get(),
                () -> Futures.delayedFuture(() -> streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor), 1000, executor)
                        .thenApply(x -> x.getObject().isUpdating())
                        .thenAccept(loop::set), executor).join();

        // event posted, first step performed. now pick the event for processing
        TruncateStreamTask truncateStreamTask = new TruncateStreamTask(streamMetadataTasks, streamStorePartialMock, executor);
        TruncateStreamEvent taken = (TruncateStreamEvent) requestEventWriter.eventQueue.take();
        AssertExtensions.assertFutureThrows("", truncateStreamTask.execute(taken),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        // now with state = active, process the same event. it should succeed now.
        assertTrue(Futures.await(truncateStreamTask.execute(taken)));

        // 3. multiple back to back updates.

        Map<Long, Long> streamCut3 = new HashMap<>();
        streamCut3.put(0L, 12L);
        streamCut3.put(two, 12L);
        streamCut3.put(three, 12L);
        CompletableFuture<UpdateStreamStatus.Status> truncateOp1 = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut3, null);

        // ensure that previous updatestream has posted the event and set status to updating,
        // only then call second updateStream
        AtomicBoolean loop2 = new AtomicBoolean(false);
        Futures.loop(() -> !loop2.get(),
                () -> streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor)
                        .thenApply(x -> x.getObject().isUpdating())
                        .thenAccept(loop2::set), executor).join();

        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).join().getObject();
        assertTrue(truncProp.getStreamCut().equals(streamCut3) && truncProp.isUpdating());

        // post the second update request. This should fail here itself as previous one has started.
        Map<Long, Long> streamCut4 = new HashMap<>();
        streamCut4.put(0L, 14L);
        streamCut4.put(two, 14L);
        streamCut4.put(three, 14L);
        CompletableFuture<UpdateStreamStatus.Status> truncateOpFuture2 = streamMetadataTasks.truncateStream(SCOPE, "test",
                streamCut4, null);
        assertEquals(UpdateStreamStatus.Status.FAILURE, truncateOpFuture2.join());

        // process event
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        // verify that first request for update also completes with success.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, truncateOp1.join());

        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).join().getObject();
        assertTrue(truncProp.getStreamCut().equals(streamCut3) && !truncProp.isUpdating());

        streamStorePartialMock.setState(SCOPE, "test", State.TRUNCATING, null, executor).join();

        TruncateStreamEvent event = new TruncateStreamEvent(SCOPE, "test", System.nanoTime());
        assertTrue(Futures.await(truncateStreamTask.execute(event)));
        // execute the event again. It should complete without doing anything.
        truncateStreamTask.execute(event).join();

        assertEquals(State.ACTIVE, streamStorePartialMock.getState(SCOPE, "test", true, null, executor).join());
    }

    @Test(timeout = 30000)
    public void timeBasedRetentionStreamTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofMinutes(60).toMillis())
                .build();

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();
        doAnswer(x -> CompletableFuture.completedFuture(Collections.emptyList())).when(streamStorePartialMock).listSubscribers(any(), any(), any(), any());

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, "test").get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        AtomicLong time = new AtomicLong(System.currentTimeMillis());
        streamMetadataTasks.setRetentionClock(time::get);

        long recordingTime1 = time.get();
        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 1L);
        map1.put(1L, 1L);
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime1, Long.MIN_VALUE, ImmutableMap.copyOf(map1));

        doReturn(CompletableFuture.completedFuture(streamCut1)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), any());

        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime1, null, "").get();
        // verify that one streamCut is generated and added.

        List<StreamCutRecord> list =
                streamStorePartialMock.getRetentionSet(SCOPE, "test", null, executor)
                                      .thenCompose(retentionSet -> {
                                          return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                      .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                              x, null, executor))
                                          .collect(Collectors.toList()));
                                      }).join();
        assertTrue(list.contains(streamCut1));

        Map<Long, Long> map2 = new HashMap<>();
        map2.put(0L, 10L);
        map2.put(1L, 10L);
        long recordingTime2 = recordingTime1 + Duration.ofMinutes(5).toMillis();

        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime2, Long.MIN_VALUE, ImmutableMap.copyOf(map2));
        doReturn(CompletableFuture.completedFuture(streamCut2)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), any()); //mock only isTransactionOngoing call.
        time.set(recordingTime2);
        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime2, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, "test", null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();

        StreamTruncationRecord truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).get().getObject();
        // verify that only one stream cut is in retention set. streamCut2 is not added
        // verify that truncation did not happen
        assertTrue(list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(!truncProp.isUpdating());

        Map<Long, Long> map3 = new HashMap<>();
        map3.put(0L, 20L);
        map3.put(1L, 20L);
        long recordingTime3 = recordingTime1 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime3, Long.MIN_VALUE, ImmutableMap.copyOf(map3));
        doReturn(CompletableFuture.completedFuture(streamCut3)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), any()); //mock only isTransactionOngoing call.
        time.set(recordingTime3);
        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime3, null, "").get();
        // verify two stream cuts are in retention set. Cut 1 and 3.
        // verify that Truncation not not happened.
        list = streamStorePartialMock.getRetentionSet(SCOPE, "test", null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).get().getObject();

        assertTrue(list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(!truncProp.isUpdating());

        Map<Long, Long> map4 = new HashMap<>();
        map4.put(0L, 20L);
        map4.put(1L, 20L);
        long recordingTime4 = recordingTime1 + retentionPolicy.getRetentionParam() + 2;
        StreamCutRecord streamCut4 = new StreamCutRecord(recordingTime4, Long.MIN_VALUE, ImmutableMap.copyOf(map4));
        doReturn(CompletableFuture.completedFuture(streamCut4)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), any());
        time.set(recordingTime4);
        streamMetadataTasks.retention(SCOPE, "test", retentionPolicy, recordingTime4, null, "").get();
        // verify that only two stream cut are in retention set. streamcut 3 and 4
        // verify that truncation has started. verify that streamCut1 is removed from retention set as that has been used for truncation
        list = streamStorePartialMock.getRetentionSet(SCOPE, "test", null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, "test", null, executor).get().getObject();

        assertTrue(!list.contains(streamCut1));
        assertTrue(!list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertTrue(truncProp.isUpdating());
        assertTrue(truncProp.getStreamCut().get(0L) == 1L && truncProp.getStreamCut().get(1L) == 1L);
        doCallRealMethod().when(streamStorePartialMock).listSubscribers(any(), any(), any(), any());
    }

    @Test(timeout = 30000)
    public void sizeBasedRetentionStreamTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.SIZE).retentionParam(100L).build();

        String streamName = "test";
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, streamName, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, streamName, State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, streamName).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        // region size based retention on stream cuts on epoch 0
        // region no previous streamcut
        // first retention iteration
        // streamcut1: 19 bytes(0/9,1/10)
        long recordingTime1 = System.currentTimeMillis();
        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 9L);
        map1.put(1L, 10L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map1, Optional.empty(), null, executor).join();
        assertEquals(size, 19);
        StreamCutRecord streamCut1 = new StreamCutRecord(recordingTime1, size, ImmutableMap.copyOf(map1));

        doReturn(CompletableFuture.completedFuture(streamCut1)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), any());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime1, null, "").get();
        // verify that one streamCut is generated and added.
        List<StreamCutRecord> list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                                           .thenCompose(retentionSet -> {
                                                               return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                                           .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                                                   x, null, executor))
                                                                                                           .collect(Collectors.toList()));
                                                           }).join();

        assertTrue(list.contains(streamCut1));
        // endregion

        // region stream cut exists but latest - previous < retention.size
        // second retention iteration
        // streamcut2: 100 bytes(0/50, 1/50)
        Map<Long, Long> map2 = new HashMap<>();
        map2.put(0L, 50L);
        map2.put(1L, 50L);
        long recordingTime2 = recordingTime1 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map2, Optional.empty(), null, executor).join();
        assertEquals(size, 100L);
        StreamCutRecord streamCut2 = new StreamCutRecord(recordingTime2, size, ImmutableMap.copyOf(map2));
        doReturn(CompletableFuture.completedFuture(streamCut2)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime2, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        StreamTruncationRecord truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();
        // verify that two stream cut is in retention set. streamCut2 is added
        // verify that truncation did not happen
        assertTrue(list.contains(streamCut1));
        assertTrue(list.contains(streamCut2));
        assertTrue(!truncProp.isUpdating());
        // endregion

        // region latest - previous > retention.size
        // third retention iteration
        // streamcut3: 120 bytes(0/60, 1/60)
        Map<Long, Long> map3 = new HashMap<>();
        map3.put(0L, 60L);
        map3.put(1L, 60L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map3, Optional.empty(), null, executor).join();
        assertEquals(size, 120L);

        long recordingTime3 = recordingTime2 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut3 = new StreamCutRecord(recordingTime3, size, ImmutableMap.copyOf(map3));
        doReturn(CompletableFuture.completedFuture(streamCut3)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime3, null, "").get();
        // verify two stream cuts are in retention set. Cut 2 and 3.
        // verify that Truncation has happened.
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                      .thenCompose(retentionSet -> {
                                          return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                      .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                              x, null, executor))
                                                                                      .collect(Collectors.toList()));
                                      }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        assertTrue(!list.contains(streamCut1));
        assertTrue(list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(truncProp.isUpdating());
        assertTrue(truncProp.getStreamCut().get(0L) == 9L && truncProp.getStreamCut().get(1L) == 10L);

        assertTrue(Futures.await(processEvent(requestEventWriter)));
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();
        assertFalse(truncProp.isUpdating());
        // endregion
        // endregion

        // region test retention over multiple epochs
        // scale1 --> seal segments 0 and 1 and create 2 and 3. (0/70, 1/70)
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        Map<Long, Long> sealedSegmentsWithSize = new HashMap<>();
        sealedSegmentsWithSize.put(0L, 70L);
        sealedSegmentsWithSize.put(1L, 70L);
        scale(SCOPE, streamName, sealedSegmentsWithSize, new ArrayList<>(newRanges));
        long two = computeSegmentId(2, 1);
        long three = computeSegmentId(3, 1);
        // region latest streamcut on new epoch but latest (newepoch) - previous (oldepoch) < retention.size
        // 4th retention iteration
        // streamcut4: (2/29, 3/30)
        Map<Long, Long> map4 = new HashMap<>();
        map4.put(two, 29L);
        map4.put(three, 30L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map4, Optional.empty(), null, executor).join();
        assertEquals(size, 199L);

        long recordingTime4 = recordingTime3 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut4 = new StreamCutRecord(recordingTime4, size, ImmutableMap.copyOf(map4));
        doReturn(CompletableFuture.completedFuture(streamCut4)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime4, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        assertFalse(list.contains(streamCut1));
        assertTrue(list.contains(streamCut2));
        assertTrue(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertFalse(truncProp.isUpdating());

        // endregion

        // region latest streamcut on new epoch but latest (newepoch) - previous (oldepoch) > retention.size
        // 5th retention iteration
        // streamcut5: 221 bytes(2/41, 3/40)
        Map<Long, Long> map5 = new HashMap<>();
        map5.put(two, 41L);
        map5.put(three, 40L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map5, Optional.empty(), null, executor).join();
        assertEquals(size, 221L);

        long recordingTime5 = recordingTime4 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut5 = new StreamCutRecord(recordingTime5, size, ImmutableMap.copyOf(map5));
        doReturn(CompletableFuture.completedFuture(streamCut5)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime5, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        assertFalse(list.contains(streamCut1));
        assertFalse(list.contains(streamCut2));
        assertFalse(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertTrue(list.contains(streamCut5));
        assertTrue(truncProp.isUpdating());
        assertTrue(truncProp.getStreamCut().get(0L) == 60L && truncProp.getStreamCut().get(1L) == 60L);

        assertTrue(Futures.await(processEvent(requestEventWriter)));
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();
        assertFalse(truncProp.isUpdating());
        // endregion

        // region test retention with external manual truncation
        // scale2 -->  split segment 2 to 4 and 5. Sealed size for segment 2 = 50
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.25, 0.5));
        sealedSegmentsWithSize = new HashMap<>();
        sealedSegmentsWithSize.put(two, 50L);
        scale(SCOPE, streamName, sealedSegmentsWithSize, new ArrayList<>(newRanges));
        long four = computeSegmentId(4, 2);
        long five = computeSegmentId(5, 2);
        // region add streamcut on new epoch such that latest - oldest < retention.size
        // streamcut6: 290 bytes (3/40, 4/30, 5/30)
        // verify no new truncation happens..
        Map<Long, Long> map6 = new HashMap<>();
        map6.put(three, 40L);
        map6.put(four, 30L);
        map6.put(five, 30L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map6, Optional.empty(), null, executor).join();
        assertEquals(size, 290L);

        long recordingTime6 = recordingTime5 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut6 = new StreamCutRecord(recordingTime6, size, ImmutableMap.copyOf(map6));
        doReturn(CompletableFuture.completedFuture(streamCut6)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime6, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        assertFalse(list.contains(streamCut1));
        assertFalse(list.contains(streamCut2));
        assertFalse(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertTrue(list.contains(streamCut5));
        assertTrue(list.contains(streamCut6));
        assertFalse(truncProp.isUpdating());

        // endregion

        // truncate on manual streamcutManual: (1/65, 4/10, 5/10)
        Map<Long, Long> streamCutManual = new HashMap<>();
        streamCutManual.put(1L, 65L);
        streamCutManual.put(four, 10L);
        streamCutManual.put(five, 10L);
        CompletableFuture<UpdateStreamStatus.Status> future = streamMetadataTasks.truncateStream(SCOPE, streamName, streamCutManual, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));
        assertTrue(Futures.await(future));
        assertEquals(future.join(), UpdateStreamStatus.Status.SUCCESS);

        // streamcut7: 340 bytes (3/50, 4/50, 5/50)
        Map<Long, Long> map7 = new HashMap<>();
        map7.put(three, 50L);
        map7.put(four, 50L);
        map7.put(five, 50L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map7, Optional.empty(), null, executor).join();
        assertEquals(size, 340L);

        long recordingTime7 = recordingTime6 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut7 = new StreamCutRecord(recordingTime7, size, ImmutableMap.copyOf(map7));
        doReturn(CompletableFuture.completedFuture(streamCut7)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        // verify no new truncation.. streamcut5 should be chosen but discarded because it is not strictly-ahead-of-truncationRecord
        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime7, null, "").join();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        assertFalse(list.contains(streamCut1));
        assertFalse(list.contains(streamCut2));
        assertFalse(list.contains(streamCut3));
        assertTrue(list.contains(streamCut4));
        assertTrue(list.contains(streamCut5));
        assertTrue(list.contains(streamCut6));
        assertTrue(list.contains(streamCut7));
        assertFalse(truncProp.isUpdating());

        // streamcut8: 400 bytes (3/70, 4/70, 5/70)
        Map<Long, Long> map8 = new HashMap<>();
        map8.put(three, 70L);
        map8.put(four, 70L);
        map8.put(five, 70L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, streamName, map8, Optional.empty(), null, executor).join();
        assertEquals(size, 400L);

        long recordingTime8 = recordingTime7 + Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis() + 1;
        StreamCutRecord streamCut8 = new StreamCutRecord(recordingTime8, size, ImmutableMap.copyOf(map8));
        doReturn(CompletableFuture.completedFuture(streamCut8)).when(streamMetadataTasks).generateStreamCut(
                anyString(), anyString(), any(), any(), anyString());

        streamMetadataTasks.retention(SCOPE, streamName, retentionPolicy, recordingTime8, null, "").get();
        list = streamStorePartialMock.getRetentionSet(SCOPE, streamName, null, executor)
                                     .thenCompose(retentionSet -> {
                                         return Futures.allOfWithResults(retentionSet.getRetentionRecords().stream()
                                                                                     .map(x -> streamStorePartialMock.getStreamCutRecord(SCOPE, "test",
                                                                                             x, null, executor))
                                                                                     .collect(Collectors.toList()));
                                     }).join();
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();

        // verify truncation happens at streamcut6
        assertFalse(list.contains(streamCut1));
        assertFalse(list.contains(streamCut2));
        assertFalse(list.contains(streamCut3));
        assertFalse(list.contains(streamCut4));
        assertFalse(list.contains(streamCut5));
        assertFalse(list.contains(streamCut6));
        assertTrue(list.contains(streamCut7));
        assertTrue(truncProp.isUpdating());
        assertTrue(truncProp.getStreamCut().get(three) == 40L && truncProp.getStreamCut().get(four) == 30L
                && truncProp.getStreamCut().get(five) == 30L);

        assertTrue(Futures.await(processEvent(requestEventWriter)));
        truncProp = streamStorePartialMock.getTruncationRecord(SCOPE, streamName, null, executor).get().getObject();
        assertFalse(truncProp.isUpdating());
        // endregion
        // endregion
        doCallRealMethod().when(streamStorePartialMock).listSubscribers(any(), any(), any(), any());
    }
    
    @Test(timeout = 30000)
    public void consumptionBasedRetentionSizeLimitTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.bySizeBytes(2L, 10L);

        String stream1 = "consumptionSize";
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);
        // region case 1: basic retention
        // add subscriber 1
        // add subscriber 2
        String subscriber1 = "subscriber1";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).join();

        String subscriber2 = "subscriber2";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).join();
        
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 2L, 1L, 1L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 1L, 1L, 2L), null).join();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 2L);
        map1.put(1L, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(1L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 1L, null, "").join();
        // now retention set has one stream cut 0/2, 1/2
        // subscriber lowerbound is 0/1, 1/1.. trucation should happen at lowerbound

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 1L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
        
        // region case 2 min policy check
        // we will update the new streamcut to 0/10, 1/10
        map1.put(0L, 2L);
        map1.put(1L, 2L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(20L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // update both readers to make sure they have read till the latest position. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 2L, 1L, 2L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 2L, 1L, 2L), null).join();

        // no new truncation should happen. 
        // verify that truncation record has not changed. 
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 20L, null, "").join();
        // now retention set has two stream cut 0/2, 1/2...0/2, 1/2
        // subscriber lowerbound is 0/2, 1/2.. does not meet min bound criteria. we also do not have a max that satisfies the limit. no truncation should happen. 
        // no change:
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 1L);
        assertFalse(truncationRecord.getObject().isUpdating());
        // endregion
        
        // region case 3: min criteria not met on lower bound. truncate at max. 
        map1.put(0L, 10L);
        map1.put(1L, 10L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(30L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // update both readers to make sure they have read till the latest position - 1. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 10L, 1L, 9L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 10L, 1L, 9L), null).join();

        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 30L, null, "").join();
        // now retention set has three stream cut 0/2, 1/2...0/2, 1/2... 0/10, 1/10
        // subscriber lowerbound is 0/10, 1/9.. does not meet min bound criteria. but we have max bound on truncation record
        // truncation should happen at 0/2, 1/2
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 2L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 2L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
        
        // region case 4: lowerbound behind max
        // now move the stream further ahead so that max truncation limit is crossed but lowerbound is behind max. 
        map1.put(0L, 20L);
        map1.put(1L, 20L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(40L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());
        
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 40L, null, "").join();
        // now retention set has three stream cut 0/2, 1/2...0/2, 1/2... 0/10, 1/10.. 0/20, 1/20
        // subscriber lowerbound is 0/10, 1/9.. meets min bound criteria. but we have max bound on truncation record
        // truncation should happen at 0/10, 1/10
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 10L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 10L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion

        // region case 5: lowerbound overlaps with max
        map1.put(0L, 30L);
        map1.put(1L, 30L);
        size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(50L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // update both readers to make sure they have read till the latest position - 1. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 21L, 1L, 19L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 21L, 1L, 19L), null).join();

        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 50L, null, "").join();
        // now retention set has three stream cut 0/2, 1/2...0/2, 1/2... 0/10, 1/10.. 0/20, 1/20.. 0/30, 1/30
        // subscriber lowerbound is 0/21, 1/19.. meets min bound criteria. and its also greater than max bound. but it overlaps with max bound. 
        // truncation should happen at 0/21, 1/19
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 21L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 19L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
    }
    
    @Test(timeout = 30000)
    public void consumptionBasedRetentionTimeLimitTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMillis(1L), Duration.ofMillis(10L));

        String stream1 = "consumptionTime";
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);
        AtomicLong time = new AtomicLong(0L);
        streamMetadataTasks.setRetentionClock(time::get);
        // region case 1: basic retention
        // add subscriber 1
        // add subscriber 2
        String subscriber1 = "subscriber1";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).join();

        String subscriber2 = "subscriber2";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).join();
        
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 2L, 1L, 1L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 1L, 1L, 2L), null).join();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(0L, 2L);
        map1.put(1L, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(time.get(), size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, time.get(), null, "").join();
        // now retention set has one stream cut 0/2, 1/2, recording time 1L
        // subscriber lowerbound is 0/1, 1/1.. trucation should not happen as this lowerbound is ahead of min retention streamcut.
        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertFalse(truncationRecord.getObject().isUpdating());
        // endregion
        
        // region case 2 min policy check
        // subscriber streamcut > min time streamcut while
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(2L, 4L, ImmutableMap.of(0L, 2L, 1L, 2L)), null, executor).join();

        time.set(10L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 20L, ImmutableMap.of(0L, 10L, 1L, 10L)), null, executor).join();

        time.set(11L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 20L, ImmutableMap.of(0L, 10L, 1L, 10L)), null, executor).join();

        // retentionset: 0L: 0L/2L, 1L/2L... 2L: 0L/2L, 1L/2L... 10L: 0/10, 1/10....11L: 0/10, 1/10. 
        // update both readers to 0/3, 1/3.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 3L, 1L, 3L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 3L, 1L, 3L), null).join();

        // new truncation should happen at subscriber lowerbound.
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, time.get(), null, "").join();

        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 3L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 3L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
        
        // region case 3: min criteria not met on lower bound. truncate at max.
        time.set(20L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 22L, ImmutableMap.of(0L, 11L, 1L, 11L)), null, executor).join();

        // update both readers to make sure they have read till the latest position - 1. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 11L, 1L, 11L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 11L, 1L, 11L), null).join();

        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, time.get(), null, "").join();
        // retentionset: 0L: 0L/2L, 1L/2L... 2L: 0L/2L, 1L/2L... 10L: 0/10, 1/10....11L: 0/10, 1/10... 20: 0/11, 1/11
        // subscriber lowerbound is 0/11, 1/11 
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        // truncate at limit min
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 10L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 10L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
        
        // region case 4: lowerbound behind max
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(30L, 40L, ImmutableMap.of(0L, 20L, 1L, 20L)), null, executor).join();
        time.set(40L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 42L, ImmutableMap.of(0L, 21L, 1L, 21L)), null, executor).join();

        // update both readers to make sure they have read till the latest position - 1. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 11L, 1L, 11L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 11L, 1L, 11L), null).join();

        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, time.get(), null, "").join();
        // now retention set has five stream cuts 1: 0/2, 1/2...10: 0/10, 1/10... 20: 0/11, 1/11.. 30: 0/20, 1/20.. 40L: 0/21, 1/21
        // subscriber lowerbound is 0/11, 1/11 
        // max = 30. truncate at max
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 20L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 20L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion

        // region case 5: lowerbound overlaps with max
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(50L, 43L, ImmutableMap.of(0L, 21L, 1L, 22L)), null, executor).join();
        time.set(59L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 60L, ImmutableMap.of(0L, 30L, 1L, 30L)), null, executor).join();
        time.set(60L);
        streamStorePartialMock.addStreamCutToRetentionSet(SCOPE, stream1,
                new StreamCutRecord(time.get(), 60L, ImmutableMap.of(0L, 30L, 1L, 30L)), null, executor).join();

        // update both readers to make sure they have read till the latest position - 1. we have set the min limit to 2.  
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 22L, 1L, 21L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 22L, 1L, 21L), null).join();

        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, time.get(), null, "").join();
        // now retention set has five stream cuts 1: 0/2, 1/2...10: 0/10, 1/10... 20: 0/11, 1/11.. 30: 0/20, 1/20.. 40L: 0/21, 1/21
        // 50: 0/21, 1/22 ... 59: 0/30, 1/30.. 60: 0/30, 1/30
        // subscriber lowerbound is 0/22, 1/21 
        // this overlaps with max. so truncate at max (50: 0/21, 1/22)
        truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 21L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 22L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
        // endregion
    }

    @Test(timeout = 30000)
    public void consumptionBasedRetentionWithNoSubscriber() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMillis(0L), Duration.ofMillis(Long.MAX_VALUE));

        String stream1 = "consumptionSize4";
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(retentionPolicy).build();
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream1, configuration, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join();
        streamStorePartialMock.completeUpdateConfiguration(SCOPE, stream1, configRecord, null, executor).join();

        // example::
        // | s0 | s2           | s7 |
        // |    |              |
        // |    |              |
        // |    |    | s4 | s6 | s8 | s10
        // | s1 | s3 | s5 |    | s9 |
        // valid stream cuts: { s0/off, s9/off, s2/-1, s8/-1}, { s1/off, s2/-1 }
        // lower bound = { s0/off, s1/off }

        long two = NameUtils.computeSegmentId(2, 1);
        long three = NameUtils.computeSegmentId(3, 1);
        long four = NameUtils.computeSegmentId(4, 2);
        long five = NameUtils.computeSegmentId(5, 2);
        long six = NameUtils.computeSegmentId(6, 3);
        long seven = NameUtils.computeSegmentId(7, 4);
        long eight = NameUtils.computeSegmentId(8, 4);
        long nine = NameUtils.computeSegmentId(9, 4);
        long ten = NameUtils.computeSegmentId(10, 5);

        // 0, 1 -> 2, 3 with different split
        scale(SCOPE, stream1, ImmutableMap.of(0L, 1L, 1L, 1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.6),
                new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // s3 -> 4, 5
        scale(SCOPE, stream1, ImmutableMap.of(three, 1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 0.8), new AbstractMap.SimpleEntry<>(0.8, 1.0)));
        // 4,5 -> 6
        scale(SCOPE, stream1, ImmutableMap.of(four, 1L, five, 1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 2, 6 -> 7, 8, 9
        scale(SCOPE, stream1, ImmutableMap.of(two, 1L, six, 1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.3), new AbstractMap.SimpleEntry<>(0.3, 0.6),
                        new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 7, 8, 9 -> 10
        scale(SCOPE, stream1, ImmutableMap.of(seven, 1L, eight, 1L, nine, 1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)));

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(ten, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(1L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 1L, null, "").join();
        // now retention set has one stream cut 10/2
        // subscriber lowerbound is 0/1, 1/1.. trucation should happen at 10/2

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(ten).longValue(), 2L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
    }

    @Test(timeout = 30000)
    public void consumptionBasedRetentionWithScale() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(3);
        final RetentionPolicy retentionPolicy = RetentionPolicy.bySizeBytes(0L, 1000L);

        String stream1 = "consumptionSize";
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(retentionPolicy).build();
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream1, configuration, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join();
        streamStorePartialMock.completeUpdateConfiguration(SCOPE, stream1, configRecord, null, executor).join();

        // example::
        // | s0 | s3      |
        // |    | s4 |    | s6
        // | s1      | s5 |
        // | s2      |    |
        // valid stream cuts: { s0/off, s5/-1 }, { s0/off, s2/off, s5/-1 }
        // lower bound = { s0/off, s2/off, s5/-1 }  
        // valid stream cuts: { s0/off, s5/-1 }, { s0/off, s2/off, s5/-1 }, { s0/off, s1/off, s2/off }
        // lower bound = { s0/off, s1/off, s2/off }

        long three = NameUtils.computeSegmentId(3, 1);
        long four = NameUtils.computeSegmentId(4, 1);
        long five = NameUtils.computeSegmentId(5, 2);
        long six = NameUtils.computeSegmentId(6, 3);
        // 0 split to 3 and 4
        scale(SCOPE, stream1, ImmutableMap.of(0L, 1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0 / 6), 
                new AbstractMap.SimpleEntry<>(1.0 / 6, 1.0 / 3)));
        // 4, 1, 2 merged to 5
        scale(SCOPE, stream1, ImmutableMap.of(1L, 1L, 2L, 2L, four, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(1.0 / 6, 1.0)));
        // merge 3, 5 to 6
        scale(SCOPE, stream1, ImmutableMap.of(three, 1L, five, 2L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)));

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);
        
        String subscriber1 = "subscriber1";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).join();

        String subscriber2 = "subscriber2";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).join();
        
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 1L, five, -1L), null).join();
        streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, ImmutableMap.of(0L, 1L, 2L, 1L, five, -1L), null).join();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(six, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(1L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 1L, null, "").join();
        // now retention set has one stream cut 6/2
        // subscriber lowerbound is 0/1, 2/1, 5/-1.. trucation should happen at lowerbound

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(2L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(five).longValue(), -1L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
    }

    @Test(timeout = 30000)
    public void consumptionBasedRetentionWithScale2() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.bySizeBytes(0L, 1000L);

        String stream1 = "consumptionSize2";
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(retentionPolicy).build();
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream1, configuration, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join();
        streamStorePartialMock.completeUpdateConfiguration(SCOPE, stream1, configRecord, null, executor).join();

        // example::
        // | s0 | s2           | s7 |
        // |    |              | 
        // |    |              | 
        // |    |    | s4 | s6 | s8 | s10
        // | s1 | s3 | s5 |    | s9 |
        // valid stream cuts: { s0/off, s9/off, s2/-1, s8/-1}, { s1/off, s2/-1 }
        // lower bound = { s0/off, s1/off }  

        long two = NameUtils.computeSegmentId(2, 1);
        long three = NameUtils.computeSegmentId(3, 1);
        long four = NameUtils.computeSegmentId(4, 2);
        long five = NameUtils.computeSegmentId(5, 2);
        long six = NameUtils.computeSegmentId(6, 3);
        long seven = NameUtils.computeSegmentId(7, 4);
        long eight = NameUtils.computeSegmentId(8, 4);
        long nine = NameUtils.computeSegmentId(9, 4);
        long ten = NameUtils.computeSegmentId(10, 5);
        
        // 0, 1 -> 2, 3 with different split
        scale(SCOPE, stream1, ImmutableMap.of(0L, 1L, 1L, 1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.6), 
                new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // s3 -> 4, 5
        scale(SCOPE, stream1, ImmutableMap.of(three, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 0.8), new AbstractMap.SimpleEntry<>(0.8, 1.0)));
        // 4,5 -> 6
        scale(SCOPE, stream1, ImmutableMap.of(four, 1L, five, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 2, 6 -> 7, 8, 9
        scale(SCOPE, stream1, ImmutableMap.of(two, 1L, six, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.3), new AbstractMap.SimpleEntry<>(0.3, 0.6), 
                        new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 7, 8, 9 -> 10
        scale(SCOPE, stream1, ImmutableMap.of(seven, 1L, eight, 1L, nine, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)));

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);
        
        String subscriber1 = "subscriber1";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).join();

        String subscriber2 = "subscriber2";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).join();

        // invalid streamcut should be rejected
        UpdateSubscriberStatus.Status status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 1L, three, 1L), null).join();
        assertEquals(status, UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID);
        
        status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, 
                ImmutableMap.of(0L, 1L, two, -1L, eight, -1L, nine, 1L), null).join();
        status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, 
                ImmutableMap.of(1L, 1L, two, -1L), null).join();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(ten, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(1L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 1L, null, "").join();
        // now retention set has one stream cut 10/2
        // subscriber lowerbound is 0/1, 1/1.. trucation should happen at lowerbound

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 1L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
    }
    
    @Test(timeout = 30000)
    public void consumptionBasedRetentionWithNoBounds() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMillis(0L), Duration.ofMillis(Long.MAX_VALUE));

        String stream1 = "consumptionSize3";
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        streamStorePartialMock.createStream(SCOPE, stream1, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).retentionPolicy(retentionPolicy).build();
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream1, configuration, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStorePartialMock.getConfigurationRecord(SCOPE, stream1, null, executor).join();
        streamStorePartialMock.completeUpdateConfiguration(SCOPE, stream1, configRecord, null, executor).join();

        // example::
        // | s0 | s2           | s7 |
        // |    |              | 
        // |    |              | 
        // |    |    | s4 | s6 | s8 | s10
        // | s1 | s3 | s5 |    | s9 |
        // valid stream cuts: { s0/off, s9/off, s2/-1, s8/-1}, { s1/off, s2/-1 }
        // lower bound = { s0/off, s1/off }  

        long two = NameUtils.computeSegmentId(2, 1);
        long three = NameUtils.computeSegmentId(3, 1);
        long four = NameUtils.computeSegmentId(4, 2);
        long five = NameUtils.computeSegmentId(5, 2);
        long six = NameUtils.computeSegmentId(6, 3);
        long seven = NameUtils.computeSegmentId(7, 4);
        long eight = NameUtils.computeSegmentId(8, 4);
        long nine = NameUtils.computeSegmentId(9, 4);
        long ten = NameUtils.computeSegmentId(10, 5);
        
        // 0, 1 -> 2, 3 with different split
        scale(SCOPE, stream1, ImmutableMap.of(0L, 1L, 1L, 1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.6), 
                new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // s3 -> 4, 5
        scale(SCOPE, stream1, ImmutableMap.of(three, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 0.8), new AbstractMap.SimpleEntry<>(0.8, 1.0)));
        // 4,5 -> 6
        scale(SCOPE, stream1, ImmutableMap.of(four, 1L, five, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 2, 6 -> 7, 8, 9
        scale(SCOPE, stream1, ImmutableMap.of(two, 1L, six, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.3), new AbstractMap.SimpleEntry<>(0.3, 0.6), 
                        new AbstractMap.SimpleEntry<>(0.6, 1.0)));
        // 7, 8, 9 -> 10
        scale(SCOPE, stream1, ImmutableMap.of(seven, 1L, eight, 1L, nine, 1L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)));

        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        streamMetadataTasks.setRetentionFrequencyMillis(1L);
        
        String subscriber1 = "subscriber1";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber1, 0L, null).join();

        String subscriber2 = "subscriber2";
        streamMetadataTasks.addSubscriber(SCOPE, stream1, subscriber2, 0L, null).join();

        // invalid streamcut should be rejected
        UpdateSubscriberStatus.Status status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, ImmutableMap.of(0L, 1L, three, 1L), null).join();
        assertEquals(status, UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID);
        
        status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber1, 
                ImmutableMap.of(0L, 1L, two, -1L, eight, -1L, nine, 1L), null).join();
        status = streamMetadataTasks.updateSubscriberStreamCut(SCOPE, stream1, subscriber2, 
                ImmutableMap.of(1L, 1L, two, -1L), null).join();

        Map<Long, Long> map1 = new HashMap<>();
        map1.put(ten, 2L);
        long size = streamStorePartialMock.getSizeTillStreamCut(SCOPE, stream1, map1, Optional.empty(), null, executor).join();
        doReturn(CompletableFuture.completedFuture(new StreamCutRecord(1L, size, ImmutableMap.copyOf(map1))))
                .when(streamMetadataTasks).generateStreamCut(anyString(), anyString(), any(), any(), any());

        // call retention and verify that retention policy applies
        streamMetadataTasks.retention(SCOPE, stream1, retentionPolicy, 1L, null, "").join();
        // now retention set has one stream cut 10/2
        // subscriber lowerbound is 0/1, 1/1.. trucation should happen at lowerbound

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, stream1, null, executor).join();
        assertEquals(truncationRecord.getObject().getStreamCut().get(0L).longValue(), 1L);
        assertEquals(truncationRecord.getObject().getStreamCut().get(1L).longValue(), 1L);
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, stream1, truncationRecord, null, executor).join();
    }

    private void scale(String scope, String stream, Map<Long, Long> sealedSegmentsWithSize,
                       List<Map.Entry<Double, Double>> newSegments) {
        long scaleTs = System.currentTimeMillis();

        ArrayList<Long> sealedSegments = Lists.newArrayList(sealedSegmentsWithSize.keySet());
        VersionedMetadata<EpochTransitionRecord> response = streamStorePartialMock.submitScale(scope, stream, sealedSegments,
                newSegments, scaleTs, null, null, executor).join();
        response.getObject().getNewSegmentsWithRange();
        VersionedMetadata<State> state = streamStorePartialMock.getVersionedState(scope, stream, null, executor).join();
        state = streamStorePartialMock.updateVersionedState(scope, stream, State.SCALING, state, null, executor).join();
        streamStorePartialMock.startScale(scope, stream, false, response, state, null, executor).join();
        streamStorePartialMock.scaleCreateNewEpochs(scope, stream, response, null, executor).join();
        streamStorePartialMock.scaleSegmentsSealed(scope, stream, sealedSegmentsWithSize, response, null, executor).join();
        streamStorePartialMock.completeScale(scope, stream, response, null, executor).join();
        streamStorePartialMock.setState(scope, stream, State.ACTIVE, null, executor).join();
    }

    @Test(timeout = 30000)
    public void retentionPolicyUpdateTest() {
        final ScalingPolicy policy = ScalingPolicy.fixed(2);

        String stream = "test";
        final StreamConfiguration noRetentionConfig = StreamConfiguration.builder().scalingPolicy(policy).build();

        // add stream without retention policy
        streamMetadataTasks.createStreamBody(SCOPE, stream, noRetentionConfig, System.currentTimeMillis()).join();
        String scopedStreamName = String.format("%s/%s", SCOPE, stream);

        // verify that stream is not added to bucket
        assertTrue(!bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).join().contains(scopedStreamName));

        UpdateStreamTask task = new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, bucketStore, executor);

        final RetentionPolicy retentionPolicy = RetentionPolicy.builder()
                .retentionType(RetentionPolicy.RetentionType.TIME)
                .retentionParam(Duration.ofMinutes(60).toMillis())
                .build();

        final StreamConfiguration withRetentionConfig = StreamConfiguration.builder().scalingPolicy(policy)
                .retentionPolicy(retentionPolicy).build();

        // now update stream with a retention policy
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream, withRetentionConfig, null, executor).join();
        UpdateStreamEvent update = new UpdateStreamEvent(SCOPE, stream, System.nanoTime());
        task.execute(update).join();

        // verify that bucket has the stream.
        assertTrue(bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).join().contains(scopedStreamName));

        // update stream such that stream is updated with null retention policy
        streamStorePartialMock.startUpdateConfiguration(SCOPE, stream, noRetentionConfig, null, executor).join();
        task.execute(update).join();

        // verify that the stream is no longer present in the bucket
        assertTrue(!bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, 0, executor).join().contains(scopedStreamName));
    }

    @Test(timeout = 30000)
    public void sealStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        SealStreamTask sealStreamTask = new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStorePartialMock, executor);
        AssertExtensions.assertFutureThrows("Stream not sealed", sealStreamTask.execute(new SealStreamEvent(SCOPE, stream1, 0L)),
            e -> Exceptions.unwrap(e) instanceof IllegalStateException);

        //seal a stream.
        CompletableFuture<UpdateStreamStatus.Status> sealOperationResult = streamMetadataTasks.sealStream(SCOPE, stream1, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealOperationResult.get());

        //a sealed stream should have zero active/current segments
        assertEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());

        //reseal a sealed stream.
        assertEquals(UpdateStreamStatus.Status.SUCCESS, streamMetadataTasks.sealStream(SCOPE, stream1, null).get());
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        //scale operation on the sealed stream.
        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.2);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.2, 0.4);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.4, 0.5);

        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, stream1, Collections.singletonList(0L),
                Arrays.asList(segment3, segment4, segment5), 30, null).get();

        // scaling operation fails once a stream is sealed.
        assertEquals(ScaleStreamStatus.FAILURE, scaleOpResult.getStatus());

        AssertExtensions.assertFutureThrows("Scale should not be allowed as stream is already sealed",
                streamStorePartialMock.submitScale(SCOPE, stream1, Collections.singletonList(0L), Arrays.asList(segment3, segment4, segment5), 30, null, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.IllegalStateException);
    }

    @Test(timeout = 30000)
    public void sealStreamFailing() throws Exception {
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        // attempt to seal a stream which is in creating state. This should fail and be retried. 
        // now set the stream state to active. 
        // it should be sealed.
        String creating = "creating";
        streamStorePartialMock.createStream(SCOPE, creating, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), null, executor).join();
        UpdateStreamStatus.Status status = streamMetadataTasks.sealStream(SCOPE, creating, null, 1).join();
        assertEquals(status, UpdateStreamStatus.Status.FAILURE);

        streamStorePartialMock.setState(SCOPE, creating, State.ACTIVE, null, executor).join();
        CompletableFuture<UpdateStreamStatus.Status> statusFuture = streamMetadataTasks.sealStream(SCOPE, creating, null, 1);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(UpdateStreamStatus.Status.SUCCESS, statusFuture.join());
    }
    
    @Test(timeout = 30000)
    public void sealStreamWithTxnTest() throws Exception {
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        String streamWithTxn = "streamWithTxn";

        // region seal a stream with transactions
        long start = System.currentTimeMillis();
        final ScalingPolicy policy = ScalingPolicy.fixed(2);
        final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, streamWithTxn, config, start, null, executor).get();
        streamStorePartialMock.setState(SCOPE, streamWithTxn, State.ACTIVE, null, executor).get();

        // create txn
        VersionedTransactionData openTxn = streamTransactionMetadataTasks.createTxn(SCOPE, streamWithTxn, 10000L, null)
                .get().getKey();

        VersionedTransactionData committingTxn = streamTransactionMetadataTasks.createTxn(SCOPE, streamWithTxn, 10000L, null)
                .get().getKey();

        VersionedTransactionData abortingTxn = streamTransactionMetadataTasks.createTxn(SCOPE, streamWithTxn, 10000L, null)
                .get().getKey();
        
        // set transaction to committing
        streamStorePartialMock.sealTransaction(SCOPE, streamWithTxn, committingTxn.getId(), true, Optional.empty(), 
                "", Long.MIN_VALUE, null, executor).join();

        // set transaction to aborting
        streamStorePartialMock.sealTransaction(SCOPE, streamWithTxn, abortingTxn.getId(), false, Optional.empty(), 
                "", Long.MIN_VALUE, null, executor).join();
        
        // Mock getActiveTransactions call such that we return committing txn as OPEN txn.
        Map<UUID, ActiveTxnRecord> activeTxns = streamStorePartialMock.getActiveTxns(SCOPE, streamWithTxn, null, executor).join();

        Map<UUID, ActiveTxnRecord> retVal = activeTxns.entrySet().stream()
                .map(tx -> {
                    if (!tx.getValue().getTxnStatus().equals(TxnStatus.OPEN) && !tx.getValue().getTxnStatus().equals(TxnStatus.ABORTING)) {
                        ActiveTxnRecord txRecord = tx.getValue();
                        return new AbstractMap.SimpleEntry<>(tx.getKey(),
                                new ActiveTxnRecord(txRecord.getTxCreationTimestamp(), txRecord.getLeaseExpiryTime(),
                                        txRecord.getMaxExecutionExpiryTime(), TxnStatus.OPEN));
                    } else {
                        return tx;
                    }
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        doReturn(CompletableFuture.completedFuture(retVal)).when(streamStorePartialMock).getActiveTxns(
                eq(SCOPE), eq(streamWithTxn), any(), any());

        List<AbortEvent> abortListBefore = abortWriter.getEventList();
        
        streamMetadataTasks.sealStream(SCOPE, streamWithTxn, null);
        AssertExtensions.assertFutureThrows("seal stream did not fail processing with correct exception",
                processEvent(requestEventWriter), e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        requestEventWriter.eventQueue.take();

        reset(streamStorePartialMock);

        // verify that the txn status is set to aborting
        VersionedTransactionData txnData = streamStorePartialMock.getTransactionData(SCOPE, streamWithTxn, openTxn.getId(), null, executor).join();
        assertEquals(txnData.getStatus(), TxnStatus.ABORTING);
        assertEquals(requestEventWriter.getEventQueue().size(), 1);

        // verify that events are posted for the abort txn.
        List<AbortEvent> abortListAfter = abortWriter.getEventList();
        assertEquals(abortListAfter.size(), abortListBefore.size() + 2);
        assertTrue(abortListAfter.stream().anyMatch(x -> x.getTxid().equals(openTxn.getId())));
        assertTrue(abortListAfter.stream().anyMatch(x -> x.getTxid().equals(abortingTxn.getId())));
        
        txnData = streamStorePartialMock.getTransactionData(SCOPE, streamWithTxn, committingTxn.getId(), null, executor).join();
        assertEquals(txnData.getStatus(), TxnStatus.COMMITTING);

        // Mock getActiveTransactions call such that we return some non existent transaction id so that DataNotFound is simulated.
        // returning a random transaction with list of active txns such that when its abort is attempted, Data Not Found Exception gets thrown
        retVal = new HashMap<>();
        retVal.put(UUID.randomUUID(), new ActiveTxnRecord(1L, 1L, 1L, TxnStatus.OPEN));

        doReturn(CompletableFuture.completedFuture(retVal)).when(streamStorePartialMock).getActiveTxns(
                eq(SCOPE), eq(streamWithTxn), any(), any());

        AssertExtensions.assertFutureThrows("seal stream did not fail processing with correct exception",
                processEvent(requestEventWriter), e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        reset(streamStorePartialMock);

        // Now complete all existing transactions and verify that seal completes
        streamStorePartialMock.abortTransaction(SCOPE, streamWithTxn, openTxn.getId(), null, executor).join();
        streamStorePartialMock.abortTransaction(SCOPE, streamWithTxn, abortingTxn.getId(), null, executor).join();
        ((AbstractStreamMetadataStore) streamStorePartialMock).commitTransaction(SCOPE, streamWithTxn, committingTxn.getId(), null, executor).join();
        activeTxns = streamStorePartialMock.getActiveTxns(SCOPE, streamWithTxn, null, executor).join();
        assertTrue(activeTxns.isEmpty());

        assertTrue(Futures.await(processEvent(requestEventWriter)));
        // endregion
    }

    @Test(timeout = 30000)
    public void deleteStreamTest() throws Exception {
        deleteStreamTest(stream1);
    }

    private void deleteStreamTest(String stream) throws InterruptedException, ExecutionException {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        // delete before seal
        Controller.DeleteStreamStatus.Status deleteStatus = streamMetadataTasks.deleteStream(SCOPE, stream, null).get();
        assertEquals(Controller.DeleteStreamStatus.Status.STREAM_NOT_SEALED, deleteStatus);
        assertNull(requestEventWriter.getEventQueue().peek());

        //seal stream.
        CompletableFuture<UpdateStreamStatus.Status> sealOperationResult = streamMetadataTasks.sealStream(SCOPE, stream, null);

        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream, null, executor).get());
        Futures.await(sealOperationResult);
        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealOperationResult.get());

        // delete after seal
        CompletableFuture<Controller.DeleteStreamStatus.Status> future = streamMetadataTasks.deleteStream(SCOPE, stream, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, future.get());

        assertFalse(streamStorePartialMock.checkStreamExists(SCOPE, stream).join());
    }

    @Test
    public void deletePartiallyCreatedStreamTest() throws InterruptedException {
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        StreamMetadataStore store = streamStorePartialMock;
        
        final String scopeName = "RecreationScopePartial";
        final String streamName = "RecreatedStreamPartial";

        store.createScope(scopeName).join();
        Controller.DeleteStreamStatus.Status deleteStatus;
        
        // region case 1: only add stream to scope without any additional metadata
        StreamMetadataStoreTestHelper.addStreamToScope(store, scopeName, streamName);
        assertTrue(store.checkStreamExists(scopeName, streamName).join());
        deleteStatus = streamMetadataTasks.deleteStream(scopeName, streamName, null).join();
        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, deleteStatus);
        // verify that event is not posted 
        assertTrue(requestEventWriter.eventQueue.isEmpty());
        // endregion

        // region case 2: only add creation time for the stream and then delete it. 
        StreamMetadataStoreTestHelper.partiallyCreateStream(store, scopeName, streamName, 
                Optional.of(100L), false);

        assertTrue(store.checkStreamExists(scopeName, streamName).join());

        deleteStatus = streamMetadataTasks.deleteStream(scopeName, streamName, null).join();
        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, deleteStatus);
        // verify that event is not posted 
        assertTrue(requestEventWriter.eventQueue.isEmpty());
        // endregion

        // region case 3: create stream again but this time create the `state` but not history record.
        // this should result in delete workflow being invoked as segments also have to be deleted. 
        StreamMetadataStoreTestHelper.partiallyCreateStream(store, scopeName, streamName, Optional.of(100L), true);
        assertTrue(store.checkStreamExists(scopeName, streamName).join());

        CompletableFuture<Controller.DeleteStreamStatus.Status> future = streamMetadataTasks.deleteStream(scopeName,
                streamName, null);

        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, future.join());
        // endregion

        // region case 4: now create full stream metadata. 
        // now create full stream metadata without setting state to active
        // since there was no active segments, so we should have segments created from segment 0.
        // configuration 2 has 3 segments. So highest segment number should be 2. 
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build();
        store.createStream(scopeName, streamName, configuration, 101L, null, executor).join();
        assertTrue(store.checkStreamExists(scopeName, streamName).join());

        assertEquals(store.getActiveEpoch(scopeName, streamName, null, true, executor).join()
                          .getSegmentIds().stream().max(Long::compareTo).get().longValue(), 2L);

        // delete stream should succeed
        future = streamMetadataTasks.deleteStream(scopeName, streamName, null);
        assertTrue(Futures.await(processEvent(requestEventWriter)));

        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, future.join());

        store.createStream(scopeName, streamName, configuration, 102L, null, executor).join();
        assertEquals(store.getActiveEpoch(scopeName, streamName, null, true, executor).join()
                          .getSegmentIds().stream().max(Long::compareTo).get().longValue(), 5L);
        // endregion
    }
    
    @Test(timeout = 30000)
    public void eventWriterInitializationTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executor));
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0L),
                newRanges, 30, null).get();

        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());

        Controller.ScaleStatusResponse scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, "UNKNOWN", 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());

        scaleStatusResult = streamMetadataTasks.checkScale("UNKNOWN", "test", 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());
        
        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, "test", 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());
        
        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, "test", 5, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.INVALID_INPUT, scaleStatusResult.getStatus());
    }

    @Test(timeout = 30000)
    public void manualScaleTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0L),
                newRanges, 30, null).get();

        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());
        OperationContext context = streamStorePartialMock.createContext(SCOPE, "test");
        assertEquals(streamStorePartialMock.getState(SCOPE, "test", false, context, executor).get(), State.ACTIVE);

        // Now when runScale runs even after that we should get the state as active.
        VersionedMetadata<EpochTransitionRecord> response = streamStorePartialMock.submitScale(SCOPE, "test", Collections.singletonList(0L),
                new LinkedList<>(newRanges), 30, null, null, executor).get();
        assertEquals(response.getObject().getActiveEpoch(), 0);
        VersionedMetadata<State> versionedState = streamStorePartialMock.getVersionedState(SCOPE, "test", context, executor).get();
        assertEquals(versionedState.getObject(), State.ACTIVE);

        // if we call start scale without scale being set to SCALING, this should throw illegal argument exception
        AssertExtensions.assertThrows("", () -> streamStorePartialMock.startScale(SCOPE, "test", true,
                response, versionedState, context, executor).get(), ex -> Exceptions.unwrap(ex) instanceof IllegalArgumentException);

        ScaleOperationTask task = new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor);
        task.runScale((ScaleOpEvent) requestEventWriter.getEventQueue().take(), true, context, "").get();
        Map<Long, Map.Entry<Double, Double>> segments = response.getObject().getNewSegmentsWithRange();
        assertTrue(segments.entrySet().stream()
                .anyMatch(x -> x.getKey() == computeSegmentId(1, 1)
                             && AssertExtensions.nearlyEquals(x.getValue().getKey(), 0.0, 0)
                             && AssertExtensions.nearlyEquals(x.getValue().getValue(), 0.5, 0)));
        assertTrue(segments.entrySet().stream()
                .anyMatch(x -> x.getKey() == computeSegmentId(2, 1)
                             && AssertExtensions.nearlyEquals(x.getValue().getKey(), 0.5, 0)
                             && AssertExtensions.nearlyEquals(x.getValue().getValue(), 1.0, 0)));
    }

    @Test(timeout = 10000)
    public void checkScaleCompleteTest() throws ExecutionException, InterruptedException {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        String test = "testCheckScale";
        streamStorePartialMock.createStream(SCOPE, test, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).get();
        List<Map.Entry<Double, Double>> newRanges = Collections.singletonList(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        
        // region scale
        ScaleResponse scaleOpResult = streamMetadataTasks.manualScale(SCOPE, test, Collections.singletonList(0L),
                newRanges, 30, null).get();
        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());

        streamStorePartialMock.setState(SCOPE, test, State.SCALING, null, executor).join();
        
        Controller.ScaleStatusResponse scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());

        // perform scale steps and check scale after each step
        VersionedMetadata<EpochTransitionRecord> etr = streamStorePartialMock.getEpochTransition(SCOPE, test, null, executor).join();
        streamStorePartialMock.scaleCreateNewEpochs(SCOPE, test, etr, null, executor).join();

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());

        streamStorePartialMock.scaleSegmentsSealed(SCOPE, test, Collections.singletonMap(0L, 0L), etr, null, executor).join();

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());

        streamStorePartialMock.completeScale(SCOPE, test, etr, null, executor).join();

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());

        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).join();

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS, scaleStatusResult.getStatus());

        // start another scale
        scaleOpResult = streamMetadataTasks.manualScale(SCOPE, test, Collections.singletonList(NameUtils.computeSegmentId(1, 1)),
                newRanges, 30, null).get();
        assertEquals(ScaleStreamStatus.STARTED, scaleOpResult.getStatus());
        streamStorePartialMock.setState(SCOPE, test, State.SCALING, null, executor).join();

        // even now we should get success for epoch 0 
        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 0, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.SUCCESS, scaleStatusResult.getStatus());

        scaleStatusResult = streamMetadataTasks.checkScale(SCOPE, test, 1, null).get();
        assertEquals(Controller.ScaleStatusResponse.ScaleStatus.IN_PROGRESS, scaleStatusResult.getStatus());
        // endregion
    }
    
    @Test(timeout = 10000)
    public void checkUpdateCompleteTest() throws ExecutionException, InterruptedException {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        String test = "testUpdate";
        streamStorePartialMock.createStream(SCOPE, test, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).get();
        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        // region update
        
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build();

        streamMetadataTasks.updateStream(SCOPE, test, configuration2, null);
        // wait till configuration is updated
        Supplier<Boolean> configUpdated = () -> !streamStorePartialMock.getConfigurationRecord(SCOPE, test, null, executor).join().getObject().isUpdating();
        Futures.loop(configUpdated, () -> Futures.delayedFuture(Duration.ofMillis(100), executor), executor).join();

        streamStorePartialMock.setState(SCOPE, test, State.UPDATING, null, executor).join();

        assertFalse(streamMetadataTasks.isUpdated(SCOPE, test, configuration2, null).get());

        VersionedMetadata<StreamConfigurationRecord> configurationRecord = streamStorePartialMock.getConfigurationRecord(SCOPE, test, null, executor).join();
        assertTrue(configurationRecord.getObject().isUpdating());
        streamStorePartialMock.completeUpdateConfiguration(SCOPE, test, configurationRecord, null, executor).join();

        assertFalse(streamMetadataTasks.isUpdated(SCOPE, test, configuration2, null).get());

        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).join();
        assertTrue(streamMetadataTasks.isUpdated(SCOPE, test, configuration2, null).get());

        // start next update with different configuration. 
        final StreamConfiguration configuration3 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        streamMetadataTasks.updateStream(SCOPE, test, configuration3, null);
        Futures.loop(configUpdated, () -> Futures.delayedFuture(Duration.ofMillis(100), executor), executor).join();

        streamStorePartialMock.setState(SCOPE, test, State.UPDATING, null, executor).join();
        // we should still get complete for previous configuration we attempted to update
        assertTrue(streamMetadataTasks.isUpdated(SCOPE, test, configuration2, null).get());
        
        assertFalse(streamMetadataTasks.isUpdated(SCOPE, test, configuration3, null).get());
        // end region
    }
    
    @Test(timeout = 10000)
    public void checkTruncateCompleteTest() throws ExecutionException, InterruptedException {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy).build();

        String test = "testTruncate";
        streamStorePartialMock.createStream(SCOPE, test, configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).get();
        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        
        // region truncate
        Map<Long, Long> map = Collections.singletonMap(0L, 1L);
        streamMetadataTasks.truncateStream(SCOPE, test, map, null);
        // wait till configuration is updated
        Supplier<Boolean> truncationStarted = () -> !streamStorePartialMock.getTruncationRecord(SCOPE, test, null, executor).join().getObject().isUpdating();
        Futures.loop(truncationStarted, () -> Futures.delayedFuture(Duration.ofMillis(100), executor), executor).join();

        streamStorePartialMock.setState(SCOPE, test, State.TRUNCATING, null, executor).join();

        assertFalse(streamMetadataTasks.isTruncated(SCOPE, test, map, null).get());

        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock.getTruncationRecord(SCOPE, test, null, executor).join();
        assertTrue(truncationRecord.getObject().isUpdating());
        streamStorePartialMock.completeTruncation(SCOPE, test, truncationRecord, null, executor).join();

        assertFalse(streamMetadataTasks.isTruncated(SCOPE, test, map, null).get());

        streamStorePartialMock.setState(SCOPE, test, State.ACTIVE, null, executor).join();
        assertTrue(streamMetadataTasks.isTruncated(SCOPE, test, map, null).get());

        // start next update with different configuration. 
        Map<Long, Long> map2 = Collections.singletonMap(0L, 10L);

        streamMetadataTasks.truncateStream(SCOPE, test, map2, null);
        Futures.loop(truncationStarted, () -> Futures.delayedFuture(Duration.ofMillis(100), executor), executor).join();

        streamStorePartialMock.setState(SCOPE, test, State.TRUNCATING, null, executor).join();
        
        // we should still get complete for previous configuration we attempted to update
        assertTrue(streamMetadataTasks.isTruncated(SCOPE, test, map, null).get());
        assertFalse(streamMetadataTasks.isTruncated(SCOPE, test, map2, null).get());
        // end region
    }

    @Test(timeout = 10000)
    public void testThrowSynchronousExceptionOnWriteEvent() {
        @SuppressWarnings("unchecked")
        EventStreamWriter<ControllerEvent> requestEventWriter = mock(EventStreamWriter.class);
        doAnswer(x -> {
            throw new RuntimeException();
        }).when(requestEventWriter).writeEvent(anyString(), any());
        
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        AssertExtensions.assertFutureThrows("",
                streamMetadataTasks.writeEvent(new UpdateStreamEvent("scope", "stream", 0L)),
                e -> Exceptions.unwrap(e) instanceof TaskExceptions.PostEventException);
    }

    @Test(timeout = 10000)
    public void testAddIndexAndSubmitTask() {
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        UpdateStreamEvent updateEvent = new UpdateStreamEvent("scope", "stream", 0L);
        AssertExtensions.assertFutureThrows("throw Connection error", streamMetadataTasks.addIndexAndSubmitTask(updateEvent,
                () -> Futures.failedFuture(StoreException.create(StoreException.Type.CONNECTION_ERROR, "Connection"))), 
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);
        // verify that the event is posted
        assertFalse(requestEventWriter.eventQueue.isEmpty());
        assertEquals(requestEventWriter.eventQueue.poll(), updateEvent);

        TruncateStreamEvent truncateEvent = new TruncateStreamEvent("scope", "stream", 0L);

        AssertExtensions.assertFutureThrows("throw write conflict", streamMetadataTasks.addIndexAndSubmitTask(truncateEvent,
                () -> Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT, "write conflict"))),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
        // verify that the event is posted
        assertFalse(requestEventWriter.eventQueue.isEmpty());
        assertEquals(requestEventWriter.eventQueue.poll(), truncateEvent);

        AssertExtensions.assertFutureThrows("any other exception", streamMetadataTasks.addIndexAndSubmitTask(truncateEvent,
                () -> Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "data not found"))),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        // no event should be posted for any other failure
        assertTrue(requestEventWriter.eventQueue.isEmpty());
    }
    
    @Test(timeout = 30000)
    public void concurrentCreateStreamTest() {
        TaskMetadataStore taskMetadataStore = spy(TaskStoreFactory.createZKStore(zkClient, executor));

        StreamMetadataTasks metadataTask = new StreamMetadataTasks(streamStorePartialMock, bucketStore, taskMetadataStore, 
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(authEnabled, "key", 300), requestTracker);

        final ScalingPolicy policy = ScalingPolicy.fixed(2);

        String stream = "concurrent";
        final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(policy).build();
        
        CompletableFuture<Void> createStreamCalled = new CompletableFuture<>();
        CompletableFuture<Void> waitOnCreateStream = new CompletableFuture<>();
        
        doAnswer(x -> {
            createStreamCalled.complete(null);
            waitOnCreateStream.join();
            return x.callRealMethod();
        }).when(streamStorePartialMock).createStream(anyString(), anyString(), any(), anyLong(), any(), any());
        
        CompletableFuture<Controller.CreateStreamStatus.Status> createStreamFuture1 = metadataTask.createStreamRetryOnLockFailure(
                SCOPE, stream, config, System.currentTimeMillis(), 10);

        // wait until create stream is called. let create stream be blocked on `wait` future. 
        createStreamCalled.join();

        // start a new create stream with 1 retries. this should throw lock failed exception
        // second request should fail with LockFailedException as we have not asked for a retry. 
        AssertExtensions.assertFutureThrows("Lock Failed Exception should be thrown", 
                metadataTask.createStreamRetryOnLockFailure(SCOPE, stream, config, System.currentTimeMillis(), 1), 
                e -> Exceptions.unwrap(e) instanceof LockFailedException);

        CompletableFuture<Void> signalLockFailed = new CompletableFuture<>();
        CompletableFuture<Void> waitOnLockFailed = new CompletableFuture<>();

        // first time lock failed exception is thrown, we will complete `signalLockFailed` to indicate lock failed exception is 
        // being thrown.
        // For all subsequent times we will wait on waitOnLockFailed future.  
        doAnswer(x -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<Void> future = (CompletableFuture<Void>) x.callRealMethod();
            return future.exceptionally(e -> {
                if (Exceptions.unwrap(e) instanceof LockFailedException) {
                    if (!signalLockFailed.isDone()) {
                        signalLockFailed.complete(null);
                    } else {
                        waitOnLockFailed.join();
                    }
                }
                throw new CompletionException(e);
            });
        }).when(taskMetadataStore).lock(any(), any(), anyString(), anyString(), any(), any());

        // start a new create stream with retries. 
        CompletableFuture<Controller.CreateStreamStatus.Status> createStreamFuture2 =
                metadataTask.createStreamRetryOnLockFailure(SCOPE, stream, config, System.currentTimeMillis(), 10);

        // wait until lock failed exception is thrown
        signalLockFailed.join();
        
        // now complete first createStream request
        waitOnCreateStream.complete(null);

        assertEquals(createStreamFuture1.join(), Controller.CreateStreamStatus.Status.SUCCESS);
        
        // now let the lock failed exception be thrown for second request for subsequent retries
        waitOnLockFailed.complete(null);

        // second request should also succeed now but with stream exists
        assertEquals(createStreamFuture2.join(), Controller.CreateStreamStatus.Status.STREAM_EXISTS);
    }

    @Test(timeout = 30000)
    public void testWorkflowCompletionTimeout() {
        EventHelper helper = EventHelperMock.getEventHelperMock(executor, "host", ((AbstractStreamMetadataStore) streamStorePartialMock).getHostTaskIndex());

        StreamMetadataTasks streamMetadataTask = new StreamMetadataTasks(streamStorePartialMock, bucketStore,
                TaskStoreFactory.createZKStore(zkClient, executor),
                SegmentHelperMock.getSegmentHelperMock(), executor, "host",
                new GrpcAuthHelper(authEnabled, "key", 300), requestTracker, helper);
        streamMetadataTask.setCompletionTimeoutMillis(500L);
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();

        String completion = "completion";
        streamStorePartialMock.createStream(SCOPE, completion, configuration, System.currentTimeMillis(), null, executor).join();
        streamStorePartialMock.setState(SCOPE, completion, State.ACTIVE, null, executor).join();

        WriterMock requestEventWriter = new WriterMock(streamMetadataTask, executor);
        streamMetadataTask.setRequestEventWriter(requestEventWriter);
        
        StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build();

        AssertExtensions.assertFutureThrows("update timedout", 
                streamMetadataTask.updateStream(SCOPE, completion, configuration2, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        ControllerEvent event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof UpdateStreamEvent);
        VersionedMetadata<StreamConfigurationRecord> configurationRecord = streamStorePartialMock
                .getConfigurationRecord(SCOPE, completion, null, executor).join();
        assertTrue(configurationRecord.getObject().isUpdating());

        Map<Long, Long> streamCut = Collections.singletonMap(0L, 0L);
        AssertExtensions.assertFutureThrows("truncate timedout",
                streamMetadataTask.truncateStream(SCOPE, completion, streamCut, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof TruncateStreamEvent);
        
        VersionedMetadata<StreamTruncationRecord> truncationRecord = streamStorePartialMock
                .getTruncationRecord(SCOPE, completion, null, executor).join();
        assertTrue(truncationRecord.getObject().isUpdating());

        AssertExtensions.assertFutureThrows("seal timedout",
                streamMetadataTask.sealStream(SCOPE, completion, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof SealStreamEvent);
        
        VersionedMetadata<State> state = streamStorePartialMock
                .getVersionedState(SCOPE, completion, null, executor).join();
        assertEquals(state.getObject(), State.SEALING);

        streamStorePartialMock.setState(SCOPE, completion, State.SEALED, null, executor).join();

        AssertExtensions.assertFutureThrows("delete timedout",
                streamMetadataTask.deleteStream(SCOPE, completion, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof DeleteStreamEvent);
    }
    
    private CompletableFuture<Void> processEvent(WriterMock requestEventWriter) throws InterruptedException {
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
        public CompletableFuture<Void> writeEvents(String routingKey, List<ControllerEvent> events) {
            throw new NotImplementedException("mock doesnt require this");
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

        @Override
        public void noteTime(long timestamp) {
            
        }
    }
}
