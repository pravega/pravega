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

import com.sun.xml.internal.ws.api.pipe.FiberContextSwitchInterceptor;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.mocks.ScaleEventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StartScaleResponse;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamConfigWithVersion;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.ScaleStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

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

        this.streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStorePartialMock, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStorePartialMock, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                new SealStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStorePartialMock, executor),
                executor);
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

        StreamConfigWithVersion configWithVersion = streamStorePartialMock.getConfigurationWithVersion(SCOPE, stream1, null, executor).join();
        assertTrue(configWithVersion.getVersion() == 0);
        // 1. happy day test
        // update.. should succeed
        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture = streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration,null);
        assertTrue(FutureHelpers.await(streamRequestHandler.processEvent((UpdateStreamEvent) requestEventWriter.eventQueue.take())));
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture.join());

        configWithVersion = streamStorePartialMock.getConfigurationWithVersion(SCOPE, stream1, null, executor).join();
        assertTrue(configWithVersion.getVersion() == 1);
        assertTrue(configWithVersion.getConfiguration().equals(streamConfiguration));

        streamConfiguration = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(6)).build();
        // 2. change state to scaling
        streamStorePartialMock.setState(SCOPE, stream1, State.SCALING, null, executor).get();
        // call update should fail without posting the event
        updateOperationFuture = streamMetadataTasks.updateStream(SCOPE, stream1, streamConfiguration,null);
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateOperationFuture.get());
        UpdateStreamTask updateStreamTask = new UpdateStreamTask(streamMetadataTasks, streamStorePartialMock, executor);
        AssertExtensions.assertThrows("", updateStreamTask.execute((UpdateStreamEvent) requestEventWriter.eventQueue.take()),
                e -> ExceptionHelpers.getRealException(e) instanceof TaskExceptions.StartException);
        streamStorePartialMock.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        // 3. multiple back to back updates
        StreamConfiguration streamConfiguration1 = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.byEventRate(1, 1, 2)).build();

        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture1 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration1, null);

        StreamConfiguration streamConfiguration2 = StreamConfiguration.builder()
                .scope(SCOPE)
                .streamName(stream1)
                .scalingPolicy(ScalingPolicy.fixed(7)).build();

        CompletableFuture<UpdateStreamStatus.Status> updateOperationFuture2 = streamMetadataTasks.updateStream(SCOPE, stream1,
                streamConfiguration2, null);

        assertTrue(FutureHelpers.await(streamRequestHandler.processEvent(requestEventWriter.eventQueue.take())));

        assertFalse(FutureHelpers.await(updateStreamTask.execute((UpdateStreamEvent) requestEventWriter.eventQueue.take())));

        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateOperationFuture1.join());
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateOperationFuture2.join());

        configWithVersion = streamStorePartialMock.getConfigurationWithVersion(SCOPE, stream1, null, executor).join();
        assertTrue(configWithVersion.getVersion() == 2);
        assertTrue(configWithVersion.getConfiguration().equals(streamConfiguration1));

    }

    @Test(timeout = 30000)
    public void sealStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        WriterMock requestEventWriter = new WriterMock(streamMetadataTasks, executor);
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        //seal a stream.
        CompletableFuture<UpdateStreamStatus.Status> sealOperationResult = streamMetadataTasks.sealStream(SCOPE, stream1, null);

        assertTrue(FutureHelpers.await(streamRequestHandler.processEvent(requestEventWriter.getEventQueue().take())));

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

        assertTrue(FutureHelpers.await(streamRequestHandler.processEvent(requestEventWriter.getEventQueue().take())));

        assertTrue(streamStorePartialMock.isSealed(SCOPE, stream1, null, executor).get());
        FutureHelpers.await(sealOperationResult);
        assertEquals(UpdateStreamStatus.Status.SUCCESS, sealOperationResult.get());

        // delete after seal
        CompletableFuture<Controller.DeleteStreamStatus.Status> future = streamMetadataTasks.deleteStream(SCOPE, stream1, null);
        assertTrue(FutureHelpers.await(streamRequestHandler.processEvent(requestEventWriter.getEventQueue().take())));

        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, future.get());
    }

    @Test
    public void eventWriterInitializationTest() throws Exception {
        final ScalingPolicy policy = ScalingPolicy.fixed(1);

        final StreamConfiguration configuration = StreamConfiguration.builder().scope(SCOPE).streamName("test").scalingPolicy(policy).build();

        streamStorePartialMock.createStream(SCOPE, "test", configuration, System.currentTimeMillis(), null, executor).get();
        streamStorePartialMock.setState(SCOPE, "test", State.ACTIVE, null, executor).get();

        AssertExtensions.assertThrows("", () -> streamMetadataTasks.manualScale(SCOPE, "test", Collections.singletonList(0),
                Arrays.asList(), 30, null).get(), e -> e instanceof TaskExceptions.RequestProcessingNotEnabledException);

        streamMetadataTasks.setRequestEventWriter(new ScaleEventStreamWriterMock(streamMetadataTasks, executor));
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
                ex -> ExceptionHelpers.getRealException(ex) instanceof StoreException.IllegalStateException);

        List<Segment> segments = streamMetadataTasks.startScale((ScaleOpEvent) requestEventWriter.getEventQueue().take(), true, context).get();

        assertTrue(segments.stream().anyMatch(x -> x.getNumber() == 1 && x.getKeyStart() == 0.0 && x.getKeyEnd() == 0.5));
        assertTrue(segments.stream().anyMatch(x -> x.getNumber() == 2 && x.getKeyStart() == 0.5 && x.getKeyEnd() == 1.0));
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
        public Transaction<ControllerEvent> beginTxn(long transactionTimeout, long maxExecutionTime, long scaleGracePeriod) {
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
