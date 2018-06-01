/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.test.common.TestingServerStarter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ScaleRequestHandlerTest {
    private final String scope = "scope";
    private final String stream = "stream";
    StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
            ScalingPolicy.byEventRate(1, 2, 3)).build();

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;

    private CuratorFramework zkClient;
    private ClientFactory clientFactory;
    private ConnectionFactoryImpl connectionFactory;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(20, 1, 50));

        zkClient.start();

        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        streamStore = spy(StreamStoreFactory.createZKStore(zkClient, executor));

        taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);

        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = mock(ClientFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executor, hostId, connectionFactory, false, "");
        streamMetadataTasks.initializeStreamWriters(clientFactory, Config.SCALE_STREAM_NAME);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelper, executor, hostId, connectionFactory, false, "");

        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope).get();
        streamMetadataTasks.createStream(scope, stream, config, createTimestamp).get();
    }

    @After
    public void tearDown() throws Exception {
        clientFactory.close();
        connectionFactory.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        AutoScaleTask requestHandler = new AutoScaleTask(streamMetadataTasks, streamStore, executor);
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler multiplexer = new StreamRequestHandler(requestHandler, scaleRequestHandler, null, null, null, null, executor);
        // Send number of splits = 1
        EventWriterMock writer = new EventWriterMock();

        when(clientFactory.createEventWriter(eq(Config.SCALE_STREAM_NAME), eq(new JavaSerializer<ControllerEvent>()), any())).thenReturn(writer);

        AutoScaleEvent scaleUpEvent = new AutoScaleEvent(scope, stream, 2, AutoScaleEvent.UP, System.currentTimeMillis(), 1, false);
        assertTrue(Futures.await(multiplexer.process(scaleUpEvent)));

        // verify that one scaleOp event is written into the stream
        assertEquals(1, writer.queue.size());
        ControllerEvent event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleOpEvent = (ScaleOpEvent) event;
        double start = 2.0 / 3.0;
        double end = 1.0;
        double middle = (start + end) / 2;
        assertEquals(2, scaleOpEvent.getNewRanges().size());
        double delta = 0.0000000000001;
        assertEquals(start, scaleOpEvent.getNewRanges().get(0).getKey(), delta);
        assertEquals(middle, scaleOpEvent.getNewRanges().get(0).getValue(), delta);
        assertEquals(middle, scaleOpEvent.getNewRanges().get(1).getKey(), delta);
        assertEquals(end, scaleOpEvent.getNewRanges().get(1).getValue(), delta);
        assertEquals(1, scaleOpEvent.getSegmentsToSeal().size());
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(2));

        assertTrue(Futures.await(multiplexer.process(scaleOpEvent)));

        // verify that the event is processed successfully
        List<Segment> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 2));
        // verify that two splits are created even when we sent 1 as numOfSplits in AutoScaleEvent.
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        // process first scale down event. it should only mark the segment as cold
        AutoScaleEvent scaleDownEvent = new AutoScaleEvent(scope, stream, 4, AutoScaleEvent.DOWN, System.currentTimeMillis(), 0, false);
        assertTrue(Futures.await(multiplexer.process(scaleDownEvent)));
        assertTrue(writer.queue.isEmpty());

        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);
        assertTrue(streamStore.isCold(scope, stream, 4, null, executor).join());

        AutoScaleEvent scaleDownEvent2 = new AutoScaleEvent(scope, stream, 3, AutoScaleEvent.DOWN, System.currentTimeMillis(), 0, false);
        assertTrue(Futures.await(multiplexer.process(scaleDownEvent2)));
        assertTrue(streamStore.isCold(scope, stream, 3, null, executor).join());

        // verify that a new event has been posted
        assertEquals(1, writer.queue.size());
        event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        scaleOpEvent = (ScaleOpEvent) event;
        assertEquals(1, scaleOpEvent.getNewRanges().size());
        assertEquals(start, scaleOpEvent.getNewRanges().get(0).getKey(), delta);
        assertEquals(end, scaleOpEvent.getNewRanges().get(0).getValue(), delta);
        assertEquals(2, scaleOpEvent.getSegmentsToSeal().size());
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(3));
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(4));

        // process scale down event
        assertTrue(Futures.await(multiplexer.process(scaleOpEvent)));

        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);

        // make it throw a non retryable failure so that test does not wait for number of retries.
        // This will bring down the test duration drastically because a retryable failure can keep retrying for few seconds.
        // And if someone changes retry durations and number of attempts in retry helper, it will impact this test's running time.
        // hence sending incorrect segmentsToSeal list which will result in a non retryable failure and this will fail immediately
        assertFalse(Futures.await(multiplexer.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(6),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), true, System.currentTimeMillis()))));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);

        assertFalse(Futures.await(multiplexer.process(new AbortEvent(scope, stream, 0, UUID.randomUUID()))));
    }

    @Test
    public void testScaleRange() throws ExecutionException, InterruptedException {
        // key range values taken from issue #2543
        Segment segment = new Segment(2, 1, 100L, 0.1706574888245243, 0.7085170563088633);
        doReturn(CompletableFuture.completedFuture(segment)).when(streamStore).getSegment(any(), any(), anyInt(), any(), any());

        AutoScaleTask requestHandler = new AutoScaleTask(streamMetadataTasks, streamStore, executor);
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler multiplexer = new StreamRequestHandler(requestHandler, scaleRequestHandler, null, null, null, null, executor);
        // Send number of splits = 1
        EventWriterMock writer = new EventWriterMock();

        when(clientFactory.createEventWriter(eq(Config.SCALE_STREAM_NAME), eq(new JavaSerializer<ControllerEvent>()), any())).thenReturn(writer);

        AutoScaleEvent scaleUpEvent = new AutoScaleEvent(scope, stream, 2, AutoScaleEvent.UP, System.currentTimeMillis(), 1, false);
        assertTrue(Futures.await(multiplexer.process(scaleUpEvent)));

        reset(streamStore);
        // verify that one scaleOp event is written into the stream
        assertEquals(1, writer.queue.size());
        ControllerEvent event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleOpEvent = (ScaleOpEvent) event;

        assertEquals(2, scaleOpEvent.getNewRanges().size());
        assertEquals(0.1706574888245243, scaleOpEvent.getNewRanges().get(0).getKey(), 0.0);
        assertEquals(0.7085170563088633, scaleOpEvent.getNewRanges().get(1).getValue(), 0.0);
        assertTrue(scaleOpEvent.getNewRanges().get(0).getValue().doubleValue() == scaleOpEvent.getNewRanges().get(1).getKey().doubleValue());
    }

    private static class EventWriterMock implements EventStreamWriter<ControllerEvent> {
        BlockingQueue<ControllerEvent> queue = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Void> writeEvent(ControllerEvent event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void>  writeEvent(String routingKey, ControllerEvent event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
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
