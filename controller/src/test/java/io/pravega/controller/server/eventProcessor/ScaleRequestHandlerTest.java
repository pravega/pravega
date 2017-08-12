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
import com.google.common.collect.Sets;
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
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
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleRequestHandlerTest {
    private final String scope = "scope";
    private final String stream = "stream";
    StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
            ScalingPolicy.byEventRate(0, 2, 3)).build();

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

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);

        taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);

        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);
        clientFactory = mock(ClientFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executor, hostId, connectionFactory);
        streamMetadataTasks.initializeStreamWriters(clientFactory, Config.SCALE_STREAM_NAME);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelper, executor, hostId, connectionFactory);

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
        executor.shutdown();
    }

    @Test(timeout = 20000)
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        AutoScaleRequestHandler requestHandler = new AutoScaleRequestHandler(streamMetadataTasks, streamStore, executor);
        ScaleOperationRequestHandler scaleRequestHandler = new ScaleOperationRequestHandler(streamMetadataTasks, streamStore, executor);
        RequestHandlerMultiplexer multiplexer = new RequestHandlerMultiplexer(requestHandler, scaleRequestHandler);
        // Send number of splits = 1
        AutoScaleEvent request = new AutoScaleEvent(scope, stream, 2, AutoScaleEvent.UP, System.currentTimeMillis(), 1, false);
        CompletableFuture<ScaleOpEvent> request1 = new CompletableFuture<>();
        CompletableFuture<ScaleOpEvent> request2 = new CompletableFuture<>();
        EventStreamWriter<ControllerEvent> writer = createWriter(x -> {
            if (!request1.isDone()) {
                final ArrayList<AbstractMap.SimpleEntry<Double, Double>> expected = new ArrayList<>();
                double start = 2.0 / 3.0;
                double end = 1.0;
                double middle = (start + end) / 2;
                expected.add(new AbstractMap.SimpleEntry<>(start, middle));
                expected.add(new AbstractMap.SimpleEntry<>(middle, end));
                checkRequest(request1, x, Lists.newArrayList(2), expected);
            } else if (!request2.isDone()) {
                final ArrayList<AbstractMap.SimpleEntry<Double, Double>> expected = new ArrayList<>();
                double start = 2.0 / 3.0;
                double end = 1.0;
                expected.add(new AbstractMap.SimpleEntry<>(start, end));
                checkRequest(request2, x, Lists.newArrayList(3, 4), expected);
            }
        });

        when(clientFactory.createEventWriter(eq(Config.SCALE_STREAM_NAME), eq(new JavaSerializer<ControllerEvent>()), any())).thenReturn(writer);

        assertTrue(FutureHelpers.await(multiplexer.process(request, writer::writeEvent)));
        assertTrue(FutureHelpers.await(request1));
        assertTrue(FutureHelpers.await(multiplexer.process(request1.get(), writer::writeEvent)));

        // verify that the event is posted successfully
        List<Segment> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 2));
        // verify that two splits are created even when we sent 1 as numOfSplits in AutoScaleEvent.
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new AutoScaleEvent(scope, stream, 4, AutoScaleEvent.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(multiplexer.process(request, e -> CompletableFuture.completedFuture(null))));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new AutoScaleEvent(scope, stream, 3, AutoScaleEvent.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(multiplexer.process(request, e -> CompletableFuture.completedFuture(null))));
        assertTrue(FutureHelpers.await(request2));
        assertTrue(FutureHelpers.await(multiplexer.process(request2.get(), e -> CompletableFuture.completedFuture(null))));

        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);

        // make it throw a non retryable failure so that test does not wait for number of retries.
        // This will bring down the test duration drastically because a retryable failure can keep retrying for few seconds.
        // And if someone changes retry durations and number of attempts in retry helper, it will impact this test's running time.
        // hence sending incorrect segmentsToSeal list which will result in a non retryable failure and this will fail immediately
        assertFalse(FutureHelpers.await(multiplexer.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(6),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), true, System.currentTimeMillis()),
                e -> CompletableFuture.completedFuture(null))));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);

        assertFalse(FutureHelpers.await(multiplexer.process(new AbortEvent(scope, stream, 0, UUID.randomUUID()),
                e -> CompletableFuture.completedFuture(null))));
    }

    private void checkRequest(CompletableFuture<ScaleOpEvent> request, ControllerEvent in, List<Integer> segmentsToSeal, List<AbstractMap.SimpleEntry<Double, Double>> expected) {
        if (in instanceof ScaleOpEvent) {
            ScaleOpEvent event = (ScaleOpEvent) in;
            if (!event.isRunOnlyIfStarted() && event.getScope().equals(scope) &&
                    event.getStream().equals(stream) && Sets.newHashSet(event.getSegmentsToSeal()).equals(Sets.newHashSet(segmentsToSeal)) &&
                    Sets.newHashSet(event.getNewRanges()).equals(Sets.newHashSet(expected))) {
                request.complete(event);
            } else {
                request.completeExceptionally(new RuntimeException());
            }

        } else {
            request.completeExceptionally(new RuntimeException());
        }
    }

    private EventStreamWriter<ControllerEvent> createWriter(Consumer<ControllerEvent> consumer) {
        return new EventStreamWriter<ControllerEvent>() {
            @Override
            public CompletableFuture<Void> writeEvent(ControllerEvent event) {
                consumer.accept(event);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void>  writeEvent(String routingKey, ControllerEvent event) {
                consumer.accept(event);
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public Transaction<ControllerEvent> beginTxn(long transactionTimeout, long maxExecutionTime,
                                                        long scaleGracePeriod) {
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
        };
    }
}
