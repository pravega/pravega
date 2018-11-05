/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.controller.server;

import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.SegmentSelector;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DebugStreamSegmentsTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testStream1";
    private static final int NUMBER_OF_WRITERS = 3;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Serializer<AutoScaleEvent> autoScaleEventSerializer = new JavaSerializer<>();
    private final Random random = new Random();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private ScheduledExecutorService writerExecutor;
    private ScheduledExecutorService readExecutor;
    private ScheduledExecutorService scaleExecutor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        writerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(NUMBER_OF_WRITERS, "writer-pool");
        readExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "reader-pool");
        scaleExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "scale-pool");
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false, controllerPort, serviceHost,
                                                  servicePort, containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        writerExecutor.shutdownNow();
        readExecutor.shutdownNow();
        scaleExecutor.shutdownNow();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test
    public void testOutOfSequence() throws Exception {
        // 1. Prepare
        createScope(SCOPE);
        createStream(STREAM);

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        when(streamFactory.createOutputStreamForSegment(any(), any(), any(),
                                                        any())).thenReturn(mock(SegmentOutputStream.class));
        SegmentSelector selector = new SegmentSelector(Stream.of(SCOPE, STREAM), controllerWrapper.getController(),
                                                       streamFactory, EventWriterConfig.builder().build());

        // 2.Create clientFactory.
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        @Cleanup
        ClientFactory clientFactoryInternal = ClientFactory.withScope("_system", controllerUri);

        @Cleanup
        final Controller controller = controllerWrapper.getController();

        Segment[] lastSegments = new Segment[100];
        for (int i = 0; i < 10; i++) {
            randomScaleUpScaleDown(clientFactoryInternal, controller);
            selector.refreshSegmentEventWriters(segment -> {
            });
            for (int key = 0; key < 100; key++) {
                Segment segment = selector.getSegmentForEvent("key-" + key);
                if (lastSegments[key] != null) {
                    int lastEpoch = StreamSegmentNameUtils.getEpoch(lastSegments[key].getSegmentId());
                    int thisEpoch = StreamSegmentNameUtils.getEpoch(segment.getSegmentId());
                    assertTrue(thisEpoch >= lastEpoch);
                    if (thisEpoch == lastEpoch) {
                        assertEquals(lastSegments[key], segment);
                    }
                }
                lastSegments[key] = segment;
            }
        }
    }

    private void randomScaleUpScaleDown(final ClientFactory clientFactory, final Controller controller) {
        @Cleanup
        EventStreamWriter<AutoScaleEvent> requestStreamWriter = clientFactory.createEventWriter("_requeststream",
                                                                                                autoScaleEventSerializer,
                                                                                                EventWriterConfig.builder()
                                                                                                                 .build());
        final Collection<Segment> currentSegments = controller.getCurrentSegments(SCOPE, STREAM).join().getSegments();
        Assert.assertTrue("Current Number of segments cannot be zero", currentSegments.size() > 0);

        // fetch a randomSegment
        final Segment randomSegment = currentSegments.toArray(new Segment[0])[random.nextInt(currentSegments.size())];
        AutoScaleEvent scaleEvent = null;
        if (random.nextBoolean()) {
            // trigger random scale up
            scaleEvent = new AutoScaleEvent(randomSegment.getScope(), randomSegment.getStreamName(),
                                            randomSegment.getSegmentId(), AutoScaleEvent.UP, System.currentTimeMillis(),
                                            2, false, random.nextInt());
        } else {
            // trigger random scale down.
            scaleEvent = new AutoScaleEvent(randomSegment.getScope(), randomSegment.getStreamName(),
                                            randomSegment.getSegmentId(), AutoScaleEvent.DOWN,
                                            System.currentTimeMillis(), 2, false, random.nextInt()); // silent=false
        }
        Futures.getAndHandleExceptions(requestStreamWriter.writeEvent(scaleEvent),
                                       t -> new RuntimeException("Error while writing scale event", t));

    }

    private void createScope(final String scopeName) throws InterruptedException, ExecutionException {
        controllerWrapper.getControllerService().createScope(scopeName).get();
    }

    private void createStream(String streamName) throws Exception {
        Controller controller = controllerWrapper.getController();
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(SCOPE)
                                                        .streamName(streamName)
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 4))
                                                        .retentionPolicy(RetentionPolicy.bySizeBytes(100 * 1024))
                                                        .build();
        controller.createStream(config).get();
    }

}
