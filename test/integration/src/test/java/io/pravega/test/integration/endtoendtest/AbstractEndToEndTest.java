/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

public class AbstractEndToEndTest extends ThreadPooledTestSuite {
    protected static final String SCOPE = "testScope";
    protected static final String STREAM = "testStream1";

    protected final int controllerPort = TestUtils.getAvailableListenPort();
    protected final String serviceHost = "localhost";
    protected final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    protected final int servicePort = TestUtils.getAvailableListenPort();
    protected final int containerCount = 4;
    protected TestingServer zkTestServer;
    protected PravegaConnectionListener server;
    protected ControllerWrapper controllerWrapper;
    protected ServiceBuilder serviceBuilder;
    protected final Serializer<String> serializer = new JavaSerializer<>();

    private final Random random = new Random();
    protected final Supplier<String> randomKeyGenerator = () -> String.valueOf(random.nextInt());
    protected final Function<Integer, String> getEventData = eventNumber -> String.valueOf(eventNumber) + ":constant data"; //event

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                                                  false,
                                                  controllerPort,
                                                  serviceHost,
                                                  servicePort,
                                                  containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    protected void createScope(final String scopeName) {
        @Cleanup
        Controller controller = Exceptions.handleInterrupted(controllerWrapper::getController);
        controller.createScope(scopeName).join();
    }

    protected void createStream(final String scopeName, final String streamName, final ScalingPolicy scalingPolicy) {
        @Cleanup
        Controller controller = Exceptions.handleInterrupted(controllerWrapper::getController);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(scopeName)
                                                        .streamName(streamName)
                                                        .scalingPolicy(scalingPolicy)
                                                        .build();
        controller.createStream(config).join();
    }

    protected void readAndVerify(final EventStreamReader<String> reader, int...eventIds) throws ReinitializationRequiredException {
        ArrayList<String> results = new ArrayList<>(eventIds.length);

        // Attempt reading eventIds.length events
        for (int eventId : eventIds) {
            String event = reader.readNextEvent(15000).getEvent();
            while (event == null) { //try until a non null event is read
                event = reader.readNextEvent(15000).getEvent();
            }
            results.add(event);
        }

        //Verify if we have recieved the events according to the event ids provided.
        Arrays.stream(eventIds).forEach(i -> assertTrue(results.contains(getEventData.apply(i))));
    }

    protected Segment getSegment(int segmentNumber, int epoch) {
        return new Segment(SCOPE, STREAM, StreamSegmentNameUtils.computeSegmentId(0, 0));
    }
}
