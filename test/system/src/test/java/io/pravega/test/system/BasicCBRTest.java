/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BasicCBRTest extends AbstractReadWriteTest {

    private static final String SCOPE = "testCBRScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String STREAM = "testCBRStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testCBRReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    private static final int TOTAL_EVENTS = 10;
    private static final int EXTRA_EVENTS = TOTAL_EVENTS / 2;
    private static final int TOTAL_EXTRA_EVENTS = TOTAL_EVENTS + EXTRA_EVENTS;
    private static final int EVENT_SIZE = 18;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);

        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM,
                StreamConfiguration.builder()
                        .scalingPolicy(ScalingPolicy.fixed(1))
                        .retentionPolicy(RetentionPolicy.bySizeBytes(10, (TOTAL_EVENTS + 1) * EVENT_SIZE)).build()));
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void basicCBRTest() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        // write more events than retention max.
        writeEvents(clientFactory, STREAM, TOTAL_EXTRA_EVENTS);

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder()
                .stream(Stream.of(SCOPE, STREAM)).build());
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);

        // read those events.
        readEventFutures(clientFactory, READER_GROUP, 1);

        // verify truncation happens at the retention max.
        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(SCOPE, STREAM), 0L).join();
            System.out.println("The segments none1: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 120 * 1000L);

        // write more events than retention max.
        writeEvents(clientFactory, STREAM, TOTAL_EVENTS);

        readerGroup.resetReaderGroup(ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .stream(Stream.of(SCOPE, STREAM)).build());

        // verify truncation happens at the retention max as no stream-cut is published.
        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(SCOPE, STREAM), 0L).join();
            System.out.println("The segments none2: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 120 * 1000L);

        // fill the stream upto retention max.
        writeEvents(clientFactory, STREAM, TOTAL_EVENTS - EXTRA_EVENTS);
        // read a few events.
        readEventFutures(clientFactory, READER_GROUP, 1, EXTRA_EVENTS + 1);

        // create a checkpoint and call updateRetentionStream.
        Checkpoint checkpoint = readerGroup.initiateCheckpoint("manual checkpoint", executor).join();
        readerGroup.updateRetentionStreamCut(checkpoint.asImpl().getPositions());

        // verify truncation happens at the provided stream-cut.
        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(SCOPE, STREAM), 0L).join();
            System.out.println("The segments none3: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 120 * 1000L);

        readerGroup.resetReaderGroup(ReaderGroupConfig.builder()
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM)).build());

        // fill the stream upto retention max.
        writeEvents(clientFactory, STREAM, EXTRA_EVENTS + 1);
        // read a few events.
        readEventFutures(clientFactory, READER_GROUP, 1, EXTRA_EVENTS + 1);

        readerGroup.initiateCheckpoint("auto checkpoint", executor).join();

        // verify truncation happens at the checkpoint.
        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(SCOPE, STREAM), 0L).join();
            System.out.println("The segments none4: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 120 * 1000L);
    }
}
