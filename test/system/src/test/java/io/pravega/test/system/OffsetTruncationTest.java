/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class OffsetTruncationTest {

    private static final String STREAM = "testOffsetTruncationStream";
    private static final String SCOPE = "testOffsetTruncationScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testOffsetTruncationRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int PARALLELISM = 2;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "executor");
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(PARALLELISM);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
                                                                  .streamName(STREAM)
                                                                  .scalingPolicy(scalingPolicy).build();
    private URI controllerURI;
    private StreamManager streamManager;

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
     */
    @Environment
    public static void initialize() throws MarathonException {

        // 1. Check if zk is running, if not start it.
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        // Get the zk ip details and pass it to bk, host, controller.
        URI zkUri = zkUris.get(0);

        // 2. Check if bk is running, otherwise start, get the zk ip.
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        // 3. Start controller.
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega controller service details: {}", conUris);

        // 4.Start segmentstore.
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);
        streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
    }

    /**
     * This test verifies that truncation works specifying an offset that applies to multiple segments. To this end,
     * the test first writes a set of events on a Stream (with multiple segments) and truncates it at a specified offset
     * (truncatedEvents). The tests asserts that readers first get a TruncatedDataException as they are attempting to
     * read a truncated segment, and then they only read the remaining events that have not been truncated.
     */
    @Test
    public void offsetTruncationTest() {
        final int totalEvents = 200;
        final int truncatedEvents = 50;
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                           .clientConfig(ClientConfig.builder()
                                                                           .controllerURI(controllerURI).build()).build(),
                                                                           connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller);
        log.info("Invoking offsetTruncationTest test with Controller URI: {}", controllerURI);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM)).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);

        // Write events to the Stream.
        writeDummyEvents(clientFactory, STREAM, totalEvents);

        // Instantiate readers to consume from Stream up to truncatedEvents.
        List<CompletableFuture<Integer>> futures = readDummyEvents(clientFactory, READER_GROUP, PARALLELISM, truncatedEvents);
        Futures.allOf(futures).join();

        // Perform truncation on stream segment.
        Checkpoint cp = readerGroup.initiateCheckpoint("truncationCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();
        assertTrue(streamManager.truncateStream(SCOPE, STREAM, streamCut));

        // Just after the truncation, trying to read the whole stream should raise a TruncatedDataException.
        final String newGroupName = READER_GROUP + "new";
        groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM)).build());
        assertThrows(TruncatedDataException.class, () -> Futures.allOf(readDummyEvents(clientFactory, newGroupName, PARALLELISM)).join());

        // Read again, now expecting to read events from the offset defined in truncate call onwards.
        groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM)).build());
        futures = readDummyEvents(clientFactory, newGroupName, PARALLELISM);
        Futures.allOf(futures).join();
        assertEquals("Expected read events: ", totalEvents - (truncatedEvents * PARALLELISM),
                (int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get());
        log.debug("The stream has been successfully truncated at event {}. Offset truncation test passed.",
                truncatedEvents * PARALLELISM);
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    private void writeDummyEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < totalEvents; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.debug("Writing event: {} to stream {}", i, streamName);
        }
    }

    private List<CompletableFuture<Integer>> readDummyEvents(ClientFactory client, String rGroup, int numReaders, int limit) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r, limit))).collect(toList());
    }

    private List<CompletableFuture<Integer>> readDummyEvents(ClientFactory clientFactory, String readerGroup, int numReaders) {
        return readDummyEvents(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    @SneakyThrows
    private <T> int readEvents(EventStreamReader<T> reader, int limit) {
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    validEvents++;
                }
            } while ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit);
        } finally {
            reader.close();
        }

        return validEvents;
    }
}
