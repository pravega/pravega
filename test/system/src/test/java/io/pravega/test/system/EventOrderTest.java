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

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class EventOrderTest {

    private static final String STREAM = "testEventOrderStream";
    private static final String SCOPE = "testEventOrderScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testEventOrderRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private final static String ROUTING_KEY_SEPARATOR = ":";

    private final static int SEGMENTS = 1;
    private final static int RW_PROCESSES = 4;
    private final static long TOTAL_EVENTS = 10000;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(SEGMENTS);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
                                                                  .streamName(STREAM)
                                                                  .scalingPolicy(scalingPolicy).build();
    private final Map<String, AtomicLong> routingKeyCounter = new HashMap<>();
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

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * This test checks the order of events in the context of a Stream. To this end, a set of writers write a series of
     * events (TOTAL_EVENTS / RW_PROCESSES), each one to a unique routing key. The content of events is generated
     * following the pattern routingKey:seq_number, where seq_number is monotonically increasing for every routing key,
     * being the increment between consecutive seq_number values always 1. Then, a group of readers read all these
     * events from the Stream. Every time a reader reads an event, a callback is executed: it parses the event and
     * updates a shared map across all the readers (routingKeyCounter). In that map, keys are routing keys of writers
     * and values the most recent seq_number. If a reader gets a new event for a key already initialized in the map, the
     * callback asserts that the new value read for that routing key is seq_number + 1. This ensures that readers
     * receive events in the same order that writers produced them.
     */
    @Test
    public void eventOrderTest() {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                         .stream(Stream.of(SCOPE, STREAM))
                                                                         .build());

        // This callback will be executed after each event read to verify that the order of events is respected.
        Consumer<String> callback = event -> {
            String key = event.split(ROUTING_KEY_SEPARATOR)[0];
            long count = Long.valueOf(event.split(ROUTING_KEY_SEPARATOR)[1]);
            if (!routingKeyCounter.containsKey(key)) {
                routingKeyCounter.put(key, new AtomicLong(count));
            } else {
                // The new event for a given routing key should be exactly +1 compared to the existing value.
                Assert.assertEquals(routingKeyCounter.get(key).get() + 1, count);
                routingKeyCounter.get(key).set(count);
            }
        };

        // Write and read TOTAL_EVENTS to the Stream.
        log.info("Starting writers and readers.");
        List<CompletableFuture<Void>> writerFutures = writeEventFutures(clientFactory, STREAM, RW_PROCESSES, TOTAL_EVENTS / RW_PROCESSES);
        List<CompletableFuture<Long>> readerFutures = readEventFutures(clientFactory, READER_GROUP, RW_PROCESSES, callback);

        // Wait for all readers and writers to complete.
        log.info("Waiting for writers and readers to complete.");
        Futures.allOf(writerFutures).join();
        Futures.allOf(readerFutures).join();

        // Check that all events have been read.
        Assert.assertEquals("Expected events read: ", TOTAL_EVENTS,
                (long) readerFutures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get());
        log.info("Read all events in correct order. EventOrderTest passed.");
    }

    private List<CompletableFuture<Void>> writeEventFutures(ClientFactory client, String streamName, int numWriters, long totalEvents) {
        return IntStream.range(0, numWriters)
                        .mapToObj(i -> CompletableFuture.runAsync(() -> {
                            EventStreamWriter<String> w = client.createEventWriter(streamName, new JavaSerializer<>(),
                                    EventWriterConfig.builder().build());
                            writeEvents(w, String.valueOf(i), totalEvents);
                        })).collect(toList());
    }

    private void writeEvents(EventStreamWriter<String> writer, String routingKey, long totalEvents) {
        for (long i = 0; i < totalEvents; i++) {
            writer.writeEvent(routingKey, routingKey + ROUTING_KEY_SEPARATOR + String.valueOf(i)).join();
            log.debug("Writing event: {} to routing key {}", i, routingKey);
        }
        writer.close();
    }

    private List<CompletableFuture<Long>> readEventFutures(ClientFactory client, String rGroup, int numReaders, long limit,
                                                           Consumer<String> callback) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r, limit, callback))).collect(toList());
    }

    private List<CompletableFuture<Long>> readEventFutures(ClientFactory clientFactory, String readerGroup, int numReaders,
                                                           Consumer<String> callback) {
        return readEventFutures(clientFactory, readerGroup, numReaders, Long.MAX_VALUE, callback);
    }

    @SneakyThrows
    private <T> long readEvents(EventStreamReader<T> reader, long limit, Consumer<T> callback) {
        EventRead<T> event;
        long validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(5000);
                log.debug("Event read: {}.", event.getEvent());
                if (event.getEvent() != null) {
                    validEvents++;
                    if (callback != null) {
                        callback.accept(event.getEvent());
                    }
                }
            } while ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit);
        } finally {
            reader.close();
        }

        return validEvents;
    }
}
