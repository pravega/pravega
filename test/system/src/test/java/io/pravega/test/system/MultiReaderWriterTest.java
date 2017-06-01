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
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import mesosphere.marathon.client.utils.MarathonException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static java.util.concurrent.TimeUnit.SECONDS;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiReaderWriterTest extends AbstractScaleTests {

    private static final String STREAM_NAME = "testMultiReaderWriterStream";
    private static final int NUM_WRITERS = 20;
    private static final int NUM_READERS = 20;
    private static final long NUM_EVENTS = 20000;
    private AtomicBoolean stopReadFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {

        //1. Start 1 instance of zookeeper
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        //2. Start 3 bookies
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        //3. start 2 instances of pravega controller
        Service conService = new PravegaControllerService("controller", zkUri, 2, 0.1, 700.0);
        if (!conService.isRunning()) {
            conService.start(true);
        }
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);

        //4.start 2 instances of pravega segmentstore
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0), 2, 0.1, 1000.0);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service  details: {}", segUris);
    }

    @Before
    public void setup() {

        //1. Start 1 instance of zookeeper
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        // Verify controller is running.
        controllerInstance = new PravegaControllerService("controller",  zkUri, 2, 0.1, 700.0);
        Assert.assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Verify segment stores is running.
        segmentStoreInstance = new PravegaSegmentStoreService("segmentstore", zkUri, conURIs.get(0), 2, 0.1, 1000.0);
        Assert.assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega segment store instance details: {}", segmentStoreInstance.getServiceDetails());
    }

    @Test(timeout = 600000)
    public void multiReaderWriterTest() throws InterruptedException, URISyntaxException, ExecutionException  {

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");
        readWriteTest();

        //scale down SSS by 1 instance
        segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 SSS instance down");
        readWriteTest();

        segmentStoreInstance.scaleService(2, true);
        Thread.sleep(60000);
        controllerInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 controller instance down");
        readWriteTest();

        segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 controller  and 1 SSS instance down");
        readWriteTest();
    }

    private void readWriteTest() throws InterruptedException, ExecutionException {

        String scope = "testMultiReaderWriterScope" + new Random().nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testMultiReaderWriterReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);

        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(5);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope)
                .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();

        URI controllerUri = getControllerURI();
        Controller controller = getController(controllerUri);

        //create a scope
        Boolean createScopeStatus = controller.createScope(scope).get();
        log.debug("Create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(config).get();
        log.debug("Create stream status {}", createStreamStatus);
         eventsReadFromPravega = new ConcurrentLinkedQueue<>();

         //stopWriteFlag = new AtomicBoolean(false);
         stopReadFlag = new AtomicBoolean(false);
         eventData = new AtomicLong(); //data used by each of the writers.
         eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        log.info("events read from pravega {}", eventsReadFromPravega);
        log.info("data written {}", eventData.get());
        log.info("data read {}", eventReadCount.get());
        log.info(" stop read flag {}", stopReadFlag);
        ClientFactory clientFactory = getClientFactory(scope);

        //start writing events to the stream with 20 writers
        log.info("creating {} writers", NUM_WRITERS);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            log.info("starting writer{}", i);
            writerList.add(startNewWriter(eventData, clientFactory));
        }

        log.info("Creating Reader group : {}", readerGroupName);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerUri);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(STREAM_NAME));

        //create 20 readers
        log.info("creating {} readers", NUM_READERS);
        List<CompletableFuture<Void>> readerList = new ArrayList<>();
        for (int i = 0; i < NUM_READERS; i++) {
            log.info("starting reader{}", i);
            readerList.add(startReader("reader"+i, clientFactory, readerGroupName,
                    eventsReadFromPravega, eventData, eventReadCount, stopReadFlag));
        }
        while (NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }
        stopReadFlag.set(true);

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                    "Count: {}", eventData.get(), eventsReadFromPravega.size());
        //stop reading when no. of reads= no. of writes
        log.debug("test {} succeed", "multiReaderWriterTest");
        }

    private CompletableFuture<Void> startNewWriter(final AtomicLong data, final ClientFactory clientFactory
                                                      ) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            try {
                for (int i = 0; i < 1000; i++) {
                       long value = data.incrementAndGet();
                       log.debug("writing event {}", value);
                       writer.writeEvent(String.valueOf(value), value);
                }
                } catch (Throwable e) {
                    log.warn("test exception writing events: {}", e);
                }
        });
    }

    private CompletableFuture<Void> startReader(final String id, final ClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
                                                AtomicLong readCount, final AtomicBoolean exitFlag) {

        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    log.debug("reading event {}", longEvent );
                    if (longEvent != null) {
                        //update if event read is not null.
                        readResult.add(longEvent);
                        readCount.incrementAndGet();
                    }
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exception while reading from the stream", e);
                    break;
                }
            }
        });
    }
}

