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
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import static org.junit.Assert.assertTrue;
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
    private  Service controllerInstance = null;
    private  Service segmentStoreInstance = null;

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {

        //1. Start 1 instance of zookeeper
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        //2. Start 3 bookies
        Service bkService = new BookkeeperService("bookkeeper", zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("bookkeeper service details: {}", bkUris);

        //3. start 2 instances of pravega controller
        Service conService = new PravegaControllerService("controller", zkUri, 2, 0.1, 700.0);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service instance details: {}", conUris);

        //4.start 2 instances of pravega segmentstore
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, conUris.get(0), 2, 0.1, 1000.0);
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("pravega segmentstore service instance2 details: {}", segUris);

    }

    @Before
    public void setup() {
        Service zkService = new ZookeeperService("zookeeper");
        Assert.assertTrue(zkService.isRunning());
        List<URI> zkUris = zkService.getServiceDetails();
        log.info("zookeeper service details: {}", zkUris);

        // Verify controller is running.
        this.controllerInstance = new PravegaControllerService("controller", zkUris.get(0));
        Assert.assertTrue(this.controllerInstance.isRunning());
        List<URI> conURIs = this.controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Verify segment stores is running.
        this.segmentStoreInstance = new PravegaSegmentStoreService("segmentstore", zkUris.get(0), conURIs.get(0));
        Assert.assertTrue(this.segmentStoreInstance.isRunning());
        Assert.assertEquals(1, this.segmentStoreInstance.getServiceDetails().size());
        log.info("Pravega segment store instance details: {}", this.segmentStoreInstance.getServiceDetails());
    }

    @Test
    public void multiReaderWriterTest() throws InterruptedException, URISyntaxException, ExecutionException  {

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");
        readWriteTest();

        //scale down SSS by 1 instance
        this.segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 SSS instance down");
        readWriteTest();

        this.segmentStoreInstance.scaleService(2, true);
        Thread.sleep(60000);
        this.controllerInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 controller instance down");
        readWriteTest();

        this.segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        log.info("Test with 1 controller  and 1 SSS instance down");
        readWriteTest();

    }

    private void readWriteTest() throws InterruptedException, ExecutionException {

        String scope = "testMultiReaderWriterScope" + new Random().nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testMultiReaderWriterReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);

        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(20);
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
        ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();

        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicLong eventData = new AtomicLong(); //data used by each of the writers.
        final AtomicLong eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        ClientFactory clientFactory = getClientFactory(scope);

        //start writing events to the stream with 20 writers

        log.info("creating {} writers", NUM_WRITERS);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (int i = 0; i < NUM_WRITERS; i++) {
            CompletableFuture<Void> writer = startNewWriter(eventData, clientFactory, stopWriteFlag);
            writerList.add(writer);
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
            CompletableFuture<Void> reader = startReader("reader"+i, clientFactory, readerGroupName,
                    eventsReadFromPravega, eventData, eventReadCount, stopReadFlag);
            readerList.add(reader);
        }

        //stop writing
        stopWriteFlag.set(true);

        //wait for list of completable future
        for (int i = 0; i < writerList.size(); i++) {
            CompletableFuture.allOf(writerList.get(i));
        }

        //stop reading
        stopReadFlag.set(true);

        //wait for list of completable future
        for (int i = 0; i < readerList.size(); i++) {
            CompletableFuture.allOf(readerList.get(i));
        }
        validateResults(eventData.get(), eventsReadFromPravega);
        log.debug("test {} succeed", "multiReaderWriterTest");
    }

    private CompletableFuture<Void> startNewWriter(final AtomicLong data, final ClientFactory clientFactory,
                                                      final AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());

            while (!exitFlag.get()) {
                try {
                    long value = data.incrementAndGet();
                   writer.writeEvent(String.valueOf(value), value);
                } catch (Throwable e) {
                    log.warn("test exception writing events: {}", e);
                    break;
                }
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

    private void validateResults(final long lastEventCount, final Collection<Long> readEvents) {
        log.info("Last Event Count is {}", lastEventCount);
        assertTrue("Overflow in the number of events published ", lastEventCount > 0);
        // Number of event read should be equal to number of events published.
        assertEquals(lastEventCount, readEvents.size());
        assertEquals(lastEventCount, new TreeSet<>(readEvents).size()); //check unique events.
    }
}

