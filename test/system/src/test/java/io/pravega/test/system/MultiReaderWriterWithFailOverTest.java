/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import mesosphere.marathon.client.utils.MarathonException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiReaderWriterWithFailOverTest {
    private static final String STREAM_NAME = "testMultiReaderWriterStream";
    private static final int NUM_WRITERS = 20;
    private static final int NUM_READERS = 20;
    private AtomicBoolean stopReadFlag;
    private AtomicBoolean stopWriteFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount1;
    private AtomicLong eventWriteCount2;
    private AtomicLong eventWriteCount3;
    private AtomicLong eventWriteCount4;
    private AtomicLong eventReadCount1;
    private AtomicLong eventReadCount2;
    private AtomicLong eventReadCount3;
    private AtomicLong eventReadCount4;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private URI controllerURIDirect = null;
    private final Object waitCondition = new Object();

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

        //3. start 3 instances of pravega controller
        Service conService = new PravegaControllerService("controllerFailover1", zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }
        conService.scaleService(3, true);
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service  details: {}", conUris);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());

        URI controllerURI = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURI);

        //4.start 3 instances of pravega segmentstore
        Service segService = new PravegaSegmentStoreService("segmentstoreFailover1", zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        segService.scaleService(3, true);
        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega Segmentstore service  details: {}", segUris);
    }

    @Before
    public void setup() {

        //Start 1 instance of zookeeper
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        // Verify controller is running.
        controllerInstance = new PravegaControllerService("controllerFailover1", zkUri);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = new PravegaSegmentStoreService("segmentstoreFailover1", zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());
    }

    @After
    public void tearDown() {
        if (controllerInstance != null && controllerInstance.isRunning()) {
            controllerInstance.stop();
        }
        if (segmentStoreInstance != null && segmentStoreInstance.isRunning()) {
            segmentStoreInstance.stop();
        }
    }

    @Test(timeout = 600000)
    public void multiReaderWriterWithFailOverTest() throws Exception {

        String scope = "testMultiReaderWriterScope" + new Random().nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testMultiReaderWriterReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
        //10  readers -> 10 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(10);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope)
                .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
        //get Controller Uri
        URI controllerUri = controllerURIDirect;
        Controller controller = new ControllerImpl(controllerUri);

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
        stopWriteFlag = new AtomicBoolean(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        //create a scope
        try (StreamManager streamManager = new StreamManagerImpl(controllerUri)) {
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Creating scope with scope name {}", scope);
            log.debug("Create scope status {}", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.debug("Create stream status {}", createStreamStatus);
        }

        //get ClientFactory instance
        log.info("Scope passed to client factory {}", scope);
        try (ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerUri)) {

            log.info("Client factory details {}", clientFactory.toString());
            //create writers
            log.info("Creating {} writers", NUM_WRITERS);
            List<EventStreamWriter<Long>> writerList = new ArrayList<>();
            log.info("Writers writing in the scope {}", scope);
            for (int i = 0; i < NUM_WRITERS; i++) {
                log.info("Starting writer{}", i);
                final EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                        new JavaSerializer<Long>(),
                        EventWriterConfig.builder().build());
                writerList.add(writer);
            }

            //create a reader group
            log.info("Creating Reader group : {}", readerGroupName);
            log.info("Scope passed to readergroup manager {}", scope);

            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                    Collections.singleton(STREAM_NAME));
            log.info("Reader group name {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
            log.info("Reader group scope {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());
            log.info("Online readers {}", readerGroupManager.getReaderGroup(readerGroupName).getOnlineReaders());

            //create readers
            log.info("Creating {} readers", NUM_READERS);
            List<EventStreamReader<Long>> readerList = new ArrayList<>();
            String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
            log.info("Scope that is seen by readers {}", scope);
            //start reading events
            for (int i = 0; i < NUM_READERS; i++) {
                log.info("Starting reader{}", i);
                log.info("Creating reader with id {}", readerName + i);
                final EventStreamReader<Long> reader = clientFactory.createReader(readerName + i,
                        readerGroupName,
                        new JavaSerializer<Long>(),
                        ReaderConfig.builder().build());
                readerList.add(reader);
            }
            readWriteTest(writerList, readerList, stopWriteFlag);

            //close all the writers
            writerList.forEach(writer -> writer.close());
            //close all readers
            readerList.forEach(reader -> reader.close());
            //delete readergroup
            log.info("Deleting readergroup {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
            //seal the stream
            CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
            log.info("Sealing stream {}", STREAM_NAME);
            assertTrue(sealStreamStatus.get());
            //delete the stream
            CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
            log.info("Deleting stream {}", STREAM_NAME);
            assertTrue(deleteStreamStatus.get());

            //delete the scope
            CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
            log.info("Deleting scope {}", scope);
            assertTrue(deleteScopeStatus.get());
            log.info("Test {} succeeds ", "MultiReaderWriterWithFailOver");
        }

    private void readWriteTest(final List<EventStreamWriter<Long>> writerList,
                               List<EventStreamReader<Long>> readerList, final AtomicBoolean stopWriteFlag) throws InterruptedException, ExecutionException {
        // start writing asynchronously
        List<CompletableFuture<Void>> writerFutureList = new ArrayList<>();
        writerList.forEach(writer -> {
            CompletableFuture<Void> writerFuture = startWriting(eventData, writer, stopWriteFlag);
            writerFutureList.add(writerFuture);
        });

        //start reading asynchronously
        List<CompletableFuture<Void>> readerFutureList = new ArrayList<>();
        readerList.forEach(reader -> {
            CompletableFuture<Void> readerFuture = startReading(eventsReadFromPravega, eventData, eventReadCount, stopReadFlag, reader);
            writerFutureList.add(readerFuture);
        });

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");
        eventWriteCount1 = eventData;
        eventReadCount1 =  eventReadCount;

        //Scale down SSS instances to 2
        segmentStoreInstance.scaleService(2, true);
        Thread.sleep(60000);
        log.info("Scaling down SSS instances from 3 to 2");
        eventWriteCount2 = eventData;
        eventReadCount2 =  eventReadCount;
        //ensure reads are happening
        assertTrue(eventReadCount2.get() > eventReadCount1.get());
        //ensure writes are happening
        assertTrue(eventWriteCount2.get() > eventWriteCount1.get());

        //Scale down controller instances to 2
        controllerInstance.scaleService(2, true);
        log.info("Scaling down controller instances from 3 to 2");
        List<URI> conURIs = controllerInstance.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        Thread.sleep(60000);
        eventWriteCount3 = eventData;
        eventReadCount3 =  eventReadCount;
        //ensure reads are happening
        assertTrue(eventReadCount3.get() > eventReadCount2.get());
        //ensure writes are happening
        assertTrue(eventWriteCount3.get() > eventWriteCount2.get());

        //Scale down SSS, controller to 1 instance each.
        segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        controllerInstance.scaleService(1, true);
        log.info("Scaling down  to 1 controller, 1 SSS instance");
        List<URI> conURIs1 = controllerInstance.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris1 = conURIs1.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris1));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        Thread.sleep(60000);
        eventWriteCount4 = eventData;
        eventReadCount4 =  eventReadCount;
        //ensure reads are happening
        assertTrue(eventReadCount4.get() > eventReadCount3.get());
        //ensure writes are happening
        assertTrue(eventWriteCount4.get() > eventWriteCount3.get());

        //set the stop write flag to true
        stopWriteFlag.set(true);

        //wait for writers completion
        FutureHelpers.allOf(writerFutureList);

        //set the stop read flag to true
        stopReadFlag.set(true);

        // wait for reads = writes
        synchronized (waitCondition) {
            while (!(eventsReadFromPravega.size() == eventData.get())) {
                waitCondition.wait();
            }
        }

        //wait for readers completion
        FutureHelpers.allOf(readerFutureList);
        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.
    }

    private CompletableFuture<Void> startWriting(final AtomicLong data, final EventStreamWriter<Long> writer, final AtomicBoolean stopWriteFlag) {
        return CompletableFuture.runAsync(() -> {
            while (!stopWriteFlag.get()) {
                try {
                    long value = data.incrementAndGet();
                    log.debug("Writing event {}", value);
                    writer.writeEvent(String.valueOf(value), value);
                    writer.flush();
                } catch (Throwable e) {
                    log.warn("Test exception writing events: {}", e);
                    break;
                }
            }
        });
    }

    private CompletableFuture<Void> startReading(final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
    AtomicLong readCount, final AtomicBoolean exitFlag, final EventStreamReader<Long> reader) {
        return CompletableFuture.runAsync(() -> {
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    log.debug("Reading event {}", longEvent);
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
            //notify if num. of reads = num. of writes
            if (writeCount == readCount) {
                synchronized (waitCondition) {
                    waitCondition.notify();
                }
            }
        });
    }
}
