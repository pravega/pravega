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
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWriteAndScaleWithFailoverTest extends AbstractScaleTests {

    private static final String STREAM_NAME = "testReadWriteAndScaleStream";
    private static final int INITIAL_NUM_WRITERS = 2;
    private static final int ADD_NUM_WRITERS = 6;
    private static final int TOTAL_NUM_WRITERS = 8;
    private static final int NUM_READERS = 2;
    private static final int ZK_DEFAULT_SESSION_TIMEOUT = 30000;
    private AtomicBoolean stopReadFlag;
    private AtomicBoolean stopWriteFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private URI controllerURIDirect = null;
    private ExecutorService executorService;
    private Controller controller;
    private final String scope = "testReadWriteAndScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testReadWriteAndScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private ScalingPolicy scalingPolicy =  ScalingPolicy.byEventRate(1, 2, NUM_READERS);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
    private final String readerName = "reader";
    private Retry.RetryWithBackoff retry = Retry.withExpBackoff(10, 10, 20, ofSeconds(1).toMillis());


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
        Service conService = new PravegaControllerService("controller", zkUri);
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
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        segService.scaleService(3, true);
        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega Segmentstore service  details: {}", segUris);

    }

    @Before
    public void setup() {

        // Get zk details to verify if controller, SSS are running
        Service zkService = new ZookeeperService("zookeeper");
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        // Verify controller is running.
        controllerInstance = new PravegaControllerService("controller", zkUri);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = new PravegaSegmentStoreService("segmentstore", zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());

        //executor service
        executorService = Executors.newFixedThreadPool(NUM_READERS + TOTAL_NUM_WRITERS);
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect);
        //read and write count variables
        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
        stopWriteFlag = new AtomicBoolean(false);
        eventData = new AtomicLong(0); //data used by each of the writers.
        eventReadCount = new AtomicLong(0); // used by readers to maintain a count of events.
        eventWriteCount = new AtomicLong(0);
    }

    @After
    public void tearDown() {
        controllerInstance.scaleService(1, true);
        segmentStoreInstance.scaleService(1, true);
        executorService.shutdownNow();
        eventsReadFromPravega.clear();
    }

    public void readWriteAndScaleWithFailoverTest() throws Exception {

        try (StreamManager streamManager = new StreamManagerImpl(controllerURIDirect)) {
            //create a scope
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
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect)) {

            log.info("Client factory details {}", clientFactory.toString());
            //create writers
            log.info("Creating {} writers", INITIAL_NUM_WRITERS);
            List<EventStreamWriter<Long>> writerList = new ArrayList<>(INITIAL_NUM_WRITERS);
            log.info("Writers writing in the scope {}", scope);
            for (int i = 0; i < INITIAL_NUM_WRITERS; i++) {
                log.info("Starting writer{}", i);
                final EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                        new JavaSerializer<Long>(),
                        EventWriterConfig.builder().retryAttempts(10).build());
                writerList.add(writer);
            }

            //create a reader group
            log.info("Creating Reader group: {}, with readergroup manager using scope: {}", readerGroupName, scope);
            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                    Collections.singleton(STREAM_NAME));
            log.info("Reader group name: {}, Reader group scope: {}, Online readers: {}",
                    readerGroupManager.getReaderGroup(readerGroupName).getGroupName(), readerGroupManager
                            .getReaderGroup(readerGroupName).getScope(), readerGroupManager
                            .getReaderGroup(readerGroupName).getOnlineReaders());

            //create readers
            log.info("Creating {} readers", NUM_READERS);
            List<EventStreamReader<Long>> readerList = new ArrayList<>(NUM_READERS);
            log.info("Scope that is seen by readers {}", scope);
            //start reading events
            for (int i = 0; i < NUM_READERS; i++) {
                log.info("Starting reader: {}, with id: {}", i, readerName + i);
                final EventStreamReader<Long> reader = clientFactory.createReader(readerName + i,
                        readerGroupName,
                        new JavaSerializer<Long>(),
                        ReaderConfig.builder().build());
                readerList.add(reader);
            }

            // start writing asynchronously
            List<CompletableFuture<Void>> writerFutureList = writerList.stream().map(writer ->
                    startWriting(eventData, writer, stopWriteFlag, eventWriteCount)).collect(Collectors.toList());

            //start reading asynchronously
            List<CompletableFuture<Void>> readerFutureList = readerList.stream().map(reader ->
                    startReading(eventsReadFromPravega, eventWriteCount, eventReadCount, stopReadFlag, reader))
                    .collect(Collectors.toList());

            //run the failover test before triggering scaling
            performFailoverTest();

            //increase the number of writers to trigger scale
            List<EventStreamWriter<Long>> writerList1 = new ArrayList<>(ADD_NUM_WRITERS);
            log.info("Writers writing in the scope {}", scope);
            for (int i = 0; i < ADD_NUM_WRITERS; i++) {
                log.info("Starting writer{}", i);
                final EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(STREAM_NAME,
                        new JavaSerializer<Long>(),
                        EventWriterConfig.builder().retryAttempts(10).build());
                writerList1.add(writer1);
            }

            //start writing asynchronoulsy with newly created writers
            List<CompletableFuture<Void>> writerFutureList1 = writerList1.stream().map(writer ->
                    startWriting(eventData, writer, stopWriteFlag, eventWriteCount)).collect(Collectors.toList());

            //run the failover test while scaling
            performFailoverTest();

            //wait for scaling
            waitForScaling();

            //run the failover test after scaling
            performFailoverTest();

            //set the stop write flag to true
            log.info("Stop write flag status {}", stopWriteFlag);
            stopWriteFlag.set(true);

            //wait for writers completion
            log.info("Wait for writers execution to complete");
            FutureHelpers.allOf(writerFutureList).get();

            //wait for newly added writers completion
            FutureHelpers.allOf(writerFutureList1).get();

            //set the stop read flag to true
            log.info("Stop read flag status {}", stopReadFlag);
            stopReadFlag.set(true);

            //wait for readers completion
            log.info("Wait for readers execution to complete");
            FutureHelpers.allOf(readerFutureList).get();

            log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                    "Count: {}", eventWriteCount.get(), eventsReadFromPravega.size());
            assertEquals(eventWriteCount.get(), eventsReadFromPravega.size());
            assertEquals(eventWriteCount.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.

            //close all the writers
            log.info("Closing writers");
            writerList.forEach(writer -> writer.close());
            //close all readers
            log.info("Closing readers");
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
        log.info("Test {} succeeds ", "ReadWriteAndScaleWithFailover");

        }

        private void waitForScaling() {

            CompletableFuture<Void> testResult = Retry.withExpBackoff(10, 10, 40, ofSeconds(10).toMillis())
                    .retryingOn(AbstractScaleTests.ScaleOperationNotDoneException.class)
                    .throwingOn(RuntimeException.class)
                    .run(() -> controller.getCurrentSegments(scope, STREAM_NAME)
                            .thenAccept(x -> {
                                int currentNumOfSegments = x.getSegments().size();
                                if (currentNumOfSegments == 2) {
                                    log.info("The current number of segments is equal to 2, ScaleOperation did not happen");
                                    //Scaling operation did not happen, retry operation.
                                    throw new AbstractScaleTests.ScaleOperationNotDoneException();
                                } else if (currentNumOfSegments > 2) {
                                    //scale operation successful.
                                    log.info("Current Number of segments is {}", currentNumOfSegments);
                                    stopWriteFlag.set(true);
                                } else {
                                    Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
                                }
                            }));
        }

    private void performFailoverTest() throws InterruptedException {

        long currentWriteCount1;
        long currentReadCount1;

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Read count: {}, write count: {} without any failover", currentReadCount1, currentWriteCount1);

        int sleepTime;

        //check reads and writes after some random time
        sleepTime = new Random().nextInt(50000) + 3000;
        log.info("Sleeping for {} ", sleepTime);
        Thread.sleep(sleepTime);

        long currentWriteCount2;
        long currentReadCount2;

        currentWriteCount2 = eventWriteCount.get();
        currentReadCount2 = eventReadCount.get();

        log.info("Read count: {}, write count: {} without any failover after sleep before scaling", currentReadCount2, currentWriteCount2);

        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down SSS instances to 2
        segmentStoreInstance.scaleService(2, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(ZK_DEFAULT_SESSION_TIMEOUT);
        log.info("Scaling down SSS instances from 3 to 2");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Read count: {}, write count: {} after SSS failover after sleep", currentReadCount1, currentWriteCount1);

        //ensure writes are happening
        assertTrue(currentWriteCount1 > currentWriteCount2);
        //ensure reads are happening
        assertTrue(currentReadCount1 > currentReadCount2);

        //Scale down controller instances to 2
        controllerInstance.scaleService(2, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(ZK_DEFAULT_SESSION_TIMEOUT);
        log.info("Scaling down controller instances from 3 to 2");

        currentWriteCount2 = eventWriteCount.get();
        currentReadCount2 = eventReadCount.get();

        log.info("Read count: {}, write count: {} after controller failover after sleep", currentReadCount2, currentWriteCount2);

        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down SSS, controller to 1 instance each.
        segmentStoreInstance.scaleService(1, true);
        controllerInstance.scaleService(1, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(new Random().nextInt(50000) + ZK_DEFAULT_SESSION_TIMEOUT);
        log.info("Scaling down  to 1 controller, 1 SSS instance");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Stop write flag status: {}, stop read flag status: {} ", stopWriteFlag, stopReadFlag);
        log.info("Event data: {}, read count: {}", eventData.get(), eventReadCount.get());
        log.info("Read count: {}, write count: {} with SSS and controller failover after sleep", currentReadCount1, currentWriteCount1);
    }



    private CompletableFuture<Void> startWriting(final AtomicLong data, final EventStreamWriter<Long> writer,
                                                 final AtomicBoolean stopWriteFlag, final AtomicLong eventWriteCount) {
        return CompletableFuture.runAsync(() -> {
            while (!stopWriteFlag.get()) {
                try {
                    long value = data.incrementAndGet();
                    Thread.sleep(100);
                    log.debug("Event write count before write call {}", value);
                    writer.writeEvent(String.valueOf(value), value);
                    log.debug("Event write count before flush {}", value);
                    writer.flush();
                    eventWriteCount.getAndIncrement();
                    log.debug("Writing event {}", value);
                } catch (Throwable e) {
                    log.error("Test exception writing events: ", e);
                }
            }
        }, executorService);
    }

    private CompletableFuture<Void> startReading(final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
    AtomicLong readCount, final AtomicBoolean exitFlag, final EventStreamReader<Long> reader) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status {}", exitFlag.get());
            log.info("Read count {} and write count {}", readCount.get(), writeCount.get());
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                log.info("Entering read loop");
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    log.debug("Reading event {}", longEvent);
                    if (longEvent != null) {
                        //update if event read is not null.
                        readResult.add(longEvent);
                        readCount.incrementAndGet();
                        log.debug("Event read count {}", readCount);
                    } else {
                        log.debug("Read timeout");
                    }
                } catch (Throwable e) {
                    log.error("Test Exception while reading from the stream: ", e);
                }
            }
        }, executorService);
    }

}
