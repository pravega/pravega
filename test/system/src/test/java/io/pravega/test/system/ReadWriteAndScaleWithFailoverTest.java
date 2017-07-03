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
import io.pravega.shared.protocol.netty.ConnectionFailedException;
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
import org.junit.Test;
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
import java.util.concurrent.ExecutionException;
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
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private URI controllerURIDirect = null;
    private ExecutorService executorService;
    private Controller controller;
    private TestState testState;
    private final String scope = "testReadWriteAndScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testReadWriteAndScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private ScalingPolicy scalingPolicy =  ScalingPolicy.byEventRate(1, 2, NUM_READERS);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
    private final String readerName = "reader";
    private final Retry.RetryWithBackoff retry = Retry.withExpBackoff(10, 10, 20, ofSeconds(1).toMillis());


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

    private static class TestState {
        
            //read and write count variables
            final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
            final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
            final AtomicLong eventReadCount = new AtomicLong(0); // used by readers to maintain a count of events.
            final AtomicLong eventWriteCount = new AtomicLong(0); // used by writers to maintain a count of events.
            final AtomicLong eventData = new AtomicLong(0); //data used by each of the writers.
            final ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        
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
        
        testState = new TestState();
    }

    @After
    public void tearDown() {
        controllerInstance.scaleService(1, true);
        segmentStoreInstance.scaleService(1, true);
        executorService.shutdownNow();
        testState.eventsReadFromPravega.clear();
    }

    @Test(timeout = 600000)
    public void readWriteAndScaleWithFailoverTest() throws Exception {

        try (StreamManager streamManager = new StreamManagerImpl(controllerURIDirect)) {
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Creating scope with scope name {}", scope);
            log.debug("Create scope status {}", createScopeStatus);
            Boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.debug("Create stream status {}", createStreamStatus);
        }

        log.info("Scope passed to client factory {}", scope);
        try (ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect)) {
            log.info("Client factory details {}", clientFactory.toString());
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

            log.info("Creating Reader group: {}, with readergroup manager using scope: {}", readerGroupName, scope);
            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                    Collections.singleton(STREAM_NAME));
            log.info("Reader group name: {}, Reader group scope: {}, Online readers: {}",
                    readerGroupManager.getReaderGroup(readerGroupName).getGroupName(), readerGroupManager
                            .getReaderGroup(readerGroupName).getScope(), readerGroupManager
                            .getReaderGroup(readerGroupName).getOnlineReaders());
            log.info("Creating {} readers", NUM_READERS);
            List<EventStreamReader<Long>> readerList = new ArrayList<>(NUM_READERS);
            log.info("Scope that is seen by readers {}", scope);
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
                    startWriting(writer)).collect(Collectors.toList());

            //start reading asynchronously
            List<CompletableFuture<Void>> readerFutureList = readerList.stream().map(reader ->
                    startReading(reader))
                    .collect(Collectors.toList());

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
                    startWriting(writer)).collect(Collectors.toList());

            //run the failover test while scaling
            performFailoverTest();

            //wait for scaling
            waitForScaling();

            //bring the instances back to 3 before performing failover
            controllerInstance.scaleService(3, true);
            segmentStoreInstance.scaleService(3, true);
            Thread.sleep(ZK_DEFAULT_SESSION_TIMEOUT);

            //run the failover test after scaling
            performFailoverTest();

            log.info("Stop write flag status {}", testState.stopWriteFlag);
            testState.stopWriteFlag.set(true);

            log.info("Wait for writers execution to complete");
            FutureHelpers.allOf(writerFutureList).get();

            FutureHelpers.allOf(writerFutureList1).get();

            log.info("Stop read flag status {}", testState.stopReadFlag);
            testState.stopReadFlag.set(true);

            log.info("Wait for readers execution to complete");
            FutureHelpers.allOf(readerFutureList).get();

            log.info("All writers have stopped. Setting stopReadFlag. Event Written Count:{}, Event Read " +
                    "Count: {}", testState.eventWriteCount.get(), testState.eventsReadFromPravega.size());
            assertEquals(testState.eventWriteCount.get(), testState.eventsReadFromPravega.size());
            assertEquals(testState.eventWriteCount.get(), new TreeSet<>(testState.eventsReadFromPravega).size()); //check unique events.

            log.info("Closing writers");
            writerList.forEach(writer -> writer.close());
            log.info("Closing readers");
            readerList.forEach(reader -> reader.close());
            log.info("Deleting readergroup {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
        log.info("Sealing stream {}", STREAM_NAME);
        assertTrue(sealStreamStatus.get());
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
        log.info("Deleting stream {}", STREAM_NAME);
        assertTrue(deleteStreamStatus.get());
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
        log.info("Test {} succeeds ", "ReadWriteAndScaleWithFailover");

        }

        private void waitForScaling() throws InterruptedException, ExecutionException {

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
                                } else {
                                    Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
                                }
                            }));
            testResult.get();
        }

    private void performFailoverTest() throws InterruptedException {

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");
        long currentWriteCount1 = testState.eventWriteCount.get();
        long currentReadCount1 = testState.eventReadCount.get();
        log.info("Read count: {}, write count: {} without any failover", currentReadCount1, currentWriteCount1);

        //check reads and writes after some random time
        int sleepTime = new Random().nextInt(50000) + 3000;
        log.info("Sleeping for {} ", sleepTime);
        Thread.sleep(sleepTime);

        long currentWriteCount2 = testState.eventWriteCount.get();
        long currentReadCount2 =  testState.eventReadCount.get();
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

        currentWriteCount1 = testState.eventWriteCount.get();
        currentReadCount1 = testState.eventReadCount.get();
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

        currentWriteCount2 = testState.eventWriteCount.get();
        currentReadCount2 = testState.eventReadCount.get();
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

        currentWriteCount1 = testState.eventWriteCount.get();
        currentReadCount1 = testState.eventReadCount.get();
        log.info("Stop write flag status: {}, stop read flag status: {} ", testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with SSS and controller failover after sleep", currentReadCount1, currentWriteCount1);
    }



    private CompletableFuture<Void> startWriting(final EventStreamWriter<Long> writer) {
        return CompletableFuture.runAsync(() -> {
            while (!testState.stopWriteFlag.get()) {
                try {
                    long value = testState.eventData.incrementAndGet();
                    Thread.sleep(100);
                    log.debug("Event write count before write call {}", value);
                    writer.writeEvent(String.valueOf(value), value);
                    log.debug("Event write count before flush {}", value);
                    writer.flush();
                    testState.eventWriteCount.getAndIncrement();
                    log.debug("Writing event {}", value);
                } catch (Throwable e ) {
                    if (e instanceof InterruptedException) {
                        log.error("error in sleep: ", e);
                        break;
                    }
                    if (e instanceof ConnectionFailedException) {
                        log.error("Test exception in writing events: ", e);
                        break;
                    }
                    log.error("Test exception in writing events: ", e);
                        break;
                    }
                }
        }, executorService);
    }

    private CompletableFuture<Void> startReading(final EventStreamReader<Long> reader) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status {}", testState.stopReadFlag.get());
            log.info("Read count {} and write count {}", testState.eventReadCount.get(), testState.eventWriteCount.get());
            while (!(testState.stopReadFlag.get() && testState.eventReadCount.get() == testState.eventWriteCount.get())) {
                log.info("Entering read loop");
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    log.debug("Reading event {}", longEvent);
                    if (longEvent != null) {
                        //update if event read is not null.
                        testState.eventsReadFromPravega.add(longEvent);
                        testState.eventReadCount.incrementAndGet();
                        log.debug("Event read count {}", testState.eventReadCount.get());
                    } else {
                        log.debug("Read timeout");
                    }
                } catch (Throwable e ) {
                    if (e instanceof ConnectionFailedException) {
                        log.error("Test exception in writing events: ", e);
                        continue;
                    } else {
                        log.error("Test exception in writing events: ", e);
                        break;
                    }
                }
            }
        }, executorService);
    }
}
