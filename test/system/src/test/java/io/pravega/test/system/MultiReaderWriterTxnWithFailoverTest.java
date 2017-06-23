/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiReaderWriterTxnWithFailoverTest  extends  MultiReaderWriterWithFailOverTest {
    private static final String STREAM_NAME = "testMultiReaderWriterTxnStream";
    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;
    private ExecutorService executorService;
    private AtomicBoolean stopReadFlag;
    private AtomicBoolean stopWriteFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private URI controllerURIDirect = null;

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
        Service conService = new PravegaControllerService("controllerFailover2", zkUri);
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
        Service segService = new PravegaSegmentStoreService("segmentstoreFailover2", zkUri, controllerURI);
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
        controllerInstance = new PravegaControllerService("controllerFailover2", zkUri);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = new PravegaSegmentStoreService("segmentstoreFailover2", zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());

        executorService = Executors.newFixedThreadPool(NUM_READERS + NUM_WRITERS);
    }

    @After
    public void tearDown() {
        if (controllerInstance != null && controllerInstance.isRunning()) {
            controllerInstance.stop();
        }
        if (segmentStoreInstance != null && segmentStoreInstance.isRunning()) {
            segmentStoreInstance.stop();
        }
        executorService.shutdownNow();
    }

    @Test(timeout = 600000)
    public void multiReaderWriterWithFailOverTest() throws Exception {

        String scope = "testMultiReaderWriterTxnScope" + new Random().nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testMultiReaderWriterTxnReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope)
                .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
        //get Controller Uri
        URI controllerUri = controllerURIDirect;
        Controller controller = new ControllerImpl(controllerUri);

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
        stopWriteFlag = new AtomicBoolean(false);
        eventData = new AtomicLong(0); //data used by each of the writers.
        eventReadCount = new AtomicLong(0); // used by readers to maintain a count of events.
        eventWriteCount = new AtomicLong(0);

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
                        EventWriterConfig.builder().retryAttempts(10).build());
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

            // start writing asynchronously
            List<CompletableFuture<Void>> writerFutureList = new ArrayList<>();
            writerList.forEach(writer -> {
                CompletableFuture<Void> writerFuture = startWritingIntoTxn(eventData, writer, stopWriteFlag, eventWriteCount);
                writerFutureList.add(writerFuture);
            });

            //start reading asynchronously
            List<CompletableFuture<Void>> readerFutureList = new ArrayList<>();
            readerList.forEach(reader -> {
                CompletableFuture<Void> readerFuture = startReading(eventsReadFromPravega, eventWriteCount, eventReadCount, stopReadFlag, reader);
                readerFutureList.add(readerFuture);
            });

            //perform the scaling operations
            performFailoverTest();

            //set the stop write flag to true
            log.info("Stop write flag status {}", stopWriteFlag);
            stopWriteFlag.set(true);

            //wait for writers completion
            log.info("Wait for writers execution to complete");
            FutureHelpers.allOf(writerFutureList).get();

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
        log.info("Test {} succeeds ", "MultiReaderWriterTxnWithFailOver");
    }

    private void performFailoverTest() throws InterruptedException {

        long currentWriteCount1;
        long currentReadCount1;

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Read count without any failover {}", currentReadCount1);
        log.info("Write count without any failover {}", currentWriteCount1);

        int sleepTime;

        //check reads and writes after some random time
        sleepTime = new Random().nextInt(50000) + 3000;
        log.info("Sleeping for {} ", sleepTime);
        Thread.sleep(sleepTime);

        long currentWriteCount2;
        long currentReadCount2;

        currentWriteCount2 = eventWriteCount.get();
        currentReadCount2 = eventReadCount.get();

        log.info("Read count without any failover after sleep before scaling  {}", currentReadCount2);
        log.info("Write count without any failover after sleep before scaling {}", currentWriteCount2);

        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down SSS instances to 2
        segmentStoreInstance.scaleService(2, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(60000);
        log.info("Scaling down SSS instances from 3 to 2");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Read count after SSS failover after sleep  {}", currentReadCount1);
        log.info("Write count after SSS failover after sleep {}", currentWriteCount1);

        //ensure writes are happening
        assertTrue(currentWriteCount1 > currentWriteCount2);
        //ensure reads are happening
        assertTrue(currentReadCount1 > currentReadCount2);

        //Scale down controller instances to 2
        controllerInstance.scaleService(2, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(60000);
        log.info("Scaling down controller instances from 3 to 2");

        currentWriteCount2 = eventWriteCount.get();
        currentReadCount2 = eventReadCount.get();

        log.info("Read count after controller failover after sleep  {}", currentReadCount2);
        log.info("Write count after controller failover after sleep  {}", currentWriteCount2);

        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down SSS, controller to 1 instance each.
        segmentStoreInstance.scaleService(1, true);
        controllerInstance.scaleService(1, true);
        //zookeeper will take about 60 seconds to detect that the node has gone down
        Thread.sleep(new Random().nextInt(50000) + 60000);
        log.info("Scaling down  to 1 controller, 1 SSS instance");

        currentWriteCount1 = eventWriteCount.get();
        currentReadCount1 = eventReadCount.get();

        log.info("Stop write flag status {}", stopWriteFlag);
        log.info("Stop read flag status {}", stopReadFlag);
        log.info("Event data {}", eventData.get());
        log.info("Read count {}", eventReadCount.get());
        log.info("Read count with SSS and controller failover after sleep {}", currentReadCount1);
        log.info("Write count with SSS and controller failover after sleep {} ", currentWriteCount1);
    }

    private CompletableFuture<Void> startWritingIntoTxn(final AtomicLong data, final EventStreamWriter<Long> writer,
                                                 final AtomicBoolean stopWriteFlag, final AtomicLong eventWriteCount) {
        return CompletableFuture.runAsync(() -> {
            while (!stopWriteFlag.get()) {
                Transaction<Long> transaction = null;
                    try {
                        transaction = Retry.withExpBackoff(10, 10, 20, ofSeconds(1).toMillis())
                                .retryingOn(MultiReaderWriterTxnWithFailoverTest.TxnCreationFailedException.class)
                                .throwingOn(RuntimeException.class)
                                .run(() -> createTransaction(writer, stopWriteFlag));

                        //each transaction has 50 events
                        for (int j = 0; j < 50; j++) {
                            long value = data.incrementAndGet();
                            Thread.sleep(100);
                            transaction.writeEvent(String.valueOf(value), value);
                            log.debug("Writing event {} into transaction {}", value, transaction);
                        }
                        //commit Txn
                        transaction.commit();
                        eventWriteCount.getAndAdd(50);
                        log.debug("Transaction {} committed", transaction);
                        log.debug("Transaction status {}", transaction.checkStatus());
                    } catch (Throwable e) {
                        log.warn("Exception while writing events in the transaction : {}", e);
                        log.debug("transaction with id {}  failed", transaction.getTxnId());
                    }
            }
        }, executorService);
    }

    private Transaction<Long> createTransaction(EventStreamWriter<Long> writer, final AtomicBoolean exitFlag) {
        Transaction<Long> txn = null;
        try {
            //Default max scale grace period is 30000
            txn = writer.beginTxn(5000, 3600000, 29000);
            log.info("Transaction created with id:{} ", txn.getTxnId());
        } catch (RuntimeException ex) {
            log.info("Exception encountered while trying to begin Transaction ", ex.getCause());
            final Class<? extends Throwable> exceptionClass = ex.getCause().getClass();
            if (exceptionClass.equals(io.grpc.StatusRuntimeException.class) && !exitFlag.get())  {
                //Exit flag is true no need to retry.
                log.warn("Cause for failure is {} and we need to retry", exceptionClass.getName());
                throw new MultiReaderWriterTxnWithFailoverTest.TxnCreationFailedException(); // we can retry on this exception.
            } else {
                throw ex;
            }
        }
        return txn;
    }

    private class TxnCreationFailedException extends RuntimeException {
    }
}
