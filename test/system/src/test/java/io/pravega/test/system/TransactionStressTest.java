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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class TransactionStressTest {

    private static final String STREAM_NAME = "testTransactionStressStream";
    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;
    private static final int ZK_DEFAULT_SESSION_TIMEOUT = 60000;
    private static final int NUM_TRANSACTIONS = 50000;
    private static final long EVENTS_PER_TRANSACTION = 5;
    private ExecutorService executorService;
    private AtomicBoolean stopReadFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private AtomicLong transactionCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private URI controllerURIDirect = null;
    private Controller controller;
    private final String scope = "testTransactionStressScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testTrasactionStressScope" + new Random().nextInt(Integer.MAX_VALUE);
    private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
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
        executorService = Executors.newFixedThreadPool(NUM_READERS + NUM_WRITERS);
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect);
        //read and write count variables
        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
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

    @Test(timeout = 600000)
    public void transactionStressTest() throws Exception {

        //create a scope
        try (StreamManager streamManager = new StreamManagerImpl(controllerURIDirect)) {
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
            log.info("Creating {} writers", NUM_WRITERS);
            List<EventStreamWriter<Long>> writerList = new ArrayList<>(NUM_WRITERS);
            log.info("Writers writing in the scope {}", scope);
            for (int i = 0; i < NUM_WRITERS; i++) {
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
                log.info("Starting reader: {}, with id: {}", i, readerName+i);
                final EventStreamReader<Long> reader = clientFactory.createReader(readerName + i,
                        readerGroupName,
                        new JavaSerializer<Long>(),
                        ReaderConfig.builder().build());
                readerList.add(reader);
            }

            // start writing asynchronously
            List<CompletableFuture<Void>> writerFutureList = writerList.stream().map(writer ->
                    startWritingIntoTxn(eventData, writer, eventWriteCount)).collect(Collectors.toList());

            //start reading asynchronously
            List<CompletableFuture<Void>> readerFutureList = readerList.stream().map(reader ->
                    startReading(eventsReadFromPravega, eventWriteCount, eventReadCount, stopReadFlag, reader))
                                                                       .collect(Collectors.toList());

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


    private CompletableFuture<Void> startReading(final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
    AtomicLong readCount, final AtomicBoolean exitFlag, final EventStreamReader<Long> reader) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status {}", exitFlag);
            log.info("Read count {} and write count {}", readCount.get(), writeCount);
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
                    log.warn("Test Exception while reading from the stream: ", e);
                }
            }
        }, executorService);
    }

    private CompletableFuture<Void> startWritingIntoTxn(final AtomicLong data, EventStreamWriter<Long> writer,
                                                        final AtomicLong eventWriteCount) {
        return CompletableFuture.runAsync(() -> {
            while (this.transactionCount.incrementAndGet() < NUM_TRANSACTIONS) {
                Transaction<Long> transaction = null;
                try {
                    transaction = retry
                            .retryingOn(TxnCreationFailedException.class)
                            .throwingOn(RuntimeException.class)
                            .run(() -> createTransaction(writer));

                    //each transaction has EVENTS_PER_TRANSACTION events
                    for (int j = 0; j < EVENTS_PER_TRANSACTION; j++) {
                        long value = data.incrementAndGet();
                        transaction.writeEvent(String.valueOf(value), value);
                        log.debug("Writing event: {} into transaction: {}", value, transaction);
                    }
                    //commit Txn
                    transaction.commit();

                    //wait for transaction to get committed
                    checkTxnStatus(transaction, eventWriteCount).get();
                } catch (Throwable e) {
                    log.warn("Exception while writing events in the transaction: {}. Transaction with id: {}  failed", e, transaction.getTxnId());
                }
            }
        }, executorService);
    }

    private CompletableFuture<Void> checkTxnStatus(Transaction<Long> txn,
                                                   final AtomicLong eventWriteCount) {
        return CompletableFuture.runAsync(() -> {
            Transaction.Status status = retry.retryingOn(TxnNotCompleteException.class)
                                             .throwingOn(RuntimeException.class).run(() -> txn.checkStatus());
            log.debug("Transaction: {} status: {}", txn, txn.checkStatus());
            if (status.equals(Transaction.Status.COMMITTED)) {
                eventWriteCount.addAndGet(EVENTS_PER_TRANSACTION);
                log.info("Event write count: {}", eventWriteCount.get());
            } else if (status.equals(Transaction.Status.ABORTED)) {
                log.debug("Transaction with id: {} aborted", txn.getTxnId());
            } else {
                throw new TxnNotCompleteException();
            }
        });
    }

    private Transaction<Long> createTransaction(EventStreamWriter<Long> writer) {
        Transaction<Long> txn = null;
        try {
            //Default max scale grace period is 30000
            txn = writer.beginTxn(5000, 3600000, 29000);
            log.info("Transaction created with id:{} ", txn.getTxnId());
        } catch (RuntimeException ex) {
            log.info("Exception encountered while trying to begin Transaction ", ex.getCause());
            if (ex instanceof io.grpc.StatusRuntimeException) {
                log.warn("Cause for failure is {} and we need to retry", ex.getClass().getName());
                throw new TxnCreationFailedException(); // we can retry on this exception.
            } else {
                throw ex;
            }
        }
        return txn;
    }

    private class TxnCreationFailedException extends RuntimeException {
    }

    private class TxnNotCompleteException extends RuntimeException {
    }
}
