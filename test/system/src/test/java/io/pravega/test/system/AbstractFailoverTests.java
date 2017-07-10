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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
abstract class AbstractFailoverTests {

    static final String STREAM = "testReadWriteAndAutoScaleStream";
    static final String STREAM_NAME = "testReadWriteAndScaleStream";
    static final int ADD_NUM_WRITERS = 6;
    static final int ZK_DEFAULT_SESSION_TIMEOUT = 30000;
    final String readerName = "reader";
    List<CompletableFuture<Void>> writerFutureList;
    List<CompletableFuture<Void>> newWritersFutureList;
    List<CompletableFuture<Void>> readerFutureList;
    List<EventStreamWriter<Long>> newlyAddedWriterList;
    List<EventStreamWriter<Long>> writerList;
    List<EventStreamReader<Long>> readerList;
    Service controllerInstance = null;
    Service segmentStoreInstance = null;
    URI controllerURIDirect = null;
    Throwable writeException = null;
    ExecutorService executorService;
    Controller controller;
    TestState testState;
    Throwable getWriteException = null;
    Throwable getReadException = null;

    static class TestState {
        //read and write count variables
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicLong eventReadCount = new AtomicLong(0); // used by readers to maintain a count of events.
        final AtomicLong eventWriteCount = new AtomicLong(0); // used by writers to maintain a count of events.
        final AtomicLong eventData = new AtomicLong(0); //data used by each of the writers.
        final ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();
    }

    void performFailoverTest() throws InterruptedException {

        log.info("Test with 3 controller, SSS instances running and without a failover scenario");
        long currentWriteCount1 = testState.eventWriteCount.get();
        long currentReadCount1 = testState.eventReadCount.get();
        log.info("Read count: {}, write count: {} without any failover", currentReadCount1, currentWriteCount1);

        //check reads and writes after some random time
        int sleepTime = new Random().nextInt(50000) + 3000;
        log.info("Sleeping for {} ", sleepTime);
        Thread.sleep(sleepTime);

        long currentWriteCount2 = testState.eventWriteCount.get();
        long currentReadCount2 = testState.eventReadCount.get();
        log.info("Read count: {}, write count: {} without any failover after sleep before scaling", currentReadCount2, currentWriteCount2);
        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down SSS instances to 2
        segmentStoreInstance.scaleService(2, true);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Thread.sleep(ZK_DEFAULT_SESSION_TIMEOUT);
        log.info("Scaling down Segment Store instances from 3 to 2");

        currentWriteCount1 = testState.eventWriteCount.get();
        currentReadCount1 = testState.eventReadCount.get();
        log.info("Read count: {}, write count: {} after Segment Store  failover after sleep", currentReadCount1, currentWriteCount1);
        //ensure writes are happening
        assertTrue(currentWriteCount1 > currentWriteCount2);
        //ensure reads are happening
        assertTrue(currentReadCount1 > currentReadCount2);

        //Scale down controller instances to 2
        controllerInstance.scaleService(2, true);
        //zookeeper will take about 30 seconds to detect that the node has gone down
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
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Thread.sleep(ZK_DEFAULT_SESSION_TIMEOUT);
        log.info("Scaling down  to 1 controller, 1 Segment Store  instance");

        currentWriteCount1 = testState.eventWriteCount.get();
        currentReadCount1 = testState.eventReadCount.get();
        log.info("Stop write flag status: {}, stop read flag status: {} ", testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with Segment Store  and controller failover after sleep", currentReadCount1, currentWriteCount1);
    }

    CompletableFuture<Void> startWriting(final EventStreamWriter<Long> writer) {
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
                } catch (InterruptedException e) {
                    log.error("Error in sleep: ", e);
                    getWriteException = e;
                } catch (ConnectionClosedException e) {
                    log.warn("Test exception in writing events: ", e);
                    continue;
                } catch (Throwable e) {
                    log.error("Test exception in writing events: ", e);
                   getWriteException = e;
                }
            }
        }, executorService);
    }

    CompletableFuture<Void> startReading(final EventStreamReader<Long> reader) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status: {}, Read count: {}, Write count: {}", testState.stopReadFlag.get(),
                    testState.eventReadCount.get(), testState.eventWriteCount.get());
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
                } catch (ConnectionClosedException e) {
                    log.warn("Test exception in reading events: ", e);
                    continue;
                } catch (ReinitializationRequiredException e) {
                    log.error("Test exception in reading events: ", e);
                    getReadException = e;
                }
            }
        }, executorService);
    }

    void cleanUp(String scope, String stream) throws InterruptedException, ExecutionException {
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, stream);
        log.info("Sealing stream {}", STREAM_NAME);
        assertTrue(sealStreamStatus.get());
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, stream);
        log.info("Deleting stream {}", STREAM_NAME);
        assertTrue(deleteStreamStatus.get());
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
    }

    void createScopeAndStream(String scope, String stream, StreamConfiguration config, URI uri) {

        try (StreamManager streamManager = new StreamManagerImpl(uri)) {
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Creating scope with scope name {}", scope);
            log.debug("Create scope status {}", createScopeStatus);
            Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
            log.debug("Create stream status {}", createStreamStatus);
        }
    }

    void createReadersAndWriters(ClientFactory clientFactory, String readerGroupName, String scope,
                                 ReaderGroupManager readerGroupManager, String stream, final int writers, final int readers) {
        log.info("Client factory details {}", clientFactory.toString());
        log.info("Creating {} writers", writers);
        writerList = new ArrayList<>(writers);
        log.info("Writers writing in the scope {}", scope);
        EventStreamWriter<Long> tmpWriter;
        for (int i = 0; i < writers; i++) {
            log.info("Starting writer{}", i);
            tmpWriter = clientFactory.createEventWriter(stream,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().retryAttempts(10).build());
            writerList.add(tmpWriter);
        }

        log.info("Creating Reader group: {}, with readergroup manager using scope: {}", readerGroupName, scope);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(stream));
        log.info("Reader group name: {}, Reader group scope: {}, Online readers: {}",
                readerGroupManager.getReaderGroup(readerGroupName).getGroupName(), readerGroupManager
                        .getReaderGroup(readerGroupName).getScope(), readerGroupManager
                        .getReaderGroup(readerGroupName).getOnlineReaders());
        log.info("Creating {} readers", readers);
        readerList = new ArrayList<>(readers);
        log.info("Scope that is seen by readers {}", scope);
        for (int i = 0; i < readers; i++) {
            log.info("Starting reader: {}, with id: {}", i, readerName + i);
            final EventStreamReader<Long> reader = clientFactory.createReader(readerName + i,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            readerList.add(reader);
        }

        // start writing asynchronously
        writerFutureList = writerList.stream().map(writer ->
                startWriting(writer)).collect(Collectors.toList());

        //start reading asynchronously
        readerFutureList = readerList.stream().map(reader ->
                startReading(reader))
                .collect(Collectors.toList());

    }

    void stopReadersAndWriters(ReaderGroupManager readerGroupManager, String readerGroupName) throws InterruptedException, ExecutionException {

        log.info("Stop write flag status {}", testState.stopWriteFlag);
        testState.stopWriteFlag.set(true);

        log.info("Wait for writers execution to complete");
        FutureHelpers.allOf(writerFutureList).get();
        FutureHelpers.allOf(newWritersFutureList).get();
        if (getWriteException != null) {
            Assert.fail("Unable to write events. Test failure");
        }

        log.info("Stop read flag status {}", testState.stopReadFlag);
        testState.stopReadFlag.set(true);

        log.info("Wait for readers execution to complete");
        FutureHelpers.allOf(readerFutureList).get();
        if (getReadException != null) {
            Assert.fail("Unable to read events. Test failure");
        }

        log.info("All writers have stopped. Setting stopReadFlag. Event Written Count:{}, Event Read " +
                "Count: {}", testState.eventWriteCount.get(), testState.eventsReadFromPravega.size());
        assertEquals(testState.eventWriteCount.get(), testState.eventsReadFromPravega.size());
        assertEquals(testState.eventWriteCount.get(), new TreeSet<>(testState.eventsReadFromPravega).size()); //check unique events.

        log.info("Closing writers");
        writerList.forEach(writer -> writer.close());
        newlyAddedWriterList.forEach(writer -> writer.close());
        log.info("Closing readers");
        readerList.forEach(reader -> reader.close());
        log.info("Deleting readergroup {}", readerGroupName);
        readerGroupManager.deleteReaderGroup(readerGroupName);
    }


    void addNewWriters(String scope, ClientFactory clientFactory) {
        //increase the number of writers to trigger scale
        newlyAddedWriterList = new ArrayList<>(ADD_NUM_WRITERS);
        log.info("Writers writing in the scope {}", scope);
        EventStreamWriter<Long> newTmpWriter;
        for (int i = 0; i < ADD_NUM_WRITERS; i++) {
            log.info("Starting writer{}", i);
            newTmpWriter = clientFactory.createEventWriter(STREAM,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().retryAttempts(10).build());
            newlyAddedWriterList.add(newTmpWriter);
        }

        //start writing asynchronoulsy with newly created writers
        newWritersFutureList = newlyAddedWriterList.stream().map(writer ->
                startWriting(writer)).collect(Collectors.toList());
    }

    static class ScaleOperationNotDoneException extends RuntimeException {
    }

}
