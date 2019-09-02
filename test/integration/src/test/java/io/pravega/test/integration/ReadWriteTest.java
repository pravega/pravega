/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ReadWriteTest {

    private static final String STREAM_NAME = "testMultiReaderWriterStream" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int NUM_WRITERS = 20;
    private static final int NUM_READERS = 20;
    private static final long TOTAL_NUM_EVENTS = 20000;
    private static final int NUM_EVENTS_BY_WRITER = 1000;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private AtomicBoolean stopReadFlag;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;

    @Before
    public void setup() throws Exception {

        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        this.server.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }
        if (this.server != null) {
            this.server.close();
            this.server = null;
        }
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }
    }

    @Test(timeout = 60000)
    public void readWriteTest() throws InterruptedException, ExecutionException {

        String scope = "testMultiReaderWriterScope";
        String readerGroupName = "testMultiReaderWriterReaderGroup";
        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(20);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.
        stopReadFlag = new AtomicBoolean(false);

        try (ConnectionFactory cf = new ConnectionFactoryImpl(ClientConfig.builder().build());
             StreamManager streamManager = new StreamManagerImpl(controller, cf)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create scope status {}", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        try (ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory)) {

            //start writing events to the stream
            log.info("Creating {} writers", NUM_WRITERS);
            List<CompletableFuture<Void>> writerList = new ArrayList<>();
            for (int i = 0; i < NUM_WRITERS; i++) {
                log.info("Starting writer{}", i);
                writerList.add(startNewWriter(eventData, clientFactory));
            }

            //create a reader group
            log.info("Creating Reader group : {}", readerGroupName);

            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, STREAM_NAME)).build());
            log.info("Reader group name {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
            log.info("Reader group scope {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());

            //create readers
            log.info("Creating {} readers", NUM_READERS);
            List<CompletableFuture<Void>> readerList = new ArrayList<>();
            String readerName = "reader" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
            //start reading events
            for (int i = 0; i < NUM_READERS; i++) {
                log.info("Starting reader{}", i);
                readerList.add(startNewReader(readerName + i, clientFactory, readerGroupName,
                        eventsReadFromPravega, eventData, eventReadCount, stopReadFlag));
            }

            //wait for writers completion
            Futures.allOf(writerList).get();

            //set stop read flag to true
            stopReadFlag.set(true);

            //wait for readers completion
            Futures.allOf(readerList).get();

            //delete readergroup
            log.info("Deleting readergroup {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(TOTAL_NUM_EVENTS, eventsReadFromPravega.size());
        assertEquals(TOTAL_NUM_EVENTS, new TreeSet<>(eventsReadFromPravega).size()); //check unique events.
        //seal the stream
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
        log.info("Sealing stream {}", STREAM_NAME);
        assertTrue(sealStreamStatus.get());
        //delete the stream
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
        log.info("Deleting stream {}", STREAM_NAME);
        assertTrue(deleteStreamStatus.get());
        //delete the  scope
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
        log.info("Read write test succeeds");
    }

    private CompletableFuture<Void> startNewWriter(final AtomicLong data,
                                                   final EventStreamClientFactory clientFactory) {
        return CompletableFuture.runAsync(() -> {
            final EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            for (int i = 0; i < NUM_EVENTS_BY_WRITER; i++) {
                try {
                    long value = data.incrementAndGet();
                    log.info("Writing event {}", value);
                    writer.writeEvent(String.valueOf(value), value);
                    writer.flush();
                } catch (Throwable e) {
                    log.warn("Test exception writing events: {}", e);
                    break;
                }
            }
            log.info("Closing writer {}", writer);
            writer.close();

        });
    }

    private CompletableFuture<Void> startNewReader(final String id, final EventStreamClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
                                                   AtomicLong readCount, final  AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                    log.info("Reading event {}", longEvent);
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
            log.info("Closing reader {}", reader);
            reader.close();
        });
    }
}
