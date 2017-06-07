/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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

@Slf4j
public class ReadWriteTest {

    private static final String STREAM_NAME = "testMultiReaderWriterStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static final int NUM_WRITERS = 20;
    private static final int NUM_READERS = 20;
    private static final long TOTAL_NUM_EVENTS = 20000;
    private static final int NUM_EVENTS_BY_WRITER = 1000;
    private AtomicBoolean stopReadFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private int i;

    @Before
    public void setup() throws Exception {

        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        this.server = new PravegaConnectionListener(false, servicePort, store);
        this.server.startListening();

        // 3. Start  controller service
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

    @Test(timeout = 300000)
    public void multiReaderWriterTest() throws Exception {
        for (i = 0; i < 2; i++) {
            readWriteTest(i);
        }
        log.info(" all tests are successful");
    }

    private void readWriteTest(int i) throws InterruptedException, ExecutionException {

        log.info("invoking read write test {} time", i);
        String scope = "testMultiReaderWriterScope" + i;
        String readerGroupName = "testMultiReaderWriterReaderGroup" + i;
        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(20);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope)
                .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();

        //create a scope
        Boolean createScopeStatus = controller.createScope(scope).get();
        log.info("Create scope status {}", createScopeStatus);
        //create a stream
        Boolean createStreamStatus = controller.createStream(config).get();
        log.info("Create stream status {}", createStreamStatus);

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
        //start writing events to the stream with 5 writers
        log.info("creating {} writers", NUM_WRITERS);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (i = 0; i < NUM_WRITERS; i++) {
            log.info("starting writer{}", i);
            writerList.add(startNewWriter(eventData, clientFactory));
        }

        //create a reader group
        log.info("Creating Reader group : {}", readerGroupName);

        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(STREAM_NAME));

        log.info(" reader group name {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
        log.info(" reader group scope {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());
        //create 5 readers
        log.info("creating {} readers", NUM_READERS);
        List<CompletableFuture<Void>> readerList = new ArrayList<>();
        String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
        //start reading events
        for (i = 0; i < NUM_READERS; i++) {
            log.info("starting reader{}", i);
            readerList.add(startReader(readerName + i, clientFactory, readerGroupName,
                    eventsReadFromPravega, eventData, eventReadCount, stopReadFlag));
        }

        log.info("online readers {}", readerGroupManager.getReaderGroup(readerGroupName).getOnlineReaders());

        //wait for writers completion
        FutureHelpers.allOf(writerList);

        // wait for reads = writes
        while (TOTAL_NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }

        stopReadFlag.set(true);

        //wait for readers completion
        FutureHelpers.allOf(readerList);

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(TOTAL_NUM_EVENTS, eventsReadFromPravega.size());
        assertEquals(TOTAL_NUM_EVENTS, new TreeSet<>(eventsReadFromPravega).size()); //check unique events.
        //stop reading when no. of reads= no. of writes
        log.info("read write test succeed");

        //seal all streams
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, STREAM_NAME);
        log.info("sealing stream {}", STREAM_NAME);
        assertTrue(sealStreamStatus.get());

        CompletableFuture<Boolean> sealStreamStatus1 = controller.sealStream(scope, "_RG" + readerGroupName);
        log.info("sealing stream {}", "_RG" + readerGroupName);
        assertTrue(sealStreamStatus1.get());

        //delete all streams
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, STREAM_NAME);
        log.info("deleting stream {}", STREAM_NAME);
        assertTrue(deleteStreamStatus.get());

        CompletableFuture<Boolean> deleteStreamStatus1 = controller.deleteStream(scope, "_RG" + readerGroupName);
        log.info("deleting stream {}", "_RG" + readerGroupName);
        assertTrue(deleteStreamStatus1.get());

        //delete scope
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
    }

    private CompletableFuture<Void> startNewWriter(final AtomicLong data,
                                                   final ClientFactory clientFactory) {
        return CompletableFuture.runAsync(() -> {
            final EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            for (int i = 0; i < NUM_EVENTS_BY_WRITER; i++) {
                try {
                    long value = data.incrementAndGet();
                    log.info("writing event {}", value);
                    writer.writeEvent(String.valueOf(value), value);
                    writer.flush();
                } catch (Throwable e) {
                    log.warn("test exception writing events: {}", e);
                    break;
                }
            }
            log.info("closing writer {}", writer);
            writer.close();

        });
    }

    private CompletableFuture<Void> startReader(final String id, final ClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
                                                AtomicLong readCount, final AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            log.info("exit flag before reading {}", exitFlag.get());
            log.info("readcount before reading {}", readCount.get());
            log.info("write count before reading {}", writeCount.get());

            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    log.info("reading event {}", longEvent);
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
            log.info("closing reader {}", reader);
            reader.close();
        });
    }
}
