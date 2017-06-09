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
import io.pravega.client.stream.impl.JavaSerializer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiReaderWriterWithFailOverTest extends AbstractScaleTests {

    private static final String STREAM_NAME = "testMultiReaderWriterStream";
    private static final int NUM_WRITERS = 20;
    private static final int NUM_READERS = 20;
    private static final long NUM_EVENTS = 20000;
    private static final long NUM_EVENTS_BY_WRITER = 1000;
    private AtomicBoolean stopReadFlag;
    private AtomicLong eventData;
    private AtomicLong eventReadCount;
    private ConcurrentLinkedQueue<Long> eventsReadFromPravega;
    private Service controllerInstance = null;
    private Service segmentStoreInstance = null;
    private ReaderGroupManager readerGroupManager;
    private URI controllerURIDirect = null;
    private URI controllerURIDiscover = null;
    private ClientFactory clientFactory;

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

        //4.start 2 instances of pravega segmentstore
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        segService.scaleService(3, true);
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
        segmentStoreInstance = new PravegaSegmentStoreService("segmentstore", zkUri, conURIs.get(0));
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega segment store instance details: {}", segmentStoreInstance.getServiceDetails());
    }

    @Test(timeout = 600000)
    public void multiReaderWriterWithFailOverTest() throws Exception {

        String scope = "testMultiReaderWriterScope" + new Random().nextInt(Integer.MAX_VALUE);
        String readerGroupName = "testMultiReaderWriterReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(20);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope)
                .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();

        //get Controller Uri
        URI controllerUri = controllerURIDirect;
        Controller controller = getController(controllerUri);
        //create a scope
        StreamManager streamManager = new StreamManagerImpl(controllerUri);
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info(" creating scope with scope name {}", scope);
        log.debug("Create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = streamManager.createStream(scope, STREAM_NAME, config);
        log.debug("Create stream status {}", createStreamStatus);

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag = new AtomicBoolean(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.
        //get ClientFactory instance
        log.info("scope passed to client factory {}", scope);
        clientFactory = new ClientFactoryImpl(scope, controller);
        log.info(" client factory details {}", clientFactory.toString());

        //create writers
        log.info("creating {} writers", NUM_WRITERS);
        List<EventStreamWriter<Long>> writerList = new ArrayList<>();
        log.info("writers writing in the scope {}", scope);

        for (int i = 0; i < NUM_WRITERS; i++) {
            log.info("starting writer{}", i);
            final EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            writerList.add(writer);
        }

        //create a reader group
        log.info("Creating Reader group : {}", readerGroupName);
        log.info(" scope passed to readergroup manager {}", scope);
        readerGroupManager = ReaderGroupManager.withScope(scope, controllerUri);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(STREAM_NAME));

        log.info(" reader group name {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
        log.info(" reader group scope {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());
        log.info("online readers {}", readerGroupManager.getReaderGroup(readerGroupName).getOnlineReaders());

        //create readers
        log.info("creating {} readers", NUM_READERS);
        List<EventStreamReader<Long>> readerList = new ArrayList<>();
        String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);
        log.info("scope that is seen by readers {}", scope);
        //start reading events
        for (int i = 0; i < NUM_READERS; i++) {
            log.info("starting reader{}", i);
            log.info("creating reader with id {}", readerName + i);
            final EventStreamReader<Long> reader = clientFactory.createReader(readerName + i,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            readerList.add(reader);
        }

        log.info("Test with 2 controller, SSS instances running and without a failover scenario");

        // start writing asynchronously
        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            writerList.forEach(writer -> startWriting(eventData, writer));
        });

        //start reading asynchronously
        CompletableFuture<Void> readerFuture = CompletableFuture.runAsync(() -> {
            readerList.forEach(reader -> startReading(eventsReadFromPravega, eventData, eventReadCount, stopReadFlag, reader));
        });

        //wait for writers completion
        writerFuture.get();

        while (NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }

        //stop reading when no. of reads= no. of writes
        stopReadFlag.set(true);

        //wait for readers completion
        readerFuture.get();

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.

        log.debug("{} with 2 controller, 2 SSS instnaces succeeds", "multiReaderWriterTest");

        segmentStoreInstance.scaleService(2, true);
        Thread.sleep(60000);
        log.info("Scaling down  1 SSS instance");

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag.set(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        // start writing asynchronously
        CompletableFuture<Void> writerFuture1 = CompletableFuture.runAsync(() -> {
            writerList.forEach(writer -> startWriting(eventData, writer));
        });

        //start reading asynchronously
        CompletableFuture<Void> readerFuture1 = CompletableFuture.runAsync(() -> {
            readerList.forEach(reader -> startReading(eventsReadFromPravega, eventData, eventReadCount, stopReadFlag, reader));
        });

        //wait for writers completion
        writerFuture1.get();

        while (NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }

        //stop reading when no. of reads= no. of writes
        stopReadFlag.set(true);

        //wait for readers completion
        readerFuture1.get();

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.

        log.debug("{} with SSS failover succeeds", "multiReaderWriterTest");

        controllerInstance.scaleService(2, true);
        List<URI> conURIs = controllerInstance.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        Thread.sleep(60000);
        log.info("Test with 1 controller instance down");

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag.set(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        // start writing asynchronously
        CompletableFuture<Void> writerFuture2 = CompletableFuture.runAsync(() -> {
            writerList.forEach(writer -> startWriting(eventData, writer));
        });

        //start reading asynchronously
        CompletableFuture<Void> readerFuture2 = CompletableFuture.runAsync(() -> {
            readerList.forEach(reader -> startReading(eventsReadFromPravega, eventData, eventReadCount, stopReadFlag, reader));
        });

        //wait for writers completion
        writerFuture2.get();

        while (NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }

        //stop reading when no. of reads= no. of writes
        stopReadFlag.set(true);

        //wait for readers completion
        readerFuture2.get();

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.

        log.debug("{} with controller failover succeeds", "multiReaderWriterTest");

        //scale down 1 instance of both controller, SSS
        segmentStoreInstance.scaleService(1, true);
        Thread.sleep(60000);
        controllerInstance.scaleService(1, true);
        List<URI> conURIs1 = controllerInstance.getServiceDetails();
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris1 = conURIs1.stream().filter(uri -> uri.getPort() == 9092).map(URI::getAuthority)
                .collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris1));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        Thread.sleep(60000);

        log.info("Test with 1 controller  and 1 SSS instance down");

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        stopReadFlag.set(false);
        eventData = new AtomicLong(); //data used by each of the writers.
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        // start writing asynchronously
        CompletableFuture<Void> writerFuture3 = CompletableFuture.runAsync(() -> {
            writerList.forEach(writer -> startWriting(eventData, writer));
        });

        //start reading asynchronously
        CompletableFuture<Void> readerFuture3 = CompletableFuture.runAsync(() -> {
            readerList.forEach(reader -> startReading(eventsReadFromPravega, eventData, eventReadCount, stopReadFlag, reader));
        });

        //wait for writers completion
        writerFuture3.get();

        while (NUM_EVENTS != eventsReadFromPravega.size()) {
            Thread.sleep(5);
        }

        //stop reading when no. of reads= no. of writes
        stopReadFlag.set(true);

        //wait for readers completion
        readerFuture3.get();

        log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                "Count: {}", eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), eventsReadFromPravega.size());
        assertEquals(eventData.get(), new TreeSet<>(eventsReadFromPravega).size()); //check unique events.

        log.debug("{} with  SSS, controller failover succeeds", "multiReaderWriterTest");

        //close all the writers
        writerList.forEach(writer -> writer.close());
        //close all readers
        readerList.forEach(reader -> reader.close());

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

        log.info("test {} succeeds ", "MultiReaderWriterWithFailOver");

    }

    private void startWriting(final AtomicLong data, final EventStreamWriter<Long> writer) {
        for (int i = 0; i < NUM_EVENTS_BY_WRITER; i++) {
            try {
                long value = data.incrementAndGet();
                log.debug("writing event {}", value);
                writer.writeEvent(String.valueOf(value), value);
                writer.flush();
            } catch (Throwable e) {
                log.warn("test exception writing events: {}", e);
                break;
            }
        }
    }

    private void startReading(final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
    AtomicLong readCount, final AtomicBoolean exitFlag, final EventStreamReader<Long> reader) {
        while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
            // exit only if exitFlag is true  and read Count equals write count.
            try {
                final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                log.debug("reading event {}", longEvent);
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
    }
}
