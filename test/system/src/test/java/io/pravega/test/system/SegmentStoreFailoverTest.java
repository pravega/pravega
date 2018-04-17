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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import static io.pravega.test.system.AbstractFailoverTests.WAIT_AFTER_FAILOVER_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentStoreFailoverTest {
    private static final String STREAM = "testSegmentStoreFailoverStream";
    private static final String SCOPE = "testSegmentStoreFailoverScope" + new Random().nextInt(Integer.MAX_VALUE);
    public  Service segService;
    private final String testString = "Event\n";
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMinutes(1));
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM).scalingPolicy(scalingPolicy).retentionPolicy(retentionPolicy).build();
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    private URI controllerURI;
    private StreamManager streamManager;
    private Service zkService;
    private TestState testState;
    private Segment segment;

    static class TestState {
        //append flags
        final AtomicBoolean stopAppendFlag = new AtomicBoolean(false);
        final AtomicLong appendCount = new AtomicLong();
        //read flags
        final AtomicLong readCount = new AtomicLong();
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
    }

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {

        //1. Start Zookeeper
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);

        //2. Start Bookkeeper
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        //3. Start Pravega Controller
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega controller service details: {}", conUris);

        //4.Start  Pravega SegmentStore
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }
        Futures.getAndHandleExceptions(segService.scaleService(2), ExecutionException::new);

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
    }

    @Before
    public void setup() {
        zkService = Utils.createZookeeperService();

        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

        segService = Utils.createPravegaSegmentStoreService(null, null);

        streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
        segment = Segment.fromScopedName(SCOPE+ "/" + STREAM + "/0");

        testState = new TestState();
    }

    @Test
    public void segmentStoreFailoverTest() throws InterruptedException, ExecutionException {
        //connection factory
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(
                ClientConfig.builder().controllerURI(controllerURI).build())
                .build(),
                connectionFactory.getInternalExecutor());

        //appends
        SegmentOutputStreamFactoryImpl segmentOutputClient = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
        SegmentOutputStream out = segmentOutputClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(), "");
        final CompletableFuture<Void> appendFuture = append(out);
        Futures.exceptionListener(appendFuture, t -> log.error("Exception while  appending events to segment:", t));

        //Kill 1 segmentstore instance
        Futures.getAndHandleExceptions(segService.scaleService(1), ExecutionException::new);
        log.info("Scaling down segmentstore instances from 2 to 1");

        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

        testState.stopAppendFlag.set(true);
        //Wait for append futures to return
        appendFuture.get();

        //reads
        SegmentInputStreamFactoryImpl segmentInputClient = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        SegmentInputStream in = segmentInputClient.createInputStreamForSegment(segment);
        final CompletableFuture<Void> readFuture = read(in);
        Futures.exceptionListener(readFuture, t -> log.error("Exception while reading events from segment:", t));

        testState.stopReadFlag.set(true);
        //Wait for read futures to return
        readFuture.get();

        //match append and read count
        log.info("Append count = {} Read count = {}", testState.appendCount.get(), testState.readCount.get());
        Assert.assertEquals(testState.appendCount.get(), testState.readCount.get());
        log.info("SegmentStore Failover test passes");
    }

    private CompletableFuture<Void> append(SegmentOutputStream segmentOutputStream) {
        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        return CompletableFuture.runAsync(() -> {
        while (!testState.stopAppendFlag.get()) {
            segmentOutputStream.write(new PendingEvent(null, ByteBuffer.wrap(testString.getBytes()), ack));
            log.info("Event {} appended to segment", testString);
            try {
                assertTrue(ack.get(5, TimeUnit.SECONDS));
                testState.appendCount.incrementAndGet();
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        });
    }

    private CompletableFuture<Void> read(SegmentInputStream segmentInputStream) {
        return CompletableFuture.runAsync(() -> {
            while (!(testState.stopReadFlag.get() && testState.readCount.get() == testState.appendCount.get())) {
                try {
                    assertEquals(ByteBuffer.wrap(testString.getBytes()), segmentInputStream.read());
                    log.info("Reading event: {}", testString);
                    testState.readCount.incrementAndGet();
                } catch (EndOfSegmentException | SegmentTruncatedException e) {
                    e.printStackTrace();
                }
            }
            segmentInputStream.close();
        });
    }
}
