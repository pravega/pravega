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
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentStoreFailoverTest {

    private static final String STREAM = "testSegmentStoreFailoverStream";
    private static final String SCOPE = "testSegmentStoreFailoverScope" + new Random().nextInt(Integer.MAX_VALUE);
    public  Service segService;
    Service zkService;
    private final ScalingPolicy scalingPolicy = ScalingPolicy.byEventRate(1, 1, 2);
    private final RetentionPolicy retentionPolicy = RetentionPolicy.byTime(Duration.ofMinutes(1));
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM).scalingPolicy(scalingPolicy).retentionPolicy(retentionPolicy).build();
    private URI controllerURI;
    private StreamManager streamManager;
    private MockHostControllerStore mockHostControllerStore = new MockHostControllerStore();
    private final Consumer<Segment> segmentSealedCallback = segment -> { };



    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {

        //1. check if zk is running, if not start it
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to bk, host, controller
        URI zkUri = zkUris.get(0);
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        //3. start controller
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega controller service details: {}", conUris);

        //4.start segmentstore
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
    }



    @Test
    public void segmentStoreFailoverTest() throws InterruptedException, ExecutionException, TimeoutException, EndOfSegmentException, SegmentTruncatedException {

        //connection factory
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        SegmentHelper segmentHelper = new SegmentHelper();

        //create segment
        CompletableFuture<Boolean> segmentCreated = segmentHelper.createSegment(SCOPE, STREAM, 2, scalingPolicy, mockHostControllerStore, connectionFactory, "");
        assertTrue(segmentCreated.get(5, TimeUnit.SECONDS));
        log.info("Segment number 2 created");

        //appends
        String testString = "First Event\n";
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(
                ClientConfig.builder().controllerURI(controllerURI).build())
                .build(),
                connectionFactory.getInternalExecutor());

        SegmentOutputStreamFactoryImpl segmentOutputClient = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);

        Segment segment = Segment.fromScopedName(SCOPE+ "/" + STREAM + "/2");

        SegmentOutputStream out = segmentOutputClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(), "");

        CompletableFuture<Boolean> ack = new CompletableFuture<>();

        out.write(new PendingEvent(null, ByteBuffer.wrap(testString.getBytes()), ack));
        assertTrue(ack.get(5, TimeUnit.SECONDS));

        log.info("Event appended to segment");

        //Kill 1 segmentstore instance
        Futures.getAndHandleExceptions(segService.scaleService(1), ExecutionException::new);

        //create txn segment
        CompletableFuture<UUID> transactionSegment = segmentHelper.createTransaction(SCOPE, STREAM, 2, UUID.randomUUID(),
                mockHostControllerStore, connectionFactory, "");
        transactionSegment.get();

        log.info("Transaction segment created");

        //merge txn segment
        CompletableFuture<Controller.TxnStatus> commitTxn = segmentHelper.commitTransaction(SCOPE, STREAM, 2, transactionSegment.get(),
                mockHostControllerStore, connectionFactory, "");

        assertEquals(0, commitTxn.get().getStatus().getNumber());
        log.info("Transaction committed");

        //reads
        SegmentInputStreamFactoryImpl segmentInputClient = new SegmentInputStreamFactoryImpl(controller, connectionFactory);

        SegmentInputStream in = segmentInputClient.createInputStreamForSegment(segment);
        assertEquals(ByteBuffer.wrap(testString.getBytes()), in.read());

    }


    private class MockHostControllerStore implements HostControllerStore {

        @Override
        public Map<Host, Set<Integer>> getHostContainersMap() {
            return null;
        }

        @Override
        public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {

        }

        @Override
        public int getContainerCount() {
            return 0;
        }

        @Override
        public Host getHostForSegment(String scope, String stream, int segmentNumber) {
            return new Host(segService.getServiceDetails().get(0).getHost(), 12345, "");
        }
    }

}
