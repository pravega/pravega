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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWriteAndScaleWithFailoverTest extends AbstractFailoverTests {

    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;
    private final String scope = "testReadWriteAndScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testReadWriteAndScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1); // auto scaling is not enabled.
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(SCALE_STREAM).scalingPolicy(scalingPolicy).build();

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri);
        startPravegaSegmentStoreInstances(zkUri, controllerUri);
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

        //num. of readers + num. of writers + 1 to run checkScale operation
        executorService = Executors.newScheduledThreadPool(NUM_READERS + NUM_WRITERS + 1);
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect, ControllerImplConfig.builder().retryAttempts(1).build(), executorService);
        testState = new TestState();
        testState.writersListComplete.add(0, testState.writersComplete);
    }

    @After
    public void tearDown() {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        controllerInstance.scaleService(1, true);
        segmentStoreInstance.scaleService(1, true);
        executorService.shutdownNow();
        testState.eventsReadFromPravega.clear();
    }

    @Test(timeout = 12 * 60 * 1000)
    public void readWriteAndScaleWithFailoverTest() throws Exception {
        createScopeAndStream(scope, SCALE_STREAM, config, controllerURIDirect);

        log.info("Scope passed to client factory {}", scope);
        try (ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect)) {

            createWriters(clientFactory, NUM_WRITERS, scope, SCALE_STREAM);
            createReaders(clientFactory, readerGroupName, scope, readerGroupManager, SCALE_STREAM, NUM_READERS);

            //run the failover test before scaling
            performFailoverTest();

            //bring the instances back to 3 before performing failover during scaling
            controllerInstance.scaleService(3, true);
            segmentStoreInstance.scaleService(3, true);
            Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            //scale manually
            log.debug("Number of Segments before manual scale:" + controller.getCurrentSegments(scope, SCALE_STREAM)
                    .get().getSegments().size());

            Map<Double, Double> keyRanges = new HashMap<>();
            keyRanges.put(0.0, 0.5);
            keyRanges.put(0.5, 1.0);

            CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, SCALE_STREAM),
                    Collections.singletonList(0),
                    keyRanges,
                    executorService).getFuture();
            FutureHelpers.exceptionListener(scaleStatus, t -> log.error("Scale Operation completed with an error", t));

            //run the failover test while scaling
            performFailoverTest();

            //do a get on scaleStatus
            if (FutureHelpers.await(scaleStatus)) {
                log.info("Scale operation has completed: {}", scaleStatus.get());
                if (!scaleStatus.get()) {
                    log.error("Scale operation did not complete", scaleStatus.get());
                    Assert.fail("Scale operation did not complete successfully");
                }
            } else {
                Assert.fail("Scale operation threw an exception");
            }

            log.debug("Number of Segments post manual scale:" + controller.getCurrentSegments(scope, SCALE_STREAM)
                    .get().getSegments().size());

            //bring the instances back to 3 before performing failover after scaling
            controllerInstance.scaleService(3, true);
            segmentStoreInstance.scaleService(3, true);
            Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            //run the failover test after scaling
            performFailoverTest();

            stopWriters();
            stopReaders();
            validateResults(readerGroupManager, readerGroupName);
        }
        cleanUp(scope, SCALE_STREAM);
        log.info("Test {} succeeds ", "ReadWriteAndScaleWithFailover");
    }
}
