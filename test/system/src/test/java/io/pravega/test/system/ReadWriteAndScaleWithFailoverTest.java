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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
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
    private final ScalingPolicy scalingPolicy = ScalingPolicy.byEventRate(1, 2, 1);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();

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

        //executor service
        executorService = Executors.newScheduledThreadPool(NUM_READERS + NUM_WRITERS);
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

    @Test(timeout = 12 * 60 * 1000)
    public void readWriteAndScaleWithFailoverTest() throws Exception {
        createScopeAndStream(scope, STREAM_NAME, config, controllerURIDirect);

        log.info("Scope passed to client factory {}", scope);
        try (ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);
             ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect)) {

            createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
            createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

            //run the failover test before scaling
            performFailoverTest();

            //bring the instances back to 3 before performing failover during scaling
            controllerInstance.scaleService(3, true);
            segmentStoreInstance.scaleService(3, true);
            Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS);

            //scale manually
            log.debug("Scale down stream starting segments:" + controller.getCurrentSegments(scope, STREAM_NAME)
                    .get().getSegments().size());

            Map<Double, Double> keyRanges = new HashMap<>();
            keyRanges.put(0.0, 0.5);
            keyRanges.put(0.5, 1.0);

            CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, STREAM_NAME),
                    Collections.singletonList(0),
                    keyRanges,
                    executorService).getFuture();

            //run the failover test while scaling
            performFailoverTest();

            //do a get on scaleStatus
            scaleStatus.get();
            log.debug("Scale down stream final segments:" + controller.getCurrentSegments(scope, STREAM_NAME)
                    .get().getSegments().size());

            //bring the instances back to 3 before performing failover after scaling
            controllerInstance.scaleService(3, true);
            segmentStoreInstance.scaleService(3, true);
            Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS);

            //run the failover test after scaling
            performFailoverTest();

            stopReadersAndWriters(readerGroupManager, readerGroupName);
        }
        cleanUp(scope, STREAM_NAME);
        log.info("Test {} succeeds ", "ReadWriteAndScaleWithFailover");
    }
}
