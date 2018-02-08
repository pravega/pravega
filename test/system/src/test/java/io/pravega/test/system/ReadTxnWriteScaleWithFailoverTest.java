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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadTxnWriteScaleWithFailoverTest extends AbstractFailoverTests {

    private static final int NUM_READERS = 5;
    private static final int NUM_WRITERS = 5;
    private final String scope = "testReadTxnWriteScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String stream = "testReadTxnWriteScaleStream";
    private final String readerGroupName = "testReadTxnWriteScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1); // auto scaling is not enabled.
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(stream).scalingPolicy(scalingPolicy).build();
    private ClientFactory clientFactory;
    private ReaderGroupManager readerGroupManager;
    private StreamManager streamManager;

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri);
        startPravegaSegmentStoreInstances(zkUri, controllerUri);
    }


    @Before
    public void setup() {
        // Get zk details to verify if controller, segmentstore are running
        Service zkService =  Utils.createZookeeperService();
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        // Verify controller is running.
        controllerInstance = Utils.createPravegaControllerService(zkUri);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> Utils.DOCKER_BASED ? uri.getPort() == Utils.DOCKER_CONTROLLER_PORT
                : uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = Utils.createPravegaSegmentStoreService(zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());

        //num. of readers + num. of writers + 1 to run checkScale operation
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS + NUM_WRITERS + 1,
                "ReadTxnWriteScaleWithFailoverTest-main");
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                "ReadTxnWriteScaleWithFailoverTest-controller");
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect,
                ControllerImplConfig.builder().maxBackoffMillis(5000).build(),
                controllerExecutorService);
        testState = new TestState(true);
        testState.writersListComplete.add(0, testState.writersComplete);
        streamManager = new StreamManagerImpl(controllerURIDirect);
        createScopeAndStream(scope, stream, config, streamManager);
        log.info("Scope passed to client factory {}", scope);
        clientFactory = new ClientFactoryImpl(scope, controller);
        readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect);
    }

    @After
    public void tearDown() throws ExecutionException {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        testState.checkForAnomalies();
        //interrupt writers and readers threads if they are still running.
        testState.cancelAllPendingWork();
        streamManager.close();
        clientFactory.close();
        readerGroupManager.close();
        executorService.shutdownNow();
        controllerExecutorService.shutdownNow();
        //scale the controller and segmentStore back to 1 instance.
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
    }

    @Test(timeout = 30 * 60 * 1000)
    public void readTxnWriteScaleWithFailoverTest() throws Exception {
        try {
            createWriters(clientFactory, NUM_WRITERS, scope, stream);
            createReaders(clientFactory, readerGroupName, scope, readerGroupManager, stream, NUM_READERS);

            //run the failover test before scaling
            performFailoverForTestsInvolvingTxns();

        //bring the instances back to 3 before performing failover during scaling
        Futures.getAndHandleExceptions(controllerInstance.scaleService(3), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);

        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            //scale manually
            log.debug("Number of Segments before manual scale: {}", controller.getCurrentSegments(scope, stream)
                    .get().getSegments().size());

            Map<Double, Double> keyRanges = new HashMap<>();
            keyRanges.put(0.0, 0.2);
            keyRanges.put(0.2, 0.4);
            keyRanges.put(0.4, 0.6);
            keyRanges.put(0.6, 0.8);
            keyRanges.put(0.8, 1.0);

            CompletableFuture<Boolean> scaleStatus = controller.scaleStream(new StreamImpl(scope, stream),
                    Collections.singletonList(0),
                    keyRanges,
                    executorService).getFuture();
            Futures.exceptionListener(scaleStatus, t -> log.error("Scale Operation completed with an error", t));

            //run the failover test while scaling
            performFailoverForTestsInvolvingTxns();

            //do a get on scaleStatus
            if (Futures.await(scaleStatus)) {
                log.info("Scale operation has completed: {}", scaleStatus.get());
                if (!scaleStatus.get()) {
                    log.error("Scale operation did not complete", scaleStatus.get());
                    Assert.fail("Scale operation did not complete successfully");
                }
            } else {
                Assert.fail("Scale operation threw an exception");
            }

            log.debug("Number of Segments post manual scale: {}", controller.getCurrentSegments(scope, stream)
                    .get().getSegments().size());

        //bring the instances back to 3 before performing failover after scaling
        Futures.getAndHandleExceptions(controllerInstance.scaleService(3), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            //run the failover test after scaling
            performFailoverForTestsInvolvingTxns();

            stopWriters();
            waitForTxnsToComplete();
            stopReaders();
            validateResults();

            cleanUp(scope, stream, readerGroupManager, readerGroupName); //cleanup if validation is successful.
            log.info("Test ReadTxnWriteScaleWithFailover succeeds");
        } finally {
            testState.checkForAnomalies();
        }
    }

}
