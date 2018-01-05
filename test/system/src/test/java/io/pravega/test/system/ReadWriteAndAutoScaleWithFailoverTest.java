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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWriteAndAutoScaleWithFailoverTest extends AbstractFailoverTests {

    private static final int INIT_NUM_WRITERS = 2;
    private static final int ADD_NUM_WRITERS = 2;
    private static final int NUM_READERS = 2;
    private static final int TOTAL_NUM_WRITERS = INIT_NUM_WRITERS + ADD_NUM_WRITERS;

    //The execution time for @Before + @After + @Test methods should be less than 25 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(25 * 60);

    private final String scope = "testReadWriteAndAutoScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testReadWriteAndAutoScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.byEventRate(1, 2, 2);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(AUTO_SCALE_STREAM).scalingPolicy(scalingPolicy).build();
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
        // Get zk details to verify if controller, SSS are running
        Service zkService = Utils.createZookeeperService();
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

        //executor service
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS + TOTAL_NUM_WRITERS + 1,
                                                                        "ReadWriteAndAutoScaleWithFailoverTest-main");
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                                                                                  "ReadWriteAndAutoScaleWithFailoverTest-controller");
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect,
                                        ControllerImplConfig.builder().maxBackoffMillis(5000).build(),
                                        controllerExecutorService);
        testState = new TestState(false);
        testState.writersListComplete.add(0, testState.writersComplete);
        testState.writersListComplete.add(1, testState.newWritersComplete);
        streamManager = new StreamManagerImpl(controllerURIDirect);
        createScopeAndStream(scope, AUTO_SCALE_STREAM, config, streamManager);
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


    @Test(timeout = 25 * 60 * 1000)
    public void readWriteAndAutoScaleWithFailoverTest() throws Exception {
        try {
            createWriters(clientFactory, INIT_NUM_WRITERS, scope, AUTO_SCALE_STREAM);
            createReaders(clientFactory, readerGroupName, scope, readerGroupManager, AUTO_SCALE_STREAM, NUM_READERS);

            //run the failover test before scaling
            performFailoverTest();

        //bring the instances back to 3 before performing failover during scaling
        Futures.getAndHandleExceptions(controllerInstance.scaleService(3), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            addNewWriters(clientFactory, ADD_NUM_WRITERS, scope, AUTO_SCALE_STREAM);

            //run the failover test while scaling
            performFailoverTest();

            waitForScaling(scope, AUTO_SCALE_STREAM, config);

        //bring the instances back to 3 before performing failover
        Futures.getAndHandleExceptions(controllerInstance.scaleService(3), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(3), ExecutionException::new);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            //run the failover test after scaling
            performFailoverTest();

            stopWriters();
            stopReaders();
            validateResults();

            cleanUp(scope, AUTO_SCALE_STREAM, readerGroupManager, readerGroupName); //cleanup if validation is successful.
            log.info("Test ReadWriteAndAutoScaleWithFailover succeeds");
        } finally {
            testState.checkForAnomalies();
        }
    }
}
