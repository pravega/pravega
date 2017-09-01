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
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import java.net.URI;
import java.net.URISyntaxException;
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

    //The execution time for @Before + @After + @Test methods should be less than 15 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(15 * 60);

    private final String scope = "testReadWriteAndAutoScaleScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testReadWriteAndAutoScaleReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.byEventRate(1, 2, 2);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(AUTO_SCALE_STREAM).scalingPolicy(scalingPolicy).build();
    private ClientFactory clientFactory;
    private ReaderGroupManager readerGroupManager;

    @Environment
    public static void initialize() throws MarathonException, URISyntaxException {
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
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS + TOTAL_NUM_WRITERS + 1,
                                                                        "ReadWriteAndAutoScaleWithFailoverTest-main");
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                                                                                  "ReadWriteAndAutoScaleWithFailoverTest-controller");
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect,
                                        ControllerImplConfig.builder().maxBackoffMillis(5000).build(),
                                        controllerExecutorService);
        testState = new TestState();
        testState.writersListComplete.add(0, testState.writersComplete);
        testState.writersListComplete.add(1, testState.newWritersComplete);

        createScopeAndStream(scope, AUTO_SCALE_STREAM, config, controllerURIDirect);
        log.info("Scope passed to client factory {}", scope);

        clientFactory = new ClientFactoryImpl(scope, controller);
        readerGroupManager = ReaderGroupManager.withScope(scope, controllerURIDirect);
    }

    @After
    public void tearDown() {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        //interrupt writers and readers threads if they are still running.
        testState.writers.forEach(future -> future.cancel(true));
        testState.readers.forEach(future -> future.cancel(true));
        clientFactory.close();
        readerGroupManager.close();
        executorService.shutdownNow();
        controllerExecutorService.shutdownNow();
        testState.eventsReadFromPravega.clear();
        //scale the controller and segmentStore back to 1 instance.
        controllerInstance.scaleService(1, true);
        segmentStoreInstance.scaleService(1, true);
    }


    @Test
    public void readWriteAndAutoScaleWithFailoverTest() throws Exception {

        createWriters(clientFactory, INIT_NUM_WRITERS, scope, AUTO_SCALE_STREAM);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, AUTO_SCALE_STREAM, NUM_READERS);

        //run the failover test before scaling
        performFailoverTest();

        //bring the instances back to 3 before performing failover during scaling
        controllerInstance.scaleService(3, true);
        segmentStoreInstance.scaleService(3, true);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

        addNewWriters(clientFactory, ADD_NUM_WRITERS, scope, AUTO_SCALE_STREAM);

        //run the failover test while scaling
        performFailoverTest();

        waitForScaling();

        //bring the instances back to 3 before performing failover
        controllerInstance.scaleService(3, true);
        segmentStoreInstance.scaleService(3, true);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

        //run the failover test after scaling
        performFailoverTest();

        stopWriters();
        stopReaders();
        validateResults(readerGroupManager, readerGroupName);

        cleanUp(scope, AUTO_SCALE_STREAM); //cleanup if validation is successful.
    }

    private void waitForScaling() throws InterruptedException, ExecutionException {
        for (int waitCounter = 0; waitCounter < 2; waitCounter++) {
            StreamSegments streamSegments = controller.getCurrentSegments(scope, AUTO_SCALE_STREAM).get();
            testState.currentNumOfSegments.set(streamSegments.getSegments().size());
            if (testState.currentNumOfSegments.get() == 2) {
                log.info("The current number of segments is equal to 2, ScaleOperation did not happen");
                //Scaling operation did not happen, wait
                Exceptions.handleInterrupted(() -> Thread.sleep(60000));
                throw new AbstractFailoverTests.ScaleOperationNotDoneException();
            }
            if (testState.currentNumOfSegments.get() > 2) {
                //scale operation successful.
                log.info("Current Number of segments is {}", testState.currentNumOfSegments.get());
                break;
            }
        }
        if (testState.currentNumOfSegments.get() == 2) {
            Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
        }
    }
}
