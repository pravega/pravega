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
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.docker.HDFSDockerService;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static io.pravega.test.system.framework.TestFrameworkException.Type.RequestFailed;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MultiReaderTxnWriterWithFailoverTest extends AbstractFailoverTests {

    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;
    private static final String STREAM_NAME = "testMultiReaderWriterTxnStream";
    //The execution time for @Before + @After + @Test methods should be less than 15 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(15 * 60);
    private final String scope = "testMultiReaderWriterTxnScope" + new Random().nextInt(Integer.MAX_VALUE);
    private final String readerGroupName = "testMultiReaderWriterTxnReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);
    private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(scope)
            .streamName(STREAM_NAME).scalingPolicy(scalingPolicy).build();
    private ClientFactory clientFactory;
    private ReaderGroupManager readerGroupManager;
    private StreamManager streamManager;

    @Environment
    public static void initialize() throws InterruptedException, MarathonException, URISyntaxException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI hdfsUri = startHDFSInstances();
        URI controllerUri = startPravegaControllerInstances(zkUri);
        startPravegaSegmentStoreInstances(zkUri, controllerUri, hdfsUri);
    }

    @Before
    public void setup() {

        // Get zk details to verify if controller, SSS are running
        Service zkService = Utils.createServiceInstance("zookeeper", null, null, null);
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        // Verify hdfs is running.
        URI hdfsUri = null;
        if (Utils.isDockerLocalExecEnabled()) {
            Service hdfsService = new HDFSDockerService("hdfs");
            if (!hdfsService.isRunning()) {
                hdfsService.start(true);
            }
            hdfsUri = hdfsService.getServiceDetails().get(0);
            log.debug("HDFS service details: {}", hdfsService.getServiceDetails());
        }

        // Verify controller is running.
        controllerInstance = Utils.createServiceInstance("controller", zkUri, null, null);
        assertTrue(controllerInstance.isRunning());
        List<URI> conURIs = controllerInstance.getServiceDetails();
        log.info("Pravega Controller service instance details: {}", conURIs);

        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conURIs.stream().filter(uri -> Utils.isDockerLocalExecEnabled() ? uri.getPort() == Utils.DOCKER_CONTROLLER_PORT : uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                .collect(Collectors.toList());

        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);

        // Verify segment store is running.
        segmentStoreInstance = Utils.createServiceInstance("segmentstore", zkUri, hdfsUri, null);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());
        //executor service
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS + NUM_WRITERS + 2,
                                                                        "MultiReaderTxnWriterWithFailoverTest-main");
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                                                                                  "MultiReaderTxnWriterWithFailoverTest-controller");
        //get Controller Uri
        controller = new ControllerImpl(controllerURIDirect,
                                        ControllerImplConfig.builder().maxBackoffMillis(5000).build(),
                                        controllerExecutorService);

        testState = new TestState();
        testState.txnWrite.set(true);
        //read and write count variables
        testState.writersListComplete.add(0, testState.writersComplete);
        streamManager = new StreamManagerImpl(controllerURIDirect);
        createScopeAndStream(scope, STREAM_NAME, config, streamManager);
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
        streamManager.close();
        clientFactory.close(); //close the clientFactory/connectionFactory.
        readerGroupManager.close();
        executorService.shutdownNow();
        controllerExecutorService.shutdownNow();
        testState.eventsReadFromPravega.clear();
        testState.txnStatusFutureList.clear();
        //scale the controller and segmentStore back to 1 instance.
        try {
            Exceptions.handleInterrupted(() -> controllerInstance.scaleService(1).get());
            Exceptions.handleInterrupted(() -> segmentStoreInstance.scaleService(1).get());
        } catch (ExecutionException e) {
            throw new TestFrameworkException(RequestFailed, "Scaling operation failed", e);
        }
}

    @Test
    public void multiReaderTxnWriterWithFailOverTest() throws Exception {

        createWriters(clientFactory, NUM_WRITERS, scope, STREAM_NAME);
        createReaders(clientFactory, readerGroupName, scope, readerGroupManager, STREAM_NAME, NUM_READERS);

        //run the failover test
        performFailoverForTestsInvolvingTxns();

        stopWriters();
        waitForTxnsToComplete();
        stopReaders();
        validateResults();

        cleanUp(scope, STREAM_NAME, readerGroupManager, readerGroupName); //cleanup if validation is successful.

        log.info("Test MultiReaderWriterTxnWithFailOver succeeds");
    }
}
