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
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BookieFailoverTest extends AbstractFailoverTests  {

    private static final String STREAM = "testBookieFailoverStream";
    private static final String SCOPE = "testBookieFailoverScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final String readerGroupName = "testBookieFailoverReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
    private final StreamConfiguration config = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM).scalingPolicy(scalingPolicy).build();

    private StreamManager streamManager;
    private ClientFactory clientFactory;
    private ReaderGroupManager readerGroupManager;
    private Service bookkeeperService = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {

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

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
    }

    @Before
    public void setup() {

        // Get zk details to verify if controller, SSS are running
        Service zkService = Utils.createZookeeperService();
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        //get the zk ip details and pass it to  host, controller
        URI zkUri = zkUris.get(0);

        //Verify bookie is running
        bookkeeperService = Utils.createBookkeeperService(zkUri);
        List<URI> bkUris = bookkeeperService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

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

        executorService = ExecutorServiceHelpers.newScheduledThreadPool( NUM_READERS + NUM_WRITERS + 1, "BookieFailoverTest-main");

        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2,
                "BookieFailoverTest-controller");
        //get Controller Uri
        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(controllerURIDirect).build())
                .maxBackoffMillis(5000).build(),
                controllerExecutorService);

        testState = new TestState(false);
        //read and write count variables
        testState.writersListComplete.add(0, testState.writersComplete);
        streamManager = new StreamManagerImpl(ClientConfig.builder().controllerURI(controllerURIDirect).build());
        createScopeAndStream(SCOPE, STREAM, config, streamManager);
        log.info("Scope passed to client factory {}", SCOPE);
        clientFactory = new ClientFactoryImpl(SCOPE, controller);
        readerGroupManager = ReaderGroupManager.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerURIDirect).build());
    }


    @After
    public void tearDown() {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        testState.checkForAnomalies();
        //interrupt writers and readers threads if they are still running.
        testState.cancelAllPendingWork();
        streamManager.close();
        clientFactory.close(); //close the clientFactory/connectionFactory.
        readerGroupManager.close();
        ExecutorServiceHelpers.shutdown(executorService, controllerExecutorService);
    }



    @Test
    public void bookieFailoverTest() throws ExecutionException, InterruptedException {

        try {
            createWriters(clientFactory, NUM_WRITERS, SCOPE, STREAM);
            createReaders(clientFactory, readerGroupName, SCOPE, readerGroupManager, STREAM, NUM_READERS);

            // Give some time to create readers before forcing a bookie failover.
            Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            // Scale down bookie.
            Futures.getAndHandleExceptions(bookkeeperService.scaleService(2), ExecutionException::new);

            log.info("Sleeping for {} seconds.", WAIT_AFTER_FAILOVER_MILLIS / 1000);
            Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            long writeCountBeforeSleep  = testState.getEventWrittenCount();
            log.info("Write count after bookie failover after {} seconds sleep {}.", WAIT_AFTER_FAILOVER_MILLIS / 1000, writeCountBeforeSleep);

            log.info("Sleeping for {} seconds.", WAIT_AFTER_FAILOVER_MILLIS / 1000);
            Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

            long writeCountAfterSleep  = testState.getEventWrittenCount();
            log.info("Write count after bookie failover after {} seconds sleep {}.", WAIT_AFTER_FAILOVER_MILLIS / 1000, writeCountAfterSleep);

            Assert.assertEquals("Unexpected writes performed during Bookie failover.", writeCountAfterSleep, writeCountBeforeSleep);
            log.info("Writes failed when bookie is scaled down.");

            // Bring back the bookie which was killed.
            Futures.getAndHandleExceptions(bookkeeperService.scaleService(3), ExecutionException::new);

            stopWriters();

            // Also, verify writes happened after bookie is brought back.
            long finalWriteCount = testState.getEventWrittenCount();
            log.info("Final write count {}.", finalWriteCount);
            Assert.assertTrue( finalWriteCount > writeCountAfterSleep);

            stopReaders();

            // Verify that there is no data loss/duplication.
            validateResults();

            // Cleanup if validation is successful.
            cleanUp(SCOPE, STREAM, readerGroupManager, readerGroupName);

            log.info("Test BookieFailover succeeds.");
        } finally {
            testState.checkForAnomalies();
        }
    }
}
