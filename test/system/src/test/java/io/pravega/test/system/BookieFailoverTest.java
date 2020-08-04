/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.system;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class BookieFailoverTest extends AbstractFailoverTests  {

    private static final String STREAM = "testBookieFailoverStream";
    private static final String SCOPE = "testBookieFailoverScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final int NUM_WRITERS = 5;
    private static final int NUM_READERS = 5;
    private static final int BOOKIE_FAILOVER_WAIT_MILLIS = 15 * 1000;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final String readerGroupName = "testBookieFailoverReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(NUM_READERS);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private StreamManager streamManager;
    private ClientFactoryImpl clientFactory;
    private ReaderGroupManager readerGroupManager;
    private Service bookkeeperService = null;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
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
        final List<String> uris = conURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());

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
                .clientConfig(Utils.buildClientConfig(controllerURIDirect))
                .maxBackoffMillis(5000).build(),
                controllerExecutorService);

        testState = new TestState(false);
        //read and write count variables
        testState.writersListComplete.add(0, testState.writersComplete);
        streamManager = new StreamManagerImpl(Utils.buildClientConfig(controllerURIDirect));
        createScopeAndStream(SCOPE, STREAM, config, streamManager);
        log.info("Scope passed to client factory {}", SCOPE);
        clientFactory = new ClientFactoryImpl(SCOPE, controller, new ConnectionFactoryImpl(Utils.buildClientConfig(controllerURIDirect)));
        readerGroupManager = ReaderGroupManager.withScope(SCOPE, Utils.buildClientConfig(controllerURIDirect));
    }

    @After
    public void tearDown() {
        testState.stopReadFlag.set(true);
        testState.stopWriteFlag.set(true);
        //interrupt writers and readers threads if they are still running.
        testState.cancelAllPendingWork();
        streamManager.close();
        clientFactory.close(); //close the clientFactory/connectionFactory.
        readerGroupManager.close();
        ExecutorServiceHelpers.shutdown(executorService, controllerExecutorService);
    }

    @Test
    public void bookieFailoverTest() throws ExecutionException, InterruptedException {
        createWriters(clientFactory, NUM_WRITERS, SCOPE, STREAM);
        createReaders(clientFactory, readerGroupName, SCOPE, readerGroupManager, STREAM, NUM_READERS);

        // Give some time to create readers before forcing a bookie failover.
        Exceptions.handleInterrupted(() -> Thread.sleep(BOOKIE_FAILOVER_WAIT_MILLIS));

        // Scale down bookie.
        Futures.getAndHandleExceptions(bookkeeperService.scaleService(2), ExecutionException::new);

        log.info("Sleeping for {} seconds.", BOOKIE_FAILOVER_WAIT_MILLIS / 1000);
        Exceptions.handleInterrupted(() -> Thread.sleep(BOOKIE_FAILOVER_WAIT_MILLIS));

        long writeCountBeforeSleep  = testState.getEventWrittenCount();
        long readCountBeforeSleep  = testState.getEventReadCount();
        log.info("Write count is {} and read count is {} after {} seconds sleep after bookie failover.", writeCountBeforeSleep,
                readCountBeforeSleep, BOOKIE_FAILOVER_WAIT_MILLIS / 1000);

        log.info("Sleeping for {} seconds.", BOOKIE_FAILOVER_WAIT_MILLIS / 1000);
        Exceptions.handleInterrupted(() -> Thread.sleep(BOOKIE_FAILOVER_WAIT_MILLIS));

        long writeCountAfterSleep  = testState.getEventWrittenCount();
        long readCountAfterSleep  = testState.getEventReadCount();
        log.info("Write count is {} and read count is {} after {} seconds sleep after bookie failover.", writeCountAfterSleep,
                readCountAfterSleep, 2 * (BOOKIE_FAILOVER_WAIT_MILLIS / 1000));

        Assert.assertEquals("Unexpected writes performed during Bookie failover.", writeCountAfterSleep, writeCountBeforeSleep);
        log.info("Writes failed when bookie is scaled down.");

        // Bring up a new bookie instance.
        Futures.getAndHandleExceptions(bookkeeperService.scaleService(3), ExecutionException::new);

        // Give some more time to writers to write more events.
        Exceptions.handleInterrupted(() -> Thread.sleep(BOOKIE_FAILOVER_WAIT_MILLIS));
        stopWriters();

        // Also, verify writes happened after bookie is brought back.
        long finalWriteCount = testState.getEventWrittenCount();
        log.info("Final write count {}.", finalWriteCount);
        Assert.assertTrue(finalWriteCount > writeCountAfterSleep);

        while (testState.getEventReadCount() < finalWriteCount) {
            Exceptions.handleInterrupted(() -> Thread.sleep(5000));
        }
        log.info("Final read count {}.", testState.getEventReadCount());
        stopReaders();

        // Verify that there is no data loss/duplication.
        validateResults();

        // Cleanup if validation is successful.
        cleanUp(SCOPE, STREAM, readerGroupManager, readerGroupName);

        testState.checkForAnomalies();
        log.info("Test BookieFailover succeeds.");
    }
}
