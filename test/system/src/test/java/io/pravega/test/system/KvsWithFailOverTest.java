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

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
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
public class KvsWithFailOverTest extends AbstractFailoverTests {
    static final String SCOPE_NAME = "kvsScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    static final String KVT_NAME = "testKvt";
    static final String KEY_FAMILY = "testKeyFamily";
    private static final int NUM_KVPWRITE = 5;
    private static final int NUM_KVPREAD = 5;
    @Rule
    //The execution time for @Before + @After + @Test methods should be less than 25 mins. Else the test will timeout.
    public Timeout globalTimeout = Timeout.seconds(25 * 60);
    private StreamManager streamManager;
    private ConnectionFactory connectionFactory;
    private ConnectionPool connectionPool;
    private KeyValueTableFactory keyValueTableFactory;

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
    }

    @Before
    public void setup() throws ExecutionException {
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
        final List<String> uris = conURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        // Verify segment store is running.
        segmentStoreInstance = Utils.createPravegaSegmentStoreService(zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(NUM_KVPWRITE + NUM_KVPREAD + 1, "KvsFailOverTest-main");
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(2, "KvsFailOverTest-controller");
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect);
        log.info("BEFORE 1");
        // Making controller and segmentstore instance count to 1
        //log.info("controller size {}", (controllerInstance.getServiceDetails().size()) / 2);
        //if (((controllerInstance.getServiceDetails().size()) / 2) > 1)
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        log.info("BEFORE 2");
        //log.info("Segmentstore size {}", segmentStoreInstance.getServiceDetails().size());
        //if (segmentStoreInstance.getServiceDetails().size() > 1)
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        log.info("BEFORE 3");
        //get Controller Uri
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), controllerExecutorService);
        log.info("BEFORE 4");
        streamManager = new StreamManagerImpl(clientConfig);
        connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        log.info("BEFORE 5");
        keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE_NAME, controller, connectionPool);
        log.info("BEFORE 6");
        // Creating scope and KVT
        createScopeAndKVT(SCOPE_NAME, KVT_NAME, controllerURIDirect, streamManager);
        log.info("completed scope {} and kvt {} creation", SCOPE_NAME, KVT_NAME);
    }

    @After
    public void tearDown() throws ExecutionException {
        log.info("Removing KVP entry");
        deleteKVP(SCOPE_NAME, KVT_NAME, KEY_FAMILY, NUM_KVPWRITE, keyValueTableFactory);
        streamManager.close();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executorService, controllerExecutorService);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
    }

    @Test
    public void kvsWithFailOverTest() throws Exception {
        log.info("Start KVP operation to Update KVP entry");
        writeInKVP(SCOPE_NAME, KVT_NAME, KEY_FAMILY, NUM_KVPWRITE, keyValueTableFactory);
        readFromKVP(SCOPE_NAME, KVT_NAME, KEY_FAMILY, NUM_KVPREAD, keyValueTableFactory);
        log.info("Started Segmentstore and Controller scale up and scale down");
        performFailoverForTestsInvolvingKVS();
        stopKvpWriter();
        stopKvpReaders();
        log.info("Test KVS FailOver Successfully completed");
    }
}
