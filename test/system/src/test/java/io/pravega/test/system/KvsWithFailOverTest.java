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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class KvsWithFailOverTest extends AbstractKVSFailoverTest {
    static final String SCOPE_NAME = "testScope" + System.nanoTime();
    static final String KVT_NAME = "testKvt";
    static final int KVP_COUNT = 5000;
    int controllerPodCount;
    int segmentStorePodCount;
    private StreamManager streamManager;


    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
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
        final List<String> uris = conURIs.stream().filter(ISGRPC).map(URI::getAuthority).collect(Collectors.toList());
        controllerURIDirect = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURIDirect);
        // Verify segment store is running.
        segmentStoreInstance = Utils.createPravegaSegmentStoreService(zkUri, controllerURIDirect);
        assertTrue(segmentStoreInstance.isRunning());
        log.info("Pravega Segmentstore service instance details: {}", segmentStoreInstance.getServiceDetails());
        controllerExecutorService = ExecutorServiceHelpers.newScheduledThreadPool(3, "KVP-CreateGetUpdateThread");
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURIDirect);
        // Service count display twice of the, each pod IP mapped with 9090 and 10080, SO divided by 2 to get actual pod count
        controllerPodCount = (controllerInstance.getServiceDetails().size()) / 2;
        segmentStorePodCount = segmentStoreInstance.getServiceDetails().size();
        log.info("Pod count: Controller {} and Segmentstore {}", controllerPodCount, segmentStorePodCount);
        //get Controller Uri
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig)
                .maxBackoffMillis(5000).build(), controllerExecutorService);
        streamManager = new StreamManagerImpl(clientConfig);
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE_NAME, controller, connectionFactory);
        // Create scope
        createScope(SCOPE_NAME, streamManager);
        createKVT(SCOPE_NAME, KVT_NAME, config, controllerURIDirect);
        log.info("complete scope and kvt");
    }

   @After
    public void tearDown() throws ExecutionException {
        streamManager.close();
        keyValueTableFactory.close();
        ExecutorServiceHelpers.shutdown(controllerExecutorService);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(controllerPodCount), ExecutionException::new);
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(segmentStorePodCount), ExecutionException::new);
    }

    @Test
    public void kvsWithFailOverTest() throws Exception {
        // Start KBP operation like create, Update and Get KVP
        startKVPCreate(SCOPE_NAME, KVT_NAME, KVP_COUNT, keyValueTableFactory, controllerURIDirect);
        //run the failover test like scale up/down of controller and Segmentstore
        performFailoverTest();
        log.info("Test KVS FailOver Successfully completed");
    }
}
