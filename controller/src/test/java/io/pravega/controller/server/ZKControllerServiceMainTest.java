/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.controller.store.client.StoreClient;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ZK store based ControllerServiceMain tests.
 */
@Slf4j
public class ZKControllerServiceMainTest extends ControllerServiceMainTest {
    private TestingServer zkServer;

    public ZKControllerServiceMainTest() {
        super(false);
    }

    @Override
    public void setup() {
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            log.error("Error starting test zk server");
            Assert.fail("Error starting test zk server");
        }

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkServer.getConnectString())
                .initialSleepInterval(500)
                .maxRetries(10)
                .sessionTimeoutMs(10 * 1000)
                .namespace("pravega/" + UUID.randomUUID())
                .build();
        storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);
    }

    @Override
    public void tearDown() {
        try {
            zkServer.close();
        } catch (IOException e) {
            log.error("Error stopping test zk server");
            Assert.fail("Error stopping test zk server");
        }
    }
    
    @Slf4j
    static class MockControllerServiceStarter extends ControllerServiceStarter {
        static CompletableFuture<Void> signalShutdownStarted = new CompletableFuture<>();
        static CompletableFuture<Void> waitingForShutdownSignal = new CompletableFuture<>();

        MockControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient) {
            super(serviceConfig, storeClient);
        }

        @Override
        protected void startUp() { }

        @Override
        protected void shutDown() throws Exception {
            signalShutdownStarted.complete(null);
            waitingForShutdownSignal.join();
        }
    }

    @Test(timeout = 100000)
    public void testZKSessionExpiry() throws Exception {
        AtomicReference<StoreClient> client = new AtomicReference<>();
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                (x, y) -> {
                    client.set(y);
                    return new MockControllerServiceStarter(x, y);
                });

        controllerServiceMain.startAsync();

        try {
            controllerServiceMain.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceMain to get ready", e);
            Assert.fail("Failed waiting for controllerServiceMain to get ready");
        }

        MockControllerServiceStarter controllerServiceStarter = (MockControllerServiceStarter) controllerServiceMain.awaitServiceStarting();
        try {
            controllerServiceStarter.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter to get ready", e);
            Assert.fail("Failed waiting for controllerServiceStarter to get ready");
            return;
        }

        CuratorFramework curatorClient = (CuratorFramework) client.get().getClient();
        // Simulate ZK session timeout
        try {
            curatorClient.getZookeeperClient().getZooKeeper().getTestable().injectSessionExpiration();
        } catch (Exception e) {
            log.error("Failed while simulating client session expiry", e);
            Assert.fail("Failed while simulating client session expiry");
        }

        // verify that we are waiting for termination.
        MockControllerServiceStarter.signalShutdownStarted.join();
        CompletableFuture<Void> callBackCalled = new CompletableFuture<>();
        
        // issue a zkClient request.. 
        curatorClient.getData().inBackground((client1, event) -> {
            callBackCalled.complete(null);        
        }).forPath("/test");
        
        callBackCalled.join();
        
        // complete termination only when zk call completes. 
        MockControllerServiceStarter.waitingForShutdownSignal.complete(null);
        
        // Now, that session has expired, lets wait for starter to start again.
        try {
            controllerServiceMain.awaitServicePausing().awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter termination", e);
            Assert.fail("Failed waiting for controllerServiceStarter termination");
        }

        try {
            controllerServiceMain.awaitServiceStarting().awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for starter to get ready again", e);
            Assert.fail("Failed waiting for controllerServiceStarter to get ready again");
        }

        controllerServiceMain.stopAsync();

        try {
            controllerServiceMain.awaitServicePausing().awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for controllerServiceStarter termination", e);
            Assert.fail("Failed waiting for controllerServiceStarter termination");
        }

        try {
            controllerServiceMain.awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Failed waiting for termination of controllerServiceMain", e);
            Assert.fail("Failed waiting for termination of controllerServiceMain");
        }
    }
}
