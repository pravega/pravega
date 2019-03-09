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

import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ControllerServiceMain tests.
 */
public abstract class ControllerServiceMainTest {

    protected StoreClientConfig storeClientConfig;
    private final boolean disableControllerCluster;

    ControllerServiceMainTest(final boolean disableControllerCluster) {
        this.disableControllerCluster = disableControllerCluster;
    }

    @Before
    public abstract void setup();

    @After
    public abstract void tearDown();

    @Slf4j
    static class MockControllerServiceStarter extends ControllerServiceStarter {

        public MockControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient) {
            super(serviceConfig, storeClient);
        }

        @Override
        protected void startUp() {
            log.info("MockControllerServiceStarter started.");
        }

        @Override
        protected void shutDown() throws Exception {
            log.info("MockControllerServiceStarter shutdown.");
        }
    }

    static CompletableFuture<Void> called = new CompletableFuture<>();
    
    static void handleUncaughtException(Thread t, Throwable e) {
        called.complete(null);    
    }

    @Test(timeout = 10000)
    public void testUncaughtException() {
        Main.setUncaughtExceptionHandler(Main::logUncaughtException);
        Main.setUncaughtExceptionHandler(ControllerServiceMainTest::handleUncaughtException);
        
        Thread t = new Thread(() -> {
            throw new RuntimeException();
        });
        
        t.start();
        
        called.join();
    }
    
    @Test(timeout = 10000)
    public void mainShutdownTest() {
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();
        try {
            controllerServiceMain.awaitRunning();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for controllerServiceMain to get ready");
        }

        try {
            controllerServiceMain.awaitServiceStarting().awaitRunning();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for starter to get ready");
        }

        Main.onShutdown(controllerServiceMain);
        
        controllerServiceMain.awaitTerminated();
    }
    
    @Test(timeout = 10000)
    public void testControllerServiceMainStartStop() {
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();
        try {
            controllerServiceMain.awaitRunning();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for controllerServiceMain to get ready");
        }

        try {
            controllerServiceMain.awaitServiceStarting().awaitRunning();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for starter to get ready");
        }

        controllerServiceMain.stopAsync();

        try {
            controllerServiceMain.awaitServicePausing().awaitTerminated();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for termination of starter");
        }

        try {
            controllerServiceMain.awaitTerminated();
        } catch (IllegalStateException e) {
            Assert.fail("Failed waiting for termination of controllerServiceMain");
        }
    }

    protected ControllerServiceConfig createControllerServiceConfig() {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST, Config.SERVICE_PORT,
                        Config.HOST_STORE_CONTAINER_COUNT))
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .build();

        return ControllerServiceConfigImpl.builder()
                .threadPoolSize(15)
                .storeClientConfig(storeClientConfig)
                .controllerClusterListenerEnabled(!disableControllerCluster)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.empty())
                .grpcServerConfig(Optional.empty())
                .restServerConfig(Optional.empty())
                .build();
    }
}
