/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import com.emc.pravega.controller.server.impl.ControllerServiceConfigImpl;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testControllerServiceMainStartStop() {

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST, Config.SERVICE_PORT,
                        Config.HOST_STORE_CONTAINER_COUNT))
                .build();

        Optional<ControllerClusterListenerConfig> controllerClusterListenerConfig;
        if (!disableControllerCluster) {
            controllerClusterListenerConfig = Optional.of(ControllerClusterListenerConfigImpl.builder()
                    .minThreads(2)
                    .maxThreads(10)
                    .idleTime(10)
                    .idleTimeUnit(TimeUnit.SECONDS)
                    .maxQueueSize(512)
                    .build());
        } else {
            controllerClusterListenerConfig = Optional.empty();
        }

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .serviceThreadPoolSize(3)
                .taskThreadPoolSize(3)
                .storeThreadPoolSize(3)
                .eventProcThreadPoolSize(3)
                .requestHandlerThreadPoolSize(3)
                .storeClientConfig(storeClientConfig)
                .controllerClusterListenerConfig(controllerClusterListenerConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.empty())
                .requestHandlersEnabled(false)
                .grpcServerConfig(Optional.empty())
                .restServerConfig(Optional.empty())
                .build();

        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(serviceConfig,
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();
        try {
            controllerServiceMain.awaitRunning();
        } catch (IllegalStateException e) {
            Assert.fail("Failed starting controllerServiceMain");
        }

        controllerServiceMain.stopAsync();
        try {
            controllerServiceMain.awaitTerminated();
        } catch (IllegalStateException e) {
            Assert.fail("Failed stopping controllerServiceMain");
        }
    }
}
