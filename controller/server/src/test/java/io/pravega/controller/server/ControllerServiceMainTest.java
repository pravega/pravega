/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server;

import io.pravega.controller.fault.ControllerClusterListenerConfig;
import io.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
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

        return ControllerServiceConfigImpl.builder()
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
    }
}
