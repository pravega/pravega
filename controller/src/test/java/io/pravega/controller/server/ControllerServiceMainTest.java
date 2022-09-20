/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server;

import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * ControllerServiceMain tests.
 */
public abstract class ControllerServiceMainTest {
    private static final CompletableFuture<Void> INVOKED = new CompletableFuture<>();
    
    protected StoreClientConfig storeClientConfig;

    private final boolean disableControllerCluster;

    ControllerServiceMainTest(final boolean disableControllerCluster) {
        this.disableControllerCluster = disableControllerCluster;
    }

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws IOException;

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
    
    static void handleUncaughtException(Thread t, Throwable e) {
        INVOKED.complete(null);    
    }

    @Test(timeout = 10000)
    public void testUncaughtException() {
        Main.setUncaughtExceptionHandler(Main::logUncaughtException);
        Main.setUncaughtExceptionHandler(ControllerServiceMainTest::handleUncaughtException);
        
        Thread t = new Thread(() -> {
            throw new RuntimeException();
        });
        
        t.start();

        INVOKED.join();
    }
    
    @Test(timeout = 10000)
    public void mainShutdownTest() {
        @Cleanup
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();
        controllerServiceMain.awaitRunning();
        controllerServiceMain.awaitServiceStarting().awaitRunning();

        Main.onShutdown(controllerServiceMain);
        
        controllerServiceMain.awaitTerminated();
    }
    
    @Test(timeout = 10000)
    public void testControllerServiceMainStartStop() {
        @Cleanup
        ControllerServiceMain controllerServiceMain = new ControllerServiceMain(createControllerServiceConfig(),
                MockControllerServiceStarter::new);

        controllerServiceMain.startAsync();
        controllerServiceMain.awaitRunning();
        controllerServiceMain.awaitServiceStarting().awaitRunning();
        controllerServiceMain.stopAsync();
        controllerServiceMain.awaitServicePausing().awaitTerminated();
        controllerServiceMain.awaitTerminated();
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
                .minBucketRedistributionIntervalInSeconds(Config.MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS)
                .build();
    }
}
