package com.emc.pravega.controller.server;

import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import com.emc.pravega.controller.server.impl.ControllerServiceConfigImpl;
import com.emc.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import com.emc.pravega.testcommon.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * ControllerServiceStarter tests.
 */
@Slf4j
public abstract class ControllerServiceStarterTest {
    protected StoreClientConfig storeClientConfig;
    protected StoreClient storeClient;
    private final boolean disableControllerCluster;
    private final int grpcPort;

    ControllerServiceStarterTest(final boolean disableControllerCluster) {
        this.disableControllerCluster = disableControllerCluster;
        this.grpcPort = TestUtils.getAvailableListenPort();
    }

    @Before
    public abstract void setup();

    @After
    public abstract void tearDown();

    @Test
    public void testStartStop() {
        Assert.assertNotNull(storeClient);
        ControllerServiceStarter starter = new ControllerServiceStarter(createControllerServiceConfig(), storeClient);
        starter.startAsync();

        try {
            starter.awaitRunning();
        } catch (IllegalStateException e) {
            log.error("Error awaiting starter to get ready");
            Assert.fail("Error awaiting starter to get ready");
        }

        // Now, that starter has started, perform some rpc operations.
        URI uri;
        try {
            uri = new URI("tcp://localhost:" + grpcPort);
        } catch (URISyntaxException e) {
            log.error("Error creating controller URI", e);
            Assert.fail("Error creating controller URI");
            return;
        }

        final String testScope = "testScope";
        StreamManager streamManager = new StreamManagerImpl(uri);
        streamManager.createScope(testScope);
        streamManager.deleteScope(testScope);
        streamManager.close();

        starter.stopAsync();
        try {
            starter.awaitTerminated();
        } catch (IllegalStateException e) {
            log.error("Error awaiting termination of starter");
            Assert.fail("Error awaiting termination of starter");
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
                .grpcServerConfig(Optional.of(GRPCServerConfigImpl.builder().port(grpcPort).build()))
                .restServerConfig(Optional.empty())
                .build();
    }
}
