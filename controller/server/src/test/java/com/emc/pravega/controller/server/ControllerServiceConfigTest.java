/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import com.emc.pravega.controller.server.impl.ControllerServiceConfigImpl;
import com.emc.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import com.emc.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.testcommon.AssertExtensions;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ControllerServiceConfig.
 */
public class ControllerServiceConfigTest {

    @Test
    public void configTests() {
        // Config parameters should be initialized, default values of the type are not allowed.
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().build());

        AssertExtensions.assertThrows(NullPointerException.class,
                () -> GRPCServerConfigImpl.builder().publishedRPCHost(null).port(10).build());

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().publishedRPCHost("").port(10).build());

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().publishedRPCHost("localhost").port(10).build());

        // Port should be positive integer
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().port(-10).build());

        // Port should be positive integer
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().port(10).publishedRPCHost("localhost")
                        .publishedRPCPort(-10).build());

        // Config parameters should be initialized, default values of the type are not allowed.
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> RESTServerConfigImpl.builder().build());

        AssertExtensions.assertThrows(NullPointerException.class,
                () -> RESTServerConfigImpl.builder().host(null).port(10).build());

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> RESTServerConfigImpl.builder().host("").port(10).build());

        // Port should be positive integer
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> RESTServerConfigImpl.builder().host("localhost").port(-10).build());

        // Config parameters should be initialized, default values of the type are not allowed.
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> HostMonitorConfigImpl.builder().build());

        // If hostMonitorEnabled then containerMap should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> HostMonitorConfigImpl.builder()
                        .hostMonitorEnabled(false)
                        .hostContainerMap(null)
                        .containerCount(10)
                        .hostMonitorMinRebalanceInterval(10).build());

        // Port should be positive integer
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> HostMonitorConfigImpl.builder()
                        .hostMonitorEnabled(false)
                        .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap("host", 10, 2))
                        .hostMonitorMinRebalanceInterval(-10).build());

        // Port should be positive integer
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> HostMonitorConfigImpl.builder()
                        .hostMonitorEnabled(true)
                        .hostContainerMap(null)
                        .hostMonitorMinRebalanceInterval(-10).build());

        // Following combination is OK.
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostContainerMap(null)
                .containerCount(10)
                .hostMonitorMinRebalanceInterval(10).build();

        // Values should be specified
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TimeoutServiceConfig.builder().build());

        // Positive values required
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TimeoutServiceConfig.builder().maxLeaseValue(-10).maxScaleGracePeriod(10).build());

        // Positive values required
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TimeoutServiceConfig.builder().maxLeaseValue(10).maxScaleGracePeriod(-10).build());

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(10)
                .maxScaleGracePeriod(20)
                .build();

        AssertExtensions.assertThrows(NullPointerException.class,
                () -> StoreClientConfigImpl.withZKClient(null));

        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ZKClientConfigImpl.builder().connectionString(null).build());

        // Namespace should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ZKClientConfigImpl.builder().connectionString("localhost").build());

        // Sleep interval should be positive number
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ZKClientConfigImpl.builder().connectionString("localhost").namespace("test")
                        .initialSleepInterval(-10).build());

        // max retries should be positive number
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ZKClientConfigImpl.builder().connectionString("localhost").namespace("test")
                        .initialSleepInterval(10).maxRetries(-10).namespace("").build());

        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withInMemoryClient();

        // If eventProcessor config is enabled, it should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .serviceThreadPoolSize(3)
                        .taskThreadPoolSize(3)
                        .storeThreadPoolSize(3)
                        .eventProcThreadPoolSize(3)
                        .requestHandlerThreadPoolSize(3)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.of(null))
                        .grpcServerConfig(Optional.empty())
                        .restServerConfig(Optional.empty())
                        .build());

        // If grpcServerConfig is present it should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .serviceThreadPoolSize(3)
                        .taskThreadPoolSize(3)
                        .storeThreadPoolSize(3)
                        .eventProcThreadPoolSize(3)
                        .requestHandlerThreadPoolSize(3)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.empty())
                        .grpcServerConfig(Optional.of(null))
                        .restServerConfig(Optional.empty())
                        .build());

        // If restServerConfig is present it should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .serviceThreadPoolSize(3)
                        .taskThreadPoolSize(3)
                        .storeThreadPoolSize(3)
                        .eventProcThreadPoolSize(3)
                        .requestHandlerThreadPoolSize(3)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.empty())
                        .grpcServerConfig(Optional.empty())
                        .restServerConfig(Optional.of(null))
                        .build());

        // If ControllerClusterListener is present, storeClient should be ZK.
        ControllerClusterListenerConfig clusterListenerConfig =
                ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(8).build();

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .serviceThreadPoolSize(3)
                        .taskThreadPoolSize(3)
                        .storeThreadPoolSize(3)
                        .eventProcThreadPoolSize(3)
                        .requestHandlerThreadPoolSize(3)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .controllerClusterListenerConfig(Optional.of(clusterListenerConfig))
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.empty())
                        .grpcServerConfig(Optional.empty())
                        .restServerConfig(Optional.empty())
                        .build());
    }
}
