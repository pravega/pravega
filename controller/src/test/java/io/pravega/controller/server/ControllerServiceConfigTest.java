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
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ControllerServiceConfig.
 */
public class ControllerServiceConfigTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Test
    public void configTests() {
        // Config parameters should be initialized, default values of the type are not allowed.
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().build());

        // Published host/port can be empty.
        Assert.assertNotNull(GRPCServerConfigImpl.builder().port(10).build());

        // Published host cannot be empty.
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().publishedRPCHost("").port(10).build());

        // Port should be positive integer.
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> GRPCServerConfigImpl.builder().port(-10).build());

        // Published port should be a positive integer.
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
                () -> TimeoutServiceConfig.builder().maxLeaseValue(-10).build());

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(10)
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

        // zk session timeout should be positive number
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ZKClientConfigImpl.builder().connectionString("localhost").namespace("test")
                        .sessionTimeoutMs(-10).namespace("").build());

        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withInMemoryClient();

        // If eventProcessor config is enabled, it should be non-null
        AssertExtensions.assertThrows(NullPointerException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .threadPoolSize(15)
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
                        .threadPoolSize(15)
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
                        .threadPoolSize(15)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.empty())
                        .grpcServerConfig(Optional.empty())
                        .restServerConfig(Optional.of(null))
                        .build());

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ControllerServiceConfigImpl.builder()
                        .threadPoolSize(15)
                        .storeClientConfig(storeClientConfig)
                        .hostMonitorConfig(hostMonitorConfig)
                        .controllerClusterListenerEnabled(true)
                        .timeoutServiceConfig(timeoutServiceConfig)
                        .eventProcessorConfig(Optional.empty())
                        .grpcServerConfig(Optional.empty())
                        .restServerConfig(Optional.empty())
                        .build());

        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> ControllerServiceConfigImpl.builder()
                                                 .threadPoolSize(15)
                                                 .storeClientConfig(storeClientConfig)
                                                 .hostMonitorConfig(hostMonitorConfig)
                                                 .controllerClusterListenerEnabled(true)
                                                 .timeoutServiceConfig(timeoutServiceConfig)
                                                 .eventProcessorConfig(Optional.empty())
                                                 .grpcServerConfig(Optional.empty())
                                                 .restServerConfig(Optional.empty())
                                                 .minBucketRedistributionIntervalInSeconds(0)
                                                 .build());
    }
}
