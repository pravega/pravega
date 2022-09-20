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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * ControllerServiceStarter tests.
 */
@Slf4j
public abstract class ControllerServiceStarterTest {
    protected StoreClientConfig storeClientConfig;
    protected StoreClient storeClient;
    protected final int grpcPort;
    protected final int restPort;
    protected ScheduledExecutorService executor;
    private final boolean disableControllerCluster;
    private final boolean enableAuth;

    ControllerServiceStarterTest(final boolean disableControllerCluster, boolean enableAuth) {
        this.disableControllerCluster = disableControllerCluster;
        this.enableAuth = enableAuth;
        this.grpcPort = TestUtils.getAvailableListenPort();
        this.restPort = TestUtils.getAvailableListenPort();
    }

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

    @Test(timeout = 30000)
    public void testStartStop() throws URISyntaxException {
        Assert.assertNotNull(storeClient);
        @Cleanup
        ControllerServiceStarter starter = new ControllerServiceStarter(createControllerServiceConfig(), storeClient,
                SegmentHelperMock.getSegmentHelperMockForTables(executor));
        starter.startAsync();
        starter.awaitRunning();

        // Now, that starter has started, perform some rpc operations.
        URI uri = new URI( (enableAuth ? "tls" : "tcp") + "://localhost:" + grpcPort);

        final String testScope = "testScope";
        StreamManager streamManager = new StreamManagerImpl(
                ClientConfig.builder().controllerURI(uri)
                                      .credentials(new DefaultCredentials(
                                              SecurityConfigDefaults.AUTH_ADMIN_PASSWORD, SecurityConfigDefaults.AUTH_ADMIN_USERNAME))
                                      .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                            .build());

        streamManager.createScope(testScope);
        streamManager.deleteScope(testScope);
        streamManager.close();

        starter.stopAsync();
        starter.awaitTerminated();
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
                .grpcServerConfig(Optional.of(GRPCServerConfigImpl.builder()
                                                                  .port(grpcPort)
                                                                  .authorizationEnabled(enableAuth)
                                                                  .tlsEnabled(enableAuth)
                                                                  .tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                                                                  .tlsCertFile(SecurityConfigDefaults.TLS_SERVER_CERT_PATH)
                                                                  .tlsKeyFile(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH)
                                                                  .userPasswordFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                                                                  .build()))
                .restServerConfig(Optional.of(RESTServerConfigImpl.builder()
                        .port(restPort)
                        .host("localhost")
                        .authorizationEnabled(enableAuth)
                        .userPasswordFile(SecurityConfigDefaults.AUTH_HANDLER_INPUT_PATH)
                        .build()))
                .minBucketRedistributionIntervalInSeconds(10)
                .build();
    }
}
