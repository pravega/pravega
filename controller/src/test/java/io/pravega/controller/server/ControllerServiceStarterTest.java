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

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
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
    private final boolean disableControllerCluster;
    private final int grpcPort;
    private final boolean enableAuth;

    ControllerServiceStarterTest(final boolean disableControllerCluster, boolean enableAuth) {
        this.disableControllerCluster = disableControllerCluster;
        this.enableAuth = enableAuth;
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
            uri = new URI( (enableAuth ? "tls" : "tcp") + "://localhost:" + grpcPort);
        } catch (URISyntaxException e) {
            log.error("Error creating controller URI", e);
            Assert.fail("Error creating controller URI");
            return;
        }

        final String testScope = "testScope";
        StreamManager streamManager = new StreamManagerImpl(ClientConfig.builder().controllerURI(uri)
                                                                        .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                                                        .trustStore("../config/cert.pem").build());

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

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
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
                                                                  .tlsCertFile("../config/cert.pem")
                                                                  .tlsKeyFile("../config/key.pem")
                                                                  .build()))
                .restServerConfig(Optional.empty())
                .build();
    }
}
