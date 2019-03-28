/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.ZKHostStore;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Host store tests.
 */
@Slf4j
public class HostStoreTest {

    private final String host = "localhost";
    private final int controllerPort = 9090;
    private final int containerCount = 4;

    @Test
    public void inMemoryStoreTests() {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(host, controllerPort, containerCount))
                .hostMonitorMinRebalanceInterval(10)
                .containerCount(containerCount)
                .build();

        // Create a host store
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(hostMonitorConfig);

        validateStore(hostStore);

        // Create a host store via other factory method
        hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createInMemoryStoreClient());

        validateStore(hostStore);
    }

    @Test(timeout = 10000L)
    public void zkHostStoreTests() {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServerStarter().start();

            ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkTestServer.getConnectString())
                    .initialSleepInterval(2000)
                    .maxRetries(1)
                    .sessionTimeoutMs(10 * 1000)
                    .namespace("hostStoreTest/" + UUID.randomUUID())
                    .build();
            StoreClientConfig storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);

            @Cleanup
            StoreClient storeClient = StoreClientFactory.createStoreClient(storeClientConfig);

            HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                    .hostMonitorEnabled(true)
                    .hostMonitorMinRebalanceInterval(10)
                    .containerCount(containerCount)
                    .build();

            // Create ZK based host store.
            HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, storeClient);

            CompletableFuture<Void> latch = new CompletableFuture<>();
            ((ZKHostStore) hostStore).addListener(() -> {
                latch.complete(null);
            });
            // Update host store map.
            hostStore.updateHostContainersMap(HostMonitorConfigImpl.getHostContainerMap(host, controllerPort, containerCount));
            latch.join();
            validateStore(hostStore);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail();
        }
    }

    private void validateStore(HostControllerStore hostStore) {
        // Validate store values.
        Assert.assertEquals(containerCount, hostStore.getContainerCount());
        Host hostObj = hostStore.getHostForSegment("dummyScope", "dummyStream",
                (int) Math.floor(containerCount * Math.random()));
        Assert.assertEquals(controllerPort, hostObj.getPort());
        Assert.assertEquals(host, hostObj.getIpAddr());
    }
}
