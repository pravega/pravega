/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.client.impl.StoreClientConfigImpl;
import com.emc.pravega.controller.store.client.impl.ZKClientConfigImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Host store tests.
 */
@Slf4j
public class HostStoreTest {

    private final String host = "localhost";
    private final int port = 9090;
    private final int containerCount = 4;

    @Test
    public void inMemoryStoreTests() {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(false)
                .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(host, port, containerCount))
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

    @Test
    public void zkHostStoreTests() {
        try {
            @Cleanup
            TestingServer zkTestServer = new TestingServer();

            ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder().connectionString(zkTestServer.getConnectString())
                    .initialSleepInterval(2000)
                    .maxRetries(1)
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

            // Update host store map.
            hostStore.updateHostContainersMap(HostMonitorConfigImpl.getHostContainerMap(host, port, containerCount));

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
        Assert.assertEquals(port, hostObj.getPort());
        Assert.assertEquals(host, hostObj.getIpAddr());
    }
}
