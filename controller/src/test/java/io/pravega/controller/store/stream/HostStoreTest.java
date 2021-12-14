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
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Host store tests.
 */
@Slf4j
public class HostStoreTest {

    private final String host = "localhost";
    private final int controllerPort = 9090;
    private final int containerCount = 4;

    @Test(timeout = 30000)
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
    public void zkHostStoreTests() throws Exception {
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

        // Update host store map.
        Map<Host, Set<Integer>> hostContainerMap = HostMonitorConfigImpl.getHostContainerMap(host, controllerPort, containerCount);

        // Create ZK based host store.
        HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, storeClient);
        CompletableFuture<Void> latch1 = new CompletableFuture<>();
        CompletableFuture<Void> latch2 = new CompletableFuture<>();
        ((ZKHostStore) hostStore).addListener(() -> {
            // With the addition of updateMap() in tryInit(), this listener is actually called twice, and we need to
            // wait for the second operation to complete (related to updateHostContainersMap()).
            if (latch1.isDone()) {
                latch2.complete(null);
            }
            latch1.complete(null);
        });
        hostStore.updateHostContainersMap(hostContainerMap);
        latch1.join();
        latch2.join();
        validateStore(hostStore);

        // verify that a new hostStore is initialized with map set by previous host store.
        HostControllerStore hostStore2 = HostStoreFactory.createStore(hostMonitorConfig, storeClient);

        Map<Host, Set<Integer>> map = hostStore2.getHostContainersMap();
        assertEquals(hostContainerMap, map);
    }

    private void validateStore(HostControllerStore hostStore) {
        // Validate store values.
        Assert.assertEquals(containerCount, hostStore.getContainerCount());
        Host hostObj = hostStore.getHostForSegment("dummyScope", "dummyStream",
                (int) Math.floor(containerCount * Math.random()));
        Assert.assertEquals(controllerPort, hostObj.getPort());
        Assert.assertEquals(host, hostObj.getIpAddr());

        hostObj = hostStore.getHostForTableSegment(NameUtils.getQualifiedTableName("scope", "stream", "table", "id"));
        Assert.assertEquals(controllerPort, hostObj.getPort());
        Assert.assertEquals(host, hostObj.getIpAddr());
    }
}
