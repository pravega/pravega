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
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.host.health.ZKHealthContributor;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.test.common.SerializedClassRunner;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(SerializedClassRunner.class)
public class ServiceStarterTest {

    private ServiceStarter serviceStarter;
    private ZooKeeperServiceRunner zkService;

    @Before
    public void setup() throws Exception {
        zkService = new ZooKeeperServiceRunner(4000, false, null, null, null);
        zkService.initialize();
        zkService.start();
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.ZK_URL, "localhost:4000")
                        .with(ServiceConfig.HEALTH_CHECK_INTERVAL_SECONDS, 10)
                    );
        serviceStarter = new ServiceStarter(configBuilder.build());
        serviceStarter.start();
    }

    @After
    public void stopZookeeper() throws Exception {
        serviceStarter.shutdown();
        zkService.close();
    }

    /**
     * Check that the client created by ServiceStarter can correctly connect to a Zookeeper server using the custom
     * Zookeeper client factory.
     *
     * @throws Exception
     */
    @Test
    public void testCuratorClientCreation() throws Exception {
        CuratorFramework zkClient = serviceStarter.createZKClient();
        zkClient.blockUntilConnected();
        ZKHealthContributor zkHealthContributor = new ZKHealthContributor(zkClient);
        Health.HealthBuilder builder = Health.builder().name(zkHealthContributor.getName());
        Status zkStatus = zkHealthContributor.doHealthCheck(builder);
        Assert.assertTrue(zkClient.getZookeeperClient().isConnected());
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, zkStatus);
        zkClient.close();
        zkStatus = zkHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'DOWN' Status.", Status.DOWN, zkStatus);
    }

    /**
     * Check the health status of ServiceStarter
     *
     */
    @Test
    public void testHealth() {
        Health health = serviceStarter.healthServiceManager.getHealthSnapshot();
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, health.getStatus());
    }
}
