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

import com.sun.management.HotSpotDiagnosticMXBean;
import io.pravega.segmentstore.server.host.health.SegmentContainerRegistryHealthContributor;
import io.pravega.segmentstore.server.host.health.ZKHealthContributor;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestingServerStarter;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test the functionality of ServiceStarter used to setup segment store.
 */
@RunWith(SerializedClassRunner.class)
public class ServiceStarterTest {

    private ServiceStarter serviceStarter;
    private TestingServer zkTestServer;
    private ServiceBuilderConfig.Builder configBuilder;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        String zkUrl = zkTestServer.getConnectString();
        configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.ZK_URL, zkUrl)
                        .with(ServiceConfig.HEALTH_CHECK_INTERVAL_SECONDS, 2)
                    );
        serviceStarter = new ServiceStarter(configBuilder.build());
        serviceStarter.start();
    }

    @After
    public void stopZookeeper() throws Exception {
        serviceStarter.shutdown();
        zkTestServer.close();
    }

    /**
     * Check that the client created by ServiceStarter can correctly connect to a Zookeeper server using the custom
     * Zookeeper client factory.
     *
     * @throws Exception
     */
    @Test
    public void testCuratorClientCreation() throws Exception {
        @Cleanup
        CuratorFramework zkClient = serviceStarter.createZKClient();
        zkClient.blockUntilConnected();
        @Cleanup
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
     * Check health of SegmentContainerRegistry
     */
    @Test
    public void testSegmentContainerRegistryHealth() {
        @Cleanup
        SegmentContainerRegistryHealthContributor segmentContainerRegistryHealthContributor = new SegmentContainerRegistryHealthContributor(serviceStarter.getServiceBuilder()
                                                                                                                                                          .getSegmentContainerRegistry());
        Health.HealthBuilder builder = Health.builder().name(segmentContainerRegistryHealthContributor.getName());
        Status status = segmentContainerRegistryHealthContributor.doHealthCheck(builder);
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, status);
    }

    /**
     * Check the health status of ServiceStarter
     *
     */
    @Test
    public void testHealth() {
        Health health = serviceStarter.getHealthServiceManager().getHealthSnapshot();
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    /**
     * Test for validating the SS memory settings config
     */
    @Test
    public void testMemoryConfig() {
        //cache more than JVM MaxDirectMemory
        AssertExtensions.assertThrows("Exception to be thrown for Cache size greater than JVM MaxDirectMemory",
                () -> ServiceStarter.validateConfig(3013872542L, 1013872542L, 2013872542L, 8013872542L),
                e -> e instanceof IllegalStateException);

        //MaxDirectMem + Xmx > System Memory
        AssertExtensions.assertThrows("Exception to be thrown for MaxDirectMemory + Xmx being greater than System memory.",
                () -> ServiceStarter.validateConfig(3013872542L, 2013872542L, 7013872542L, 8013872542L),
                e -> e instanceof IllegalStateException);

        //must not throw exception
        ServiceStarter.validateConfig(3013872542L, 1013872542L, 5013872542L, 8013872542L);

        //testing the parent config method.
        long xmx = Runtime.getRuntime().maxMemory();
        Properties props = new Properties();
        props.setProperty(ServiceConfig.COMPONENT_CODE + "." + ServiceConfig.CACHE_POLICY_MAX_SIZE.getName(), String.valueOf(1013872542L + xmx));
        AssertExtensions.assertThrows("Exception to be thrown for Cache size greater than JVM MaxDirectMemory",
                () -> ServiceStarter.validateConfig(this.configBuilder.include(props).build()),
                e -> e instanceof IllegalStateException);

        long maxDirectMemorySize = Long.parseLong(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                                                                   .getVMOption("MaxDirectMemorySize").getValue());
        maxDirectMemorySize = maxDirectMemorySize == 0 ? xmx : maxDirectMemorySize;
        props.setProperty(ServiceConfig.COMPONENT_CODE + "." + ServiceConfig.CACHE_POLICY_MAX_SIZE.getName(), String.valueOf(maxDirectMemorySize - 100000));
        // must not throw exception . cache < JVM DM
        ServiceStarter.validateConfig(this.configBuilder.include(props).build());
    }
}
