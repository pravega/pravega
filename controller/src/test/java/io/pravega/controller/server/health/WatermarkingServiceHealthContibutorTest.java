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
package io.pravega.controller.server.health;

import io.pravega.controller.server.bucket.BucketManager;
import io.pravega.controller.server.bucket.BucketServiceFactory;
import io.pravega.controller.server.bucket.PeriodicWatermarking;
import io.pravega.controller.server.bucket.ZooKeeperBucketManager;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;

import io.pravega.test.common.TestingServerStarter;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for WatermarkingServiceHealthContibutor
 */
public class WatermarkingServiceHealthContibutorTest {
    private BucketManager watermarkingService;
    private WatermarkingServiceHealthContributor contributor;
    private Health.HealthBuilder builder;
    private TestingServer zkServer;
    private CuratorFramework zkClient;

    @SneakyThrows
    @Before
    public void setup() {
        ZookeeperBucketStore bucketStore = mock(ZookeeperBucketStore.class);
        doReturn(StoreType.Zookeeper).when(bucketStore).getStoreType();
        doReturn(getClient()).when(bucketStore).getClient();
        doReturn(CompletableFuture.completedFuture(Set.of())).when(bucketStore).getBucketsForController(anyString(), any());
        String hostId = UUID.randomUUID().toString();
        BucketServiceFactory bucketStoreFactory = new BucketServiceFactory(hostId, bucketStore, 2, 10);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        PeriodicWatermarking periodicWatermarking = mock(PeriodicWatermarking.class);
        watermarkingService = spy(bucketStoreFactory.createWatermarkingService(Duration.ofMillis(10), periodicWatermarking::watermark, executor));
        doReturn(CompletableFuture.completedFuture(null)).when((ZooKeeperBucketManager) watermarkingService).initializeService();
        doNothing().when((ZooKeeperBucketManager) watermarkingService).startBucketOwnershipListener();
        doNothing().when((ZooKeeperBucketManager) watermarkingService).startBucketControllerMapListener();
        doReturn(true).when(watermarkingService).isHealthy();
        contributor = new WatermarkingServiceHealthContributor("watermarkingservice", watermarkingService);
        builder = Health.builder().name("watermark");
    }

    @SneakyThrows
    @After
    public void tearDown() {
        zkServer.close();
        zkClient.close();
        contributor.close();
    }

    @Test
    public void testHealthCheck() throws Exception {
        watermarkingService.startAsync();
        Assert.assertTrue(watermarkingService.isRunning());
        Status status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.UP, status);
        watermarkingService.stopAsync();
        status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.DOWN, status);
    }

    private CuratorFramework getClient() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();

        return zkClient;
    }
}
