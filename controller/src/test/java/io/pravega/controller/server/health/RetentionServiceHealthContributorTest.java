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
import io.pravega.controller.server.bucket.PeriodicRetention;
import io.pravega.controller.server.bucket.ZooKeeperBucketManager;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class RetentionServiceHealthContributorTest {
    private BucketManager retentionService;
    private RetentionServiceHealthContributor contributor;
    private Health.HealthBuilder builder;
    @Before
    public void setup() {
        BucketStore bucketStore = mock(ZookeeperBucketStore.class);
        doReturn(StoreType.Zookeeper).when(bucketStore).getStoreType();
        String hostId = UUID.randomUUID().toString();
        BucketServiceFactory bucketStoreFactory = spy(new BucketServiceFactory(hostId, bucketStore, 2));
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        PeriodicRetention periodicRetention = mock(PeriodicRetention.class);
        retentionService = spy(bucketStoreFactory.createWatermarkingService(Duration.ofMillis(5), periodicRetention::retention, executor));
        doReturn(CompletableFuture.completedFuture(null)).when((ZooKeeperBucketManager) retentionService).initializeService();
        doNothing().when((ZooKeeperBucketManager) retentionService).startBucketOwnershipListener();
        doReturn(true).when((ZooKeeperBucketManager) retentionService).isZKConnected();

        contributor = new RetentionServiceHealthContributor("retentionservice", retentionService);
        builder = Health.builder().name("retentionservice");
    }

    @Test
    public void testHealthCheck() throws Exception {
        retentionService.startAsync();
        TimeUnit.SECONDS.sleep(5);
        Status status = contributor.doHealthCheck(builder);
        Assert.assertTrue(status == Status.UP);
        retentionService.stopAsync();
        TimeUnit.SECONDS.sleep(5);
        status = contributor.doHealthCheck(builder);
        Assert.assertTrue(status == Status.DOWN);
    }
}
