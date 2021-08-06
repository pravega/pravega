package io.pravega.controller.server.health;

import io.pravega.controller.server.bucket.*;
import io.pravega.controller.store.client.StoreType;
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
import static org.mockito.Mockito.spy;

/**
 * Unit tests for WatermarkingServiceHealthContibutor
 */
public  class WatermarkingServiceHealthContibutorTest {
    private BucketManager watermarkingService;
    private WatermarkingServiceHealthContributor contributor;
    private Health.HealthBuilder builder;
    @Before
    public void setup() {
        ZookeeperBucketStore bucketStore = mock(ZookeeperBucketStore.class);
        doReturn(StoreType.Zookeeper).when(bucketStore).getStoreType();
        String hostId = UUID.randomUUID().toString();
        BucketServiceFactory bucketStoreFactory = new BucketServiceFactory(hostId, bucketStore, 2);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        PeriodicWatermarking periodicWatermarking = mock(PeriodicWatermarking.class);
        watermarkingService = spy(bucketStoreFactory.createWatermarkingService(Duration.ofMillis(10), periodicWatermarking::watermark, executor));
        doReturn(CompletableFuture.completedFuture(null)).when((ZooKeeperBucketManager)watermarkingService).initializeService();
        doNothing().when((ZooKeeperBucketManager)watermarkingService).startBucketOwnershipListener();
        doReturn(true).when((ZooKeeperBucketManager)watermarkingService).isZKConnected();
        contributor = new WatermarkingServiceHealthContributor("watermarkingservice", watermarkingService);
        builder = Health.builder().name("watermark");
    }
    @Test
    public void testHealthCheck() throws Exception {
        watermarkingService.startAsync();
        TimeUnit.SECONDS.sleep(5);
        Assert.assertTrue(watermarkingService.isRunning());
        Status status = contributor.doHealthCheck(builder);
        Assert.assertTrue(status == Status.UP);
        watermarkingService.stopAsync();
        TimeUnit.SECONDS.sleep(5);
        status = contributor.doHealthCheck(builder);
        Assert.assertTrue(status == Status.DOWN);
    }
}
