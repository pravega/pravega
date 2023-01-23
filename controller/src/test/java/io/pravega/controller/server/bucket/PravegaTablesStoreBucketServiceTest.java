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
package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class PravegaTablesStoreBucketServiceTest extends BucketServiceTest {
    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private Cluster cluster;

    @Override
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();
        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        super.setup();
    }

    @Override
    protected void addEntryToZkCluster(Host host) {
        addControllerToZkCluster(host, cluster);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        cluster.close();
        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }

    @Override
    StreamMetadataStore createStreamStore(ScheduledExecutorService executor) {
        return StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor), 
                                                           GrpcAuthHelper.getDisabledAuthHelper(), zkClient, executor);
    }

    @Override
    BucketStore createBucketStore(int bucketCount) {
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, bucketCount,
                BucketStore.ServiceType.WatermarkingService, bucketCount);
        return StreamStoreFactory.createZKBucketStore(map, zkClient, executor);
    }



    @Test(timeout = 30000)
    public void testFailover() throws Exception {
        addEntryToZkCluster(controller);
        assertEventuallyEquals(3, () -> watermarkingService.getBucketServices().size(), 3000);
        assertEventuallyEquals(3, () -> retentionService.getBucketServices().size(), 3000);

        //make zookeeper server restart, it will test connection suspended.
        zkServer.restart();

        //add new controller instance in pravgea cluster.
        Host controller1 = new Host(UUID.randomUUID().toString(), 9090, null);
        addEntryToZkCluster(controller1);
        assertEventuallyEquals(2, () -> retentionService.getBucketServices().size(), 3000);
        assertEventuallyEquals(2, () -> watermarkingService.getBucketServices().size(), 3000);

        List<Integer> retentionBuckets = IntStream.range(0, 3).filter(x ->
                !retentionService.getBucketServices().keySet().contains(x)).boxed().collect(Collectors.toList());
        List<Integer> watermarkBuckets = IntStream.range(0, 3).filter(x ->
                !retentionService.getBucketServices().keySet().contains(x)).boxed().collect(Collectors.toList());

        assertTrue(retentionService.takeBucketOwnership(retentionBuckets.get(0), controller1.getHostId(), executor).join());
        assertTrue(watermarkingService.takeBucketOwnership(watermarkBuckets.get(0), controller1.getHostId(), executor).join());

        //add new controller instance in pravgea cluster.
        Host controller2 = new Host(UUID.randomUUID().toString(), 9090, null);
        addEntryToZkCluster(controller2);
        assertEventuallyEquals(1, () -> retentionService.getBucketServices().size(), 3000);
        assertEventuallyEquals(1, () -> watermarkingService.getBucketServices().size(), 3000);

        //remove controller instances from pravega cluster.
        removeControllerFromZkCluster(controller1, cluster);
        removeControllerFromZkCluster(controller2, cluster);

        //controller1 didn't release bucket 0 till now. So it will not start it.
        assertEventuallyEquals(2, () -> retentionService.getBucketServices().size(), 3000);
        assertEventuallyEquals(2, () -> watermarkingService.getBucketServices().size(), 3000);

        //controller1 release buckets here.
        assertTrue(retentionService.releaseBucketOwnership(retentionBuckets.get(0), controller1.getHostId()).join());
        assertTrue(watermarkingService.releaseBucketOwnership(watermarkBuckets.get(0), controller1.getHostId()).join());

        //controller1 release bucket 0 now. So it will start it.
        assertEventuallyEquals(3, () -> retentionService.getBucketServices().size(), 3000);
        assertEventuallyEquals(3, () -> watermarkingService.getBucketServices().size(), 3000);
    }

    @Test(timeout = 30000)
    public void testOwnership() throws Exception {
        String dummyProcessId = "12345";
        watermarkingService.takeBucketOwnership(0, dummyProcessId, executor).join();
        watermarkingService.takeBucketOwnership(1, dummyProcessId, executor).join();
        watermarkingService.takeBucketOwnership(2, dummyProcessId, executor).join();

        addEntryToZkCluster(controller);
        //Verify new controller will start the service, until previous one doesn't release it.
        Map<Integer, BucketService> bucketServices = watermarkingService.getBucketServices();
        assertNotNull(bucketServices);
        assertEquals(0, bucketServices.size());
        //Verify once older controller release the ownership, newer will acquire it.
        assertTrue(watermarkingService.releaseBucketOwnership(0, dummyProcessId).join());
        assertEventuallyEquals(1, () -> watermarkingService.getBucketServices().size(), 10000);
        bucketServices = watermarkingService.getBucketServices();
        assertNotNull(bucketServices);
        assertEquals(1, bucketServices.size());

        assertTrue(watermarkingService.releaseBucketOwnership(1, dummyProcessId).join());
        //For id which is not existing, it will return as true.
        assertTrue(watermarkingService.releaseBucketOwnership(1, dummyProcessId).join());
        assertTrue(watermarkingService.releaseBucketOwnership(2, dummyProcessId).join());
        //All the bucket services get released from dummy process. Now actual owner will occupy this.
        assertEventuallyEquals(3, () -> watermarkingService.getBucketServices().size(), 10000);
        bucketServices = watermarkingService.getBucketServices();
        assertNotNull(bucketServices);
        assertEquals(3, bucketServices.size());
        //Verifying only owning controller can release the buckets.
        assertFalse(watermarkingService.releaseBucketOwnership(1, dummyProcessId).join());
    }

    @Test(timeout = 30000)
    public void testFailureCase() throws Exception {
        BucketStore spyBucketStore = spy(bucketStore);
        doReturn(CompletableFuture.completedFuture(false))
                .when((ZookeeperBucketStore) spyBucketStore)
                .releaseBucketOwnership(BucketStore.ServiceType.WatermarkingService, 0, hostId);

        BucketService bucketService = spy(new ZooKeeperBucketService(BucketStore.ServiceType.WatermarkingService,
                2, (ZookeeperBucketStore) spyBucketStore, executor, 2,
                Duration.ofMillis(5), periodicWatermarking::watermark));

        BucketService bucketService1 = spy(new ZooKeeperBucketService(BucketStore.ServiceType.WatermarkingService,
                1, (ZookeeperBucketStore) spyBucketStore, executor, 2,
                Duration.ofMillis(5), periodicWatermarking::watermark));

        doThrow(new RuntimeException("Service start failed.")).when(bucketService).doStart();

        doThrow(new RuntimeException("Service stop failed.")).when(bucketService1).doStop();

        Function<Integer, BucketService> zkSupplier = bucket -> bucket == 0 ?
                new ZooKeeperBucketService(BucketStore.ServiceType.WatermarkingService,
                bucket, (ZookeeperBucketStore) spyBucketStore, executor, 2,
                Duration.ofMillis(5), periodicWatermarking::watermark)
                : bucket == 1 ? bucketService1 : bucketService;

        BucketManager bucketManager = new ZooKeeperBucketManager(hostId, (ZookeeperBucketStore) spyBucketStore,
                BucketStore.ServiceType.WatermarkingService, executor, zkSupplier,
                getBucketManagerLeader(spyBucketStore, BucketStore.ServiceType.WatermarkingService));
        bucketManager.startAsync();
        bucketManager.awaitRunning();
        // Bucket 2 will not be able to start as dostart() is throwing RunTimeException. So in this case buckets which
        // will be in running states are [0,1].
        // Start bucket service 0.
        bucketManager.tryTakeOwnership(0).join();
        assertEquals(1, bucketManager.getBucketServices().size());
        // Start bucket service 1.
        bucketManager.tryTakeOwnership(1).join();
        assertEquals(2, bucketManager.getBucketServices().size());
        // Try to start bucket service 2, it should give exception.
        AssertExtensions.assertThrows("Unable to start bucket service.",
                () -> bucketManager.tryTakeOwnership(2).join(), e -> e instanceof RuntimeException);
        assertEquals(2,  bucketManager.getBucketServices().size());
        // Try to stop bucket service 1, it should give exception.
        bucketManager.stopBucketServices(Set.of(1), true);
        assertEquals(2,  bucketManager.getBucketServices().size());
        // Unable to release bucket ownership, service should continue on same controller instance.
        bucketManager.stopBucketServices(Set.of(0), true);
        assertEventuallyEquals(2, () -> bucketManager.getBucketServices().size(), 10000);

        // Throw exception while releasing the ownership of bucket service.
        CompletableFuture<Boolean> completableFuture = new CompletableFuture();
        completableFuture.completeExceptionally(new RuntimeException("Exception while releasing ownership."));
        doReturn(completableFuture).when((ZookeeperBucketStore) spyBucketStore)
                .releaseBucketOwnership(BucketStore.ServiceType.WatermarkingService, 0, hostId);
        bucketManager.stopBucketServices(Set.of(0), false);
        assertEventuallyEquals(2, () -> bucketManager.getBucketServices().size(), 10000);
        bucketManager.stopAsync();
        AssertExtensions.assertThrows(IllegalStateException.class, () -> bucketManager.awaitTerminated());
    }

    private BucketManagerLeader getBucketManagerLeader(BucketStore bucketStore, BucketStore.ServiceType serviceType) {
        return new BucketManagerLeader(bucketStore, 1,
                new UniformBucketDistributor(), serviceType, executor);
    }
}
