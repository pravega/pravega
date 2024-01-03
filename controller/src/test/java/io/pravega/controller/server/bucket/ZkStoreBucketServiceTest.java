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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.test.common.TestingServerStarter;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZkStoreBucketServiceTest extends BucketServiceTest {
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
        return StreamStoreFactory.createZKStore(zkClient, executor);
    }

    @Override
    BucketStore createBucketStore(int bucketCount) {
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, bucketCount,
                BucketStore.ServiceType.WatermarkingService, bucketCount);

        return StreamStoreFactory.createZKBucketStore(map, zkClient, executor);
    }

    @Test(timeout = 10000)
    public void testBucketOwnership() throws Exception {
        addEntryToZkCluster(controller);
        assertEventuallyEquals(3, () -> retentionService.getBucketServices().size(), 3000);

        // Introduce connection failure error
        zkClient.getZookeeperClient().close();

        // restart
        CuratorFramework zkClient2 = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);
        zkClient2.start();
        BucketStore bucketStore2 = StreamStoreFactory.createZKBucketStore(zkClient2, executor);
        String scope = "scope1";
        String streamName = "stream1";
        bucketStore2.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, streamName, executor).join();
        zkClient2.close();

        zkClient.getZookeeperClient().start();

        Stream stream = new StreamImpl(scope, streamName);

        // verify that at least one of the buckets got the notification
        Map<Integer, BucketService> bucketServices = retentionService.getBucketServices();

        int bucketId = BucketStore.getBucket(scope, streamName, 3);
        BucketService bucketService = bucketServices.get(bucketId);
        AtomicBoolean added = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !added.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> added.set(bucketService.getKnownStreams().size() > 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getKnownStreams().contains(stream));
    }

    @Test(timeout = 60000)
    public void testOwnershipOfExistingBucket() throws Exception {
        RequestTracker requestTracker = new RequestTracker(true);
        TestingServer zkServer2 = new TestingServerStarter().start();
        zkServer2.start();
        CuratorFramework zkClient2 = CuratorFrameworkFactory.newClient(zkServer2.getConnectString(), 10000, 1000,
                (r, e, s) -> false);
        zkClient2.start();

        @Cleanup("shutdownNow")
        ScheduledExecutorService executor2 = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
        String hostId = UUID.randomUUID().toString();
        
        BucketStore bucketStore2 = StreamStoreFactory.createZKBucketStore(ImmutableMap.of(BucketStore.ServiceType.RetentionService, 1), zkClient2, executor2);
        StreamMetadataStore streamMetadataStore2 = StreamStoreFactory.createZKStore(zkClient2, executor2);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor2);

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        StreamMetadataTasks streamMetadataTasks2 = new StreamMetadataTasks(streamMetadataStore2, bucketStore2, 
                taskMetadataStore, segmentHelper, executor2, hostId, GrpcAuthHelper.getDisabledAuthHelper());

        String scope = "scope1";
        String streamName = "stream1";
        bucketStore2.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, streamName, executor2).join();

        String scope2 = "scope2";
        String streamName2 = "stream2";
        bucketStore2.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope2, streamName2, executor2).join();

        BucketServiceFactory bucketStoreFactory = new BucketServiceFactory(hostId, bucketStore2, 5, 1);

        BucketManager service2 = bucketStoreFactory.createRetentionService(Duration.ofMillis(5000), stream -> CompletableFuture.completedFuture(null), executor2);
        service2.startAsync();
        service2.awaitRunning();

        Thread.sleep(10000);
        assertTrue(service2.getBucketServices().values().stream().allMatch(x -> x.getKnownStreams().size() == 2));

        service2.stopAsync();
        service2.awaitTerminated();
        zkClient2.close();
        zkServer2.close();
        streamMetadataTasks2.close();
        ExecutorServiceHelpers.shutdown(executor2);
    }

    @Test(timeout = 30000)
    public void testMaxConcurrentJobs() {
        String scope = "scope";
        String stream1 = "stream1";
        String stream2 = "stream2";
        String stream3 = "stream3";
        String stream4 = "stream4";

        ConcurrentHashMap<String, CompletableFuture<Void>> streamWorkLatch = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, CompletableFuture<Void>> streamWorkFuture = new ConcurrentHashMap<>();

        streamWorkFuture.put(stream1, new CompletableFuture<>());
        streamWorkFuture.put(stream2, new CompletableFuture<>());
        streamWorkFuture.put(stream3, new CompletableFuture<>());
        streamWorkFuture.put(stream4, new CompletableFuture<>());
        streamWorkLatch.put(stream1, new CompletableFuture<>());
        streamWorkLatch.put(stream2, new CompletableFuture<>());
        streamWorkLatch.put(stream3, new CompletableFuture<>());
        streamWorkLatch.put(stream4, new CompletableFuture<>());
        
        BucketWork bucketWork = x -> {
            // indicate that the work has been called
            streamWorkLatch.get(x.getStreamName()).complete(null);
            return streamWorkFuture.get(x.getStreamName());
        };
        
        BucketService service = new ZooKeeperBucketService(BucketStore.ServiceType.RetentionService,
                0, (ZookeeperBucketStore) bucketStore, executor, 2, Duration.ofSeconds(2),
                bucketWork);

        service.startAsync();
        service.awaitRunning();
        
        // verify that we add a new stream and it gets added
        // send notification for stream1 and verify that it's work gets called  
        BucketService.StreamNotification notification = new BucketService.StreamNotification(scope, stream1, BucketService.NotificationType.StreamAdded);
        service.notify(notification);
        AtomicBoolean added = new AtomicBoolean(false);
        Futures.loop(added::get, () -> {
            added.set(service.getKnownStreams().stream().anyMatch(x -> x.getStreamName().equals(stream1)));
            return CompletableFuture.completedFuture(null);
        }, executor).join();

        // wait for stream work to be called
        streamWorkLatch.get(stream1).join();

        // complete stream 1 work 
        streamWorkFuture.get(stream1).complete(null);
        
        Set<Stream> streams = service.getKnownStreams();
        assertEquals(streams.size(), 1);
        
        Stream stream1obj = streams.iterator().next();
        
        // now send notification to remove the stream 
        notification = new BucketService.StreamNotification(scope, stream1, BucketService.NotificationType.StreamRemoved);
        service.notify(notification);

        // whenever notification loop kicks in, it should remove the stream from known streams and worker queue.
        AtomicBoolean removed = new AtomicBoolean(false);
        Futures.loop(removed::get, () -> {
            removed.set(!service.getKnownStreams().contains(stream1obj));
            return CompletableFuture.completedFuture(null);
        }, executor).join();

        // the work should also be removed from worker queue
        Futures.loop(removed::get, () -> {
            removed.set(service.getWorkerQueue().stream().noneMatch(x -> x.getStream().equals(stream1obj)));
            return CompletableFuture.completedFuture(null);
        }, executor).join();

        // send notification for addition of stream2, stream3 
        notification = new BucketService.StreamNotification(scope, stream2, BucketService.NotificationType.StreamAdded);
        service.notify(notification);
        notification = new BucketService.StreamNotification(scope, stream3, BucketService.NotificationType.StreamAdded);
        service.notify(notification);

        // wait for two "work" to be called.. wait on those latches to complete
        streamWorkLatch.get(stream2).join();
        streamWorkLatch.get(stream3).join();

        assertEquals(service.getWorkerQueue().size(), 0);
        
        // send notification for addition of stream4
        notification = new BucketService.StreamNotification(scope, stream4, BucketService.NotificationType.StreamAdded);
        service.notify(notification);

        // its work should not get called!
        Collection<BucketService.QueueElement> workerQueueAfterDelay = Futures.delayedFuture(
                () -> CompletableFuture.completedFuture(service.getWorkerQueue()), 5000, executor).join();
        assertFalse(streamWorkLatch.get(stream4).isDone());
        // worker queue should still have the element for stream4 waiting
        assertEquals(workerQueueAfterDelay.size(), 1);
        assertEquals(workerQueueAfterDelay.iterator().next().getStream().getStreamName(), stream4);
        
        // finish one of the work and we shall have worker queue pick up work from stream4
        streamWorkFuture.get(stream2).complete(null);

        // stream 4's work should be called and completed
        streamWorkLatch.get(stream4).join();
    }

    @Override
    protected void addEntryToZkCluster(Host host)  {
        addControllerToZkCluster(host, cluster);
    }

}
