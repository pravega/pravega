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

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class BucketServiceTest {
    StreamMetadataStore streamMetadataStore;
    BucketStore bucketStore;
    BucketManager retentionService;
    BucketManager watermarkingService;
    ScheduledExecutorService executor;
    StreamMetadataTasks streamMetadataTasks;
    private PeriodicWatermarking periodicWatermarking;
    private ConnectionFactory connectionFactory;
    private String hostId;
    private RequestTracker requestTracker = new RequestTracker(true);

    @Before
    public void setup() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
        hostId = UUID.randomUUID().toString();

        streamMetadataStore = createStreamStore(executor);
        bucketStore = createBucketStore(3);
        
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createInMemoryStore(executor);

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();

        streamMetadataTasks = new StreamMetadataTasks(streamMetadataStore, bucketStore, taskMetadataStore, 
                segmentHelper, executor, hostId, GrpcAuthHelper.getDisabledAuthHelper(), requestTracker);
        BucketServiceFactory bucketStoreFactory = new BucketServiceFactory(hostId, bucketStore, 2);
        PeriodicRetention periodicRetention = new PeriodicRetention(streamMetadataStore, streamMetadataTasks, executor, requestTracker);
        retentionService = bucketStoreFactory.createRetentionService(Duration.ofMillis(5), periodicRetention::retention, executor);
        retentionService.startAsync();
        retentionService.awaitRunning();
        
        ClientConfig clientConfig = ClientConfig.builder().build();
        periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, clientConfig, executor);
        watermarkingService = bucketStoreFactory.createWatermarkingService(Duration.ofMillis(5), periodicWatermarking::watermark, executor);

        watermarkingService.startAsync();
        watermarkingService.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamMetadataStore.close();
        watermarkingService.stopAsync().awaitTerminated();
        periodicWatermarking.close();
        retentionService.stopAsync();
        retentionService.awaitTerminated();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    abstract StreamMetadataStore createStreamStore(ScheduledExecutorService executor);

    abstract BucketStore createBucketStore(int bucketCount);

    @Test(timeout = 10000)
    public void testRetentionService() {
        Map<Integer, BucketService> bucketServices = retentionService.getBucketServices();
                                          
        assertNotNull(bucketServices);
        assertEquals(3, bucketServices.size());
        assertTrue(retentionService.takeBucketOwnership(0, hostId, executor).join());
        assertTrue(retentionService.takeBucketOwnership(1, hostId, executor).join());
        assertTrue(retentionService.takeBucketOwnership(2, hostId, executor).join());
        AssertExtensions.assertThrows("", () -> retentionService.takeBucketOwnership(3, hostId, executor).join(),
                e -> e instanceof IllegalArgumentException);
        retentionService.tryTakeOwnership(0).join();

        String scope = "scope";
        String streamName = "stream";
        Stream stream = new StreamImpl(scope, streamName);
        
        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, streamName, executor).join();

        // verify that at least one of the buckets got the notification
        int bucketId = BucketStore.getBucket(scope, streamName, 3);
        Set<String> streams = bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, bucketId, executor).join();
        
        BucketService bucketService = bucketServices.get(bucketId);
        AtomicBoolean added = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !added.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> added.set(bucketService.getKnownStreams().size() > 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getKnownStreams().contains(stream));

        bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.RetentionService, scope, streamName, executor).join();
        AtomicBoolean removed = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !removed.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> removed.set(bucketService.getKnownStreams().size() == 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertEquals(0, bucketService.getKnownStreams().size());
    }

    @Test(timeout = 10000)
    public void testWatermarkingService() {
        Map<Integer, BucketService> bucketServices = watermarkingService.getBucketServices();
                                          
        assertNotNull(bucketServices);
        assertEquals(3, bucketServices.size());
        assertTrue(watermarkingService.takeBucketOwnership(0, hostId, executor).join());
        assertTrue(watermarkingService.takeBucketOwnership(1, hostId, executor).join());
        assertTrue(watermarkingService.takeBucketOwnership(2, hostId, executor).join());
        AssertExtensions.assertThrows("", () -> watermarkingService.takeBucketOwnership(3, hostId, executor).join(),
                e -> e instanceof IllegalArgumentException);
        watermarkingService.tryTakeOwnership(0).join();

        String scope = "scope";
        String streamName = "stream";
        Stream stream = new StreamImpl(scope, streamName);
        
        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.WatermarkingService, scope, streamName, executor).join();

        // verify that at least one of the buckets got the notification
        int bucketId = BucketStore.getBucket(scope, streamName, 3);
        Set<String> streams = bucketStore.getStreamsForBucket(BucketStore.ServiceType.WatermarkingService, bucketId, executor).join();
        
        BucketService bucketService = bucketServices.get(bucketId);
        AtomicBoolean added = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !added.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> added.set(bucketService.getKnownStreams().size() > 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertTrue(bucketService.getKnownStreams().contains(stream));

        bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.WatermarkingService, scope, streamName, executor).join();
        AtomicBoolean removed = new AtomicBoolean(false);
        RetryHelper.loopWithDelay(() -> !removed.get(), () -> CompletableFuture.completedFuture(null)
                .thenAccept(x -> removed.set(bucketService.getKnownStreams().size() == 0)), Duration.ofSeconds(1).toMillis(), executor).join();
        assertEquals(0, bucketService.getKnownStreams().size());
    }
}
