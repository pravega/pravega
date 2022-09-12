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
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.util.RetryHelper;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InMemoryStoreBucketServiceTest extends BucketServiceTest {

    @Override
    protected void addEntryToZkCluster(Host host) {

    }

    @Override
    public StreamMetadataStore createStreamStore(ScheduledExecutorService executor) {
        return StreamStoreFactory.createInMemoryStore();
    }

    @Override
    public BucketStore createBucketStore(int bucketCount) {
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, bucketCount,
                BucketStore.ServiceType.WatermarkingService, bucketCount);

        return StreamStoreFactory.createInMemoryBucketStore(map);
    }

    @Test (timeout = 10000)
    public void testRetentionService() throws Exception {

        Map<Integer, BucketService> bucketServices = retentionService.getBucketServices();
        assertNotNull(bucketServices);
        assertEquals(3, bucketServices.size());

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

    @Test //(timeout = 10000)
    public void testWatermarkingService() throws Exception {
        Map<Integer, BucketService> bucketServices = watermarkingService.getBucketServices();
        assertNotNull(bucketServices);
        assertEquals(3, bucketServices.size());

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
