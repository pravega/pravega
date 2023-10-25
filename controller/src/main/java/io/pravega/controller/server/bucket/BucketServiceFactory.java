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

import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.InMemoryBucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

@Slf4j
public class BucketServiceFactory {
    private final String hostId;
    private final BucketStore bucketStore;
    private final int maxConcurrentExecutions;
    private final int minBucketRedistributionInterval;

    public BucketServiceFactory(@NonNull String hostId, @NonNull BucketStore bucketStore, int maxConcurrentExecutions,
                                int minBucketRedistributionInterval) {
        this.hostId = hostId;
        this.bucketStore = bucketStore;
        this.maxConcurrentExecutions = maxConcurrentExecutions;
        this.minBucketRedistributionInterval = minBucketRedistributionInterval;
    }

    public BucketManager createRetentionService(Duration executionDuration, BucketWork work, ScheduledExecutorService executorService) {
        switch (bucketStore.getStoreType()) {
            case Zookeeper:
                ZookeeperBucketStore zkBucketStore = (ZookeeperBucketStore) bucketStore;
                Function<Integer, BucketService> zkSupplier = bucket ->
                        new ZooKeeperBucketService(BucketStore.ServiceType.RetentionService, bucket, zkBucketStore, executorService,
                                maxConcurrentExecutions, executionDuration, work);

                return new ZooKeeperBucketManager(hostId, zkBucketStore, BucketStore.ServiceType.RetentionService,
                        executorService, zkSupplier, getBucketManagerLeader(BucketStore.ServiceType.RetentionService, executorService));
            case InMemory:
                InMemoryBucketStore inMemoryBucketStore = (InMemoryBucketStore) bucketStore;
                Function<Integer, BucketService> inMemorySupplier = bucket ->
                        new InMemoryBucketService(BucketStore.ServiceType.RetentionService, bucket, inMemoryBucketStore, executorService,
                                maxConcurrentExecutions, executionDuration, work);

                return new InMemoryBucketManager(hostId, (InMemoryBucketStore) bucketStore, BucketStore.ServiceType.RetentionService, 
                        executorService, inMemorySupplier, new UniformBucketDistributor());
            default:
                throw new IllegalArgumentException(String.format("store type %s not supported", bucketStore.getStoreType().name()));
        }
    }
    
    public BucketManager createWatermarkingService(Duration executionDuration, BucketWork work, ScheduledExecutorService executorService) {
        switch (bucketStore.getStoreType()) {
            case Zookeeper:
                ZookeeperBucketStore zkBucketStore = (ZookeeperBucketStore) bucketStore;
                Function<Integer, BucketService> zkSupplier = bucket ->
                        new ZooKeeperBucketService(BucketStore.ServiceType.WatermarkingService, bucket, zkBucketStore, executorService,
                                maxConcurrentExecutions, executionDuration, work);

                return new ZooKeeperBucketManager(hostId, zkBucketStore, BucketStore.ServiceType.WatermarkingService,
                        executorService, zkSupplier, getBucketManagerLeader(BucketStore.ServiceType.WatermarkingService, executorService));
            case InMemory:
                InMemoryBucketStore inMemoryBucketStore = (InMemoryBucketStore) bucketStore;
                Function<Integer, BucketService> inMemorySupplier = bucket ->
                        new InMemoryBucketService(BucketStore.ServiceType.WatermarkingService, bucket, inMemoryBucketStore, executorService,
                                maxConcurrentExecutions, executionDuration, work);

                return new InMemoryBucketManager(hostId, (InMemoryBucketStore) bucketStore, BucketStore.ServiceType.WatermarkingService, 
                        executorService, inMemorySupplier, new UniformBucketDistributor());
            default:
                throw new IllegalArgumentException(String.format("store type %s not supported", bucketStore.getStoreType().name()));
        }
    }

    private BucketManagerLeader getBucketManagerLeader(BucketStore.ServiceType serviceType,
                                                       ScheduledExecutorService executorService) {
        return new BucketManagerLeader(bucketStore, minBucketRedistributionInterval,
                new UniformBucketDistributor(), serviceType, executorService);
    }
}
