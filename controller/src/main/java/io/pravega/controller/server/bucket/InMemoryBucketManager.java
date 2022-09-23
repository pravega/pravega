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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryBucketManager extends BucketManager {
    private final BucketStore bucketStore;
    private final String processId;
    private final BucketManagerLeader bucketManagerLeader;
    
    InMemoryBucketManager(String processId, InMemoryBucketStore bucketStore, BucketStore.ServiceType serviceType, 
                          ScheduledExecutorService executor, Function<Integer, BucketService> bucketServiceSupplier,
                          BucketManagerLeader bucketManagerLeader) {
        super(processId, serviceType, executor, bucketServiceSupplier, bucketStore);
        this.bucketStore = bucketStore;
        this.processId = processId;
        this.bucketManagerLeader = bucketManagerLeader;
    }

    @Override
    protected int getBucketCount() {
        return bucketStore.getBucketCount(getServiceType());
    }

    /**
     * Get the health status.
     *
     * @return true by default.
     */
    @Override
    public boolean isHealthy() {
        return true;
    }

    @Override
    public void startLeaderElection() {
        //As there is no use of zookeeper in Inmemory, so directly start distributing the buckets to controller.
        bucketStore.getBucketControllerMap(getServiceType())
                   .thenApply(x -> bucketManagerLeader.getBucketDistributor().distribute(x, Set.of(processId), getBucketCount()))
                   .thenAccept(newMap -> bucketStore.updateBucketControllerMap(newMap, getServiceType()))
                   .thenAccept(v -> startLeader())
                   .whenComplete((r, e) -> {
                       if (e == null) {
                           log.debug("{}: started in InMemory mode.", getServiceType());
                       } else {
                           log.warn("{}: starting fails in InMemory mode.", getServiceType());
                       }
                   });
    }

    @Override
    public void startLeader() {
    }

    @Override
    public void stopLeader() {

    }

    @Override
    CompletableFuture<Void> initializeService() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    CompletableFuture<Void> initializeBucket(int bucket) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    void startBucketControllerMapListener() {

    }

    @Override
    void stopBucketControllerMapListener() {

    }
}
