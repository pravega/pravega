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

import com.google.common.base.Preconditions;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.InMemoryBucketStore;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

@Slf4j
public class InMemoryBucketManager extends BucketManager {
    private final BucketStore bucketStore;
    
    InMemoryBucketManager(String processId, InMemoryBucketStore bucketStore, BucketStore.ServiceType serviceType, 
                          ScheduledExecutorService executor, Function<Integer, BucketService> bucketServiceSupplier) {
        super(processId, serviceType, executor, bucketServiceSupplier);
        this.bucketStore = bucketStore;
    }

    @Override
    protected int getBucketCount() {
        return bucketStore.getBucketCount(getServiceType());
    }

    @Override
    void startBucketOwnershipListener() {

    }

    @Override
    void stopBucketOwnershipListener() {

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
    CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < getBucketCount());
        return CompletableFuture.completedFuture(true);
    }

}
