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
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.controller.store.client.StoreType;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;

public class InMemoryBucketStore implements BucketStore {
    private final ImmutableMap<ServiceType, Integer> bucketCountMap;
    
    private final ConcurrentMap<String, ConcurrentSkipListSet<String>> bucketedStreams;
    
    private final ConcurrentMap<String, BucketChangeListener> listeners;

    InMemoryBucketStore(ImmutableMap<ServiceType, Integer> bucketCountMap) {
        this.bucketCountMap = bucketCountMap;
        bucketedStreams = new ConcurrentHashMap<>();
        listeners = new ConcurrentHashMap<>();
    }
    
    private String getBucketName(ServiceType serviceType, int bucket) {
        return serviceType.getName() + "/" + bucket;
    }

    @Override
    public StoreType getStoreType() {
        return StoreType.InMemory;
    }

    @Override
    public int getBucketCount(ServiceType serviceType) {
        return bucketCountMap.get(serviceType);
    }

    @Override
    public CompletableFuture<Set<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor) {
        String bucketName = getBucketName(serviceType, bucket);
        if (bucketedStreams.containsKey(bucketName)) {
            return CompletableFuture.completedFuture(Collections.unmodifiableSet(bucketedStreams.get(bucketName)));
        } else {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    @Override
    public CompletableFuture<Void> addStreamToBucketStore(ServiceType serviceType, String scope, String stream, Executor executor) {
        int bucketCount = bucketCountMap.get(serviceType);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketName = getBucketName(serviceType, bucket);
        ConcurrentSkipListSet<String> set = bucketedStreams.compute(bucketName, (x, y) -> {
            if (y == null) {
                return new ConcurrentSkipListSet<>();
            } else {
                return y;
            }
        });
        
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);
        set.add(scopedStreamName);
        
        listeners.computeIfPresent(bucketName, (b, listener) -> {
            listener.notify(scope, stream, true);
            return listener;
        });
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeStreamFromBucketStore(ServiceType serviceType, String scope, String stream, Executor executor) {
        int bucketCount = bucketCountMap.get(serviceType);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketName = getBucketName(serviceType, bucket);

        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);

        bucketedStreams.computeIfPresent(bucketName, (b, set) -> {
            set.remove(scopedStreamName);
            return set;
        });
        
        listeners.computeIfPresent(getBucketName(serviceType, bucket), (b, listener) -> {
            listener.notify(scope, stream, false);
            return listener;
        });
        return CompletableFuture.completedFuture(null);
    }

    public void registerBucketChangeListener(ServiceType serviceType, int bucketId, BucketChangeListener listener) {
        String bucketName = getBucketName(serviceType, bucketId);

        listeners.putIfAbsent(bucketName, listener);
    }
    
    public void unregisterBucketChangeListener(ServiceType serviceType, int bucketId) {
        String bucketName = getBucketName(serviceType, bucketId);
        listeners.remove(bucketName);
    }

    @FunctionalInterface
    public interface BucketChangeListener {
        void notify(String scope, String stream, boolean add); 
    }
}
