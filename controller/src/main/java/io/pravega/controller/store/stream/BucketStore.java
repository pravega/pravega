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

import io.pravega.controller.store.client.StoreType;
import io.pravega.shared.NameUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.Getter;

/**
 * Bucket Store interface. 
 */
public interface BucketStore {
    /**
     * Method to return store type that implements this interface. Currently we have zookeeper bucket store and
     * in memory bucket store implementations. 
     * 
     * @return type of store. 
     */
    StoreType getStoreType();
    
    /**
     * Method to get count of buckets in the store.
     * @param serviceType service type
     * @return number of buckets.
     */
    int getBucketCount(ServiceType serviceType);
    
    /**
     * Return all streams in the bucket.
     *
     * @param serviceType service type
     * @param bucket      bucket id.
     * @param executor    executor
     * @return Future, which when completed will have a list of scopedStreamName (scope/stream) of all streams under the bucket.
     */
    CompletableFuture<Set<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor);

    /**
     * Add the given stream to appropriate bucket for given service type.
     *
     * @param serviceType service type
     * @param scope       scope
     * @param stream      stream
     * @param executor    executor
     * @return future, which when completed will indicate that stream is added to bucket store.
     */
    CompletableFuture<Void> addStreamToBucketStore(ServiceType serviceType, String scope, String stream, Executor executor);

    /**
     * Remove stream from service bucket.
     *
     * @param serviceType service type
     * @param scope       scope
     * @param stream      stream
     * @param executor    executor
     * @return future, which when completed will indicate that stream is removed from the store.
     */
    CompletableFuture<Void> removeStreamFromBucketStore(ServiceType serviceType, String scope, String stream, Executor executor);

    /**
     * Get the existing controller to bucket map.
     *
     * @param serviceType   service type.
     * @return future, which when completed will have map of process and associated set of buckets.
     */
    CompletableFuture<Map<String, Set<Integer>>> getBucketControllerMap(ServiceType serviceType);

    /**
     * Update the existing controller to bucket map with the new one. This operation has to be atomic.
     *
     * @param newMapping    The new controllers to bucket mapping which needs to be persisted.
     * @param serviceType   service type.
     * @return future, which when completed will indicate that bucket updated successfully.
     */
    CompletableFuture<Void> updateBucketControllerMap(Map<String, Set<Integer>> newMapping, ServiceType serviceType);

    /**
     * Get the buckets associated with a particular controller.
     * @param processId     processId of Controller.
     * @param serviceType   service type.
     * @return future, which when complete will give set of buckets.
     */
    CompletableFuture<Set<Integer>> getBucketsForController(String processId, ServiceType serviceType);

    enum ServiceType {
        // Naming the service id as "buckets" for backward compatibility
        RetentionService("buckets"),
        WatermarkingService("watermarks"),;

        @Getter
        private final String name;

        ServiceType(String name) {
            this.name = name;
        }
    }

    static int getBucket(String scope, String stream, int bucketCount) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return (scopedStreamName.hashCode() & 0x7fffffff) % bucketCount;
    }

    static String getScopedStreamName(String scope, String stream) {
        return NameUtils.getScopedStreamName(scope, stream);
    }
}
