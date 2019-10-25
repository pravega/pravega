/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.store.client.StoreType;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Getter;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
        return StreamSegmentNameUtils.getScopedStreamName(scope, stream);
    }
}
