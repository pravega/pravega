/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Stream Metadata.
 */
public interface BucketStore {
    /**
     * Method to get count of buckets in the store.
     *
     * @return number of buckets.
     */
    int getBucketCount();

    /**
     * Method to register listener for changes to bucket's ownership.
     *
     * @param serviceType service type
     * @param listener    listener
     */
    void registerBucketOwnershipListener(ServiceType serviceType, BucketOwnershipListener listener);

    /**
     * Unregister listeners for bucket ownership.
     *
     * @param serviceType service type
     */
    void unregisterBucketOwnershipListener(ServiceType serviceType);

    /**
     * Method to take ownership of a bucket.
     *
     * @param serviceType service type
     * @param bucket      bucket id
     * @param processId   process id
     * @param executor    executor
     * @return future, which when completed, will contain a boolean which tells if ownership attempt succeeded or failed.
     */
    CompletableFuture<Boolean> takeBucketOwnership(ServiceType serviceType, int bucket, String processId, Executor executor);

    // region retention

    /**
     * Method to register listeners for changes to streams under the bucket.
     *
     * @param serviceType service type
     * @param bucket      bucket
     * @param listener    listener
     */
    void registerBucketChangeListener(ServiceType serviceType, int bucket, BucketChangeListener listener);

    /**
     * Method to unregister listeners for changes to streams under the bucket.
     *
     * @param serviceType service type
     * @param bucket      bucket
     */
    void unregisterBucketChangeListener(ServiceType serviceType, int bucket);

    /**
     * Return all streams in the bucket.
     *
     * @param serviceType service type
     * @param bucket      bucket id.
     * @param executor    executor
     * @return Future, which when completed will have a list of scopedStreamName (scope/stream) of all streams under the bucket.
     */
    CompletableFuture<List<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor);

    /**
     * Add the given stream to appropriate bucket for auto-retention.
     *
     * @param serviceType service type
     * @param scope       scope
     * @param stream      stream
     * @param executor    executor
     * @return future, which when completed will indicate that stream is added to bucket store.
     */
    CompletableFuture<Void> addStreamToBucketStore(ServiceType serviceType, String scope, String stream, Executor executor);

    /**
     * Remove stream from auto retention bucket.
     *
     * @param serviceType service type
     * @param scope       scope
     * @param stream      stream
     * @param executor    executor
     * @return future, which when completed will indicate that stream is removed from the store.
     */
    CompletableFuture<Void> removeStreamFromBucketStore(ServiceType serviceType, String scope, String stream, Executor executor);
    // endregion

    enum ServiceType {
        // Naming the service id as "buckets" for backward compatibility
        RetentionService("buckets"),;

        @Getter
        private final String name;

        ServiceType(String name) {
            this.name = name;
        }
    }

    static int getBucket(String scope, String stream, int bucketCount) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return scopedStreamName.hashCode() % bucketCount;
    }

    static String getScopedStreamName(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }
}
