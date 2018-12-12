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

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Stream Metadata.
 */
public interface BucketStore {
    /**
     * Method to register listener for changes to bucket's ownership.
     *
     * @param listener listener
     */
    void registerBucketOwnershipListener(BucketOwnershipListener listener);

    /**
     * Unregister listeners for bucket ownership.
     */
    void unregisterBucketOwnershipListener();
    
    /**
     * Method to take ownership of a bucket.
     *
     * @param bucket   bucket id
     * @param processId process id
     *@param executor executor  @return future boolean which tells if ownership attempt succeeded or failed.
     */
    CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, final Executor executor);

    // region retention
    /**
     * Method to register listeners for changes to streams under the bucket.
     *
     * @param bucket   bucket
     * @param listener listener
     */
    void registerBucketChangeListenerForRetention(int bucket, BucketChangeListener listener);

    /**
     * Method to unregister listeners for changes to streams under the bucket.
     *
     * @param bucket bucket
     */
    void unregisterBucketChangeListenerForRetention(int bucket);

    /**
     * Return all streams in the bucket.
     *
     * @param bucket   bucket id.
     * @param executor executor
     * @return List of scopedStreamName (scope/stream)
     */
    CompletableFuture<List<String>> getStreamsForRetention(final int bucket, final Executor executor);

    /**
     * Add the given stream to appropriate bucket for auto-retention.
     *
     * @param scope           scope
     * @param stream          stream
     * @param retentionPolicy retention policy
     * @param executor        executor
     * @return future
     */
    CompletableFuture<Void> addUpdateStreamForRetention(final String scope, final String stream, final RetentionPolicy retentionPolicy,
                                                        final Executor executor);

    /**
     * Remove stream from auto retention bucket.
     *
     * @param scope    scope
     * @param stream   stream
     * @param executor executor
     * @return future
     */
    CompletableFuture<Void> removeStreamFromRetention(final String scope, final String stream,
                                                      final Executor executor);
    // endregion
    
    static int getBucket(String scope, String stream, int bucketCount) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return scopedStreamName.hashCode() % bucketCount;
    }

    static String getScopedStreamName(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }
}
