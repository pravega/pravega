/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.segmentstore.selftest;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction layer for Segment Store operations that are valid from the Self Tester.
 */
interface StoreAdapter extends AutoCloseable {

    void initialize() throws Exception;

    CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout);

    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout);

    CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout);

    CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout);

    CompletableFuture<String> createTransaction(String parentStreamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout);

    CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout);

    CompletableFuture<Void> sealStreamSegment(String streamSegmentName, Duration timeout);

    CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout);

    VerificationStorage getStorageAdapter();

    ExecutorServiceHelpers.Snapshot getStorePoolSnapshot();

    boolean isFeatureSupported(Feature feature);

    @Override
    void close();

    enum Feature {
        Create,
        Delete,
        Append,
        GetInfo,
        Seal,
        Read,
        Transaction,
        StorageDirect;

        protected void ensureSupported(StoreAdapter storeAdapter, String operationName) {
            if (!storeAdapter.isFeatureSupported(this)) {
                throw new UnsupportedOperationException(String.format("Cannot %s because StoreAdapter does not support '%s'.", operationName, this));
            }
        }
    }
}
