/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.mocks;

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;

/**
 * Wraps a {@link StreamSegmentStore} and turns the async calls into synchronous ones by blocking on
 * the futures for test purposes.
 */
@RequiredArgsConstructor
public class SynchronousStreamSegmentStore implements StreamSegmentStore {

    private final StreamSegmentStore impl;

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates,
                                          Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, data, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data,
                                          Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, offset, data, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        CompletableFuture<Void> result = impl.updateAttributes(streamSegmentName, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        CompletableFuture<ReadResult> result = impl.read(streamSegmentName, offset, maxLength, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        CompletableFuture<SegmentProperties> result = impl.getStreamSegmentInfo(streamSegmentName, waitForPendingOps, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes,
                                                       Duration timeout) {
        CompletableFuture<Void> result = impl.createStreamSegment(streamSegmentName, attributes, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        CompletableFuture<String> result = impl.createTransaction(parentStreamSegmentName, transactionId, attributes, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        CompletableFuture<Void> result = impl.mergeTransaction(transactionName, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Long> result = impl.sealStreamSegment(streamSegmentName, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> result = impl.deleteStreamSegment(streamSegmentName, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        CompletableFuture<Void> result = impl.truncateStreamSegment(streamSegmentName, offset, timeout);
        Futures.await(result);
        return result;
    }
}
