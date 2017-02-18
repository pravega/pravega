/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.mocks;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentStore;
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
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributes,
                                          Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, data, attributes, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data,
                                          Collection<AttributeUpdate> attributes, Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, offset, data, attributes, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        CompletableFuture<ReadResult> result = impl.read(streamSegmentName, offset, maxLength, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        CompletableFuture<SegmentProperties> result = impl.getStreamSegmentInfo(streamSegmentName, waitForPendingOps, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes,
                                                       Duration timeout) {
        CompletableFuture<Void> result = impl.createStreamSegment(streamSegmentName, attributes, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        CompletableFuture<String> result = impl.createTransaction(parentStreamSegmentName, transactionId, attributes, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> mergeTransaction(String transactionName, Duration timeout) {
        CompletableFuture<Long> result = impl.mergeTransaction(transactionName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Long> result = impl.sealStreamSegment(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> result = impl.deleteStreamSegment(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> updateStreamSegmentPolicy(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        return CompletableFuture.completedFuture(null);
    }
}
