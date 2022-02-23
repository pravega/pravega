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
package io.pravega.segmentstore.server.mocks;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import lombok.RequiredArgsConstructor;

/**
 * Wraps a {@link StreamSegmentStore} and turns the async calls into synchronous ones by blocking on
 * the futures for test purposes.
 */
@RequiredArgsConstructor
public class SynchronousStreamSegmentStore implements StreamSegmentStore {

    private final StreamSegmentStore impl;

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates,
                                          Duration timeout) {
        CompletableFuture<Long> result = impl.append(streamSegmentName, data, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data,
                                          AttributeUpdateCollection attributeUpdates, Duration timeout) {
        CompletableFuture<Long> result = impl.append(streamSegmentName, offset, data, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        CompletableFuture<Void> result = impl.updateAttributes(streamSegmentName, attributeUpdates, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
        CompletableFuture<Map<AttributeId, Long>> result = impl.getAttributes(streamSegmentName, attributeIds, cache, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> flushToStorage(int containerId, Duration timeout) {
        CompletableFuture<Void> result = impl.flushToStorage(containerId, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<List<ExtendedChunkInfo>> result = impl.getExtendedChunkInfo(streamSegmentName, timeout);
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
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> result = impl.getStreamSegmentInfo(streamSegmentName, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        CompletableFuture<Void> result = impl.createStreamSegment(streamSegmentName, segmentType, attributes, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        CompletableFuture<MergeStreamSegmentResult> result = impl.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, timeout);
        Futures.await(result);
        return result;
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment,
                                                                          AttributeUpdateCollection attributes, Duration timeout) {
        CompletableFuture<MergeStreamSegmentResult> result = impl.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, attributes, timeout);
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
