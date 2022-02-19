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
package io.pravega.segmentstore.server.store;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the Log/StreamSegment Service, that puts together everything and is what should be exposed to the outside.
 */
@Slf4j
public class StreamSegmentService extends SegmentContainerCollection implements StreamSegmentStore {

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentService class.
     *
     * @param segmentContainerRegistry The SegmentContainerRegistry to route requests to.
     * @param segmentToContainerMapper The SegmentToContainerMapper to use to map StreamSegments to Containers.
     */
    public StreamSegmentService(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        super(segmentContainerRegistry, segmentToContainerMapper);
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, data, attributeUpdates, timeout),
                "append", streamSegmentName, data.getLength(), attributeUpdates);
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, offset, data, attributeUpdates, timeout),
                "appendWithOffset", streamSegmentName, offset, data.getLength(), attributeUpdates);
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.updateAttributes(streamSegmentName, attributeUpdates, timeout),
                "updateAttributes", streamSegmentName, attributeUpdates);
    }

    @Override
    public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getAttributes(streamSegmentName, attributeIds, cache, timeout),
                "getAttributes", streamSegmentName, attributeIds);
    }

    @Override
    public CompletableFuture<Void> flushToStorage(int containerId, Duration timeout) {
        return invoke(
                containerId,
                container -> container.flushToStorage(timeout),
                "flushToStorage");
    }

    @Override
    public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getExtendedChunkInfo(streamSegmentName, timeout),
                "getExtendedChunkInfo", streamSegmentName);
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.read(streamSegmentName, offset, maxLength, timeout),
                "read", streamSegmentName, offset, maxLength);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getStreamSegmentInfo(streamSegmentName, timeout),
                "getStreamSegmentInfo", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.createStreamSegment(streamSegmentName, segmentType, attributes, timeout),
                "createStreamSegment", streamSegmentName, segmentType, attributes);
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        return invoke(
                sourceStreamSegment,
                container -> container.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, timeout),
                "mergeStreamSegment", targetStreamSegment, sourceStreamSegment);
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment,
                                                                          AttributeUpdateCollection attributes, Duration timeout) {
        return invoke(
                sourceStreamSegment,
                container -> container.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, attributes, timeout),
                "mergeStreamSegment", targetStreamSegment, sourceStreamSegment);
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.sealStreamSegment(streamSegmentName, timeout),
                "sealStreamSegment", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.deleteStreamSegment(streamSegmentName, timeout),
                "deleteStreamSegment", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.truncateStreamSegment(streamSegmentName, offset, timeout),
                "truncateStreamSegment", streamSegmentName);
    }

    //endregion
}
