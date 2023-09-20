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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.SearchUtils;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.INDEX_APPEND_EVENT_SIZE;

/**
 * A Process for all index segment related operations.
 */
@Slf4j
public final class IndexRequestProcessor {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);

    static final class SegmentTruncatedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SegmentTruncatedException(String message) {
            super(message);
        }
    }

    /**
     * Locate an indexed offset near the target location in the segment.
     *
     * @param store  The StreamSegmentStore to attach to (and issue requests to).
     * @param segment Segment name.
     * @param targetOffset The requested offset's corresponding position to search in the index segment entry.
     * @param greater boolean to determine if the next higher or the lower value to be returned in case the requested offset is not present.
     *
     * @return A future for the corresponding offset position from the index segment entry.
     * @throws SegmentTruncatedException If the segment is truncated.
     */
    public static CompletableFuture<Long> findNearestIndexedOffset(StreamSegmentStore store, String segment,
                                                                   long targetOffset,
                                                                   boolean greater) throws SegmentTruncatedException {
        String indexSegmentName = NameUtils.getIndexSegmentName(segment);

        // Fetch start and end idx.
        return store.getStreamSegmentInfo(indexSegmentName, TIMEOUT).thenCompose(properties -> {
            long startIdx = properties.getStartOffset() / INDEX_APPEND_EVENT_SIZE;
            long endIdx = properties.getLength() / INDEX_APPEND_EVENT_SIZE - 1;
            // If startIdx and endIdx are same, then pass length of segment as a result.

            if (startIdx > endIdx) {
                return store.getStreamSegmentInfo(segment, TIMEOUT).thenApply(segmentProperties -> segmentProperties.getLength());
            }

            return SearchUtils.asyncNewtonianSearch(idx -> {
                return store.read(indexSegmentName, idx * INDEX_APPEND_EVENT_SIZE, INDEX_APPEND_EVENT_SIZE, TIMEOUT)
                        .thenCompose(readResult -> getOffsetFromIndexEntry(indexSegmentName, readResult));
            }, startIdx, endIdx, targetOffset, greater).thenCompose(result -> {
                if (greater && result.getValue() < targetOffset) {
                    return store.getStreamSegmentInfo(segment, TIMEOUT).thenApply(segmentProperties -> segmentProperties.getLength());
                }
                if (!greater && result.getValue() > targetOffset) {
                    return store.getStreamSegmentInfo(segment, TIMEOUT).thenApply(segmentProperties -> segmentProperties.getStartOffset());
                }
                return CompletableFuture.completedFuture(result.getValue());
            });
        });
    }

    /**
     * Find the offset in the index segment that works as a truncation point given the provided offset in the main segment.
     *
     * @param store  The StreamSegmentStore to attach to (and issue requests to).
     * @param segment Segment name.
     * @param targetOffset The requested offset's corresponding position to search in the index segment entry.
     * 
     * @return A future for the corresponding offset of index segment.
     */
    public static CompletableFuture<Long> locateTruncateOffsetInIndexSegment(StreamSegmentStore store, String segment,
                                                                             long targetOffset) {
        String indexSegmentName = NameUtils.getIndexSegmentName(segment);

        // Fetch start and end idx.
        return store.getStreamSegmentInfo(indexSegmentName, TIMEOUT).thenCompose(properties -> {
            long startIdx = properties.getStartOffset() / INDEX_APPEND_EVENT_SIZE;
            long endIdx = properties.getLength() / INDEX_APPEND_EVENT_SIZE - 1;
            // If startIdx and endIdx are same, then pass length of segment as a result.
            if (startIdx > endIdx) {
                return CompletableFuture.completedFuture(properties.getStartOffset());
            }

            return SearchUtils.asyncNewtonianSearch(idx -> {
                return store.read(indexSegmentName, idx * INDEX_APPEND_EVENT_SIZE, INDEX_APPEND_EVENT_SIZE, TIMEOUT)
                            .thenCompose(readResult -> getOffsetFromIndexEntry(indexSegmentName, readResult));
            }, startIdx, endIdx, targetOffset, false).thenApply(result -> {
                if (targetOffset < result.getValue()) {
                    return result.getKey() * INDEX_APPEND_EVENT_SIZE;
                } else {
                    return (result.getKey() + 1) * INDEX_APPEND_EVENT_SIZE;
                }
            });
        });
    }

    private static CompletableFuture<Long> getOffsetFromIndexEntry(String segment, ReadResult readResult) {
        return readResult(segment, readResult, 0).thenApply(result -> IndexEntry.fromBytes(BufferView.wrap(result)).getOffset());
    }

    private static CompletableFuture<List<BufferView>> readResult(String segment, ReadResult readResult, int dataRead) {
        if (!readResult.hasNext()) {            
            return CompletableFuture.completedFuture(Collections.emptyList());
        } 

        ReadResultEntry entry = readResult.next();
        switch (entry.getType()) {
        case Truncated:
            throw new SegmentTruncatedException(String.format("Segment %s has been truncated.", segment));
        case EndOfStreamSegment:
            throw new IllegalStateException(String.format("Unexpected size of index segment of type: %s was encountered for segment %s.", entry.getType(), segment));
        default:
            entry.requestContent(TIMEOUT);
            return entry.getContent().thenCompose(first -> {
                if (dataRead + first.getLength() >= INDEX_APPEND_EVENT_SIZE) {
                    return CompletableFuture.completedFuture(List.of(first));
                } else {
                    return readResult(segment, readResult, dataRead + first.getLength()).thenApply(rest -> {
                        ArrayList<BufferView> result = new ArrayList<>(rest.size() + 1);
                        result.add(first);
                        result.addAll(rest);
                        return result;
                    });
                }
            });
        }
    }
}