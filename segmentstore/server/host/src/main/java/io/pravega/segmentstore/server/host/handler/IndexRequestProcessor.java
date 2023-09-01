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
import io.pravega.common.util.SortUtils;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

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
     * Locate the requested offset in segment.
     *
     * @param store  The StreamSegmentStore to attach to (and issue requests to).
     * @param segment Segment name.
     * @param targetOffset The requested offset's corresponding position to search in the index segment entry.
     * @param greater boolean to determine if the next higher or the lower value to be returned in case the requested offset is not present.
     *
     * @return the corresponding offset position from the index segment entry.
     * @throws SegmentTruncatedException If the segment is truncated.
     */
    public static long locateOffsetForSegment(StreamSegmentStore store, String segment, long targetOffset, boolean greater) throws SegmentTruncatedException {
        long offset = applySearch(store, segment, targetOffset, greater).getValue();
        return offset;
    }

    /**
     * Locate the requested offset in index segment.
     *
     * @param store  The StreamSegmentStore to attach to (and issue requests to).
     * @param segment Segment name.
     * @param targetOffset The requested offset's corresponding position to search in the index segment entry.
     * @param greater boolean to determine if the next higher or the lower value to be returned in case the requested offset is not present.
     *
     * @return the corresponding offset of index segment.
     */
    public static long locateOffsetForIndexSegment(StreamSegmentStore store, String segment, long targetOffset, boolean greater) {
        return applySearch(store, segment, targetOffset, greater).getKey() * NameUtils.INDEX_APPEND_EVENT_SIZE;
    }

    private static Map.Entry<Long, Long> applySearch(StreamSegmentStore store, String segment, long targetOffset, boolean greater) {
        String indexSegmentName = NameUtils.getIndexSegmentName(segment);

        //Fetch start and end idx.
        SegmentProperties properties = store.getStreamSegmentInfo(indexSegmentName, TIMEOUT).join();
        long startIdx = properties.getStartOffset() / NameUtils.INDEX_APPEND_EVENT_SIZE;
        long endIdx = properties.getLength() / NameUtils.INDEX_APPEND_EVENT_SIZE;
        //If startIdx and endIdx are same, then pass length of segment as a result.
        if (startIdx == endIdx) {
           return getSegmentLength(store, segment, startIdx);
        }

        return SortUtils.newtonianSearch(idx -> {
            ReadResult result = store.read(indexSegmentName, idx * NameUtils.INDEX_APPEND_EVENT_SIZE, NameUtils.INDEX_APPEND_EVENT_SIZE, TIMEOUT).join();
            result.readRemaining(0, TIMEOUT);
            return getOffsetFromIndexEntry(indexSegmentName, result);
        }, startIdx, endIdx > 0 ? endIdx - 1 : 0, targetOffset, greater);
    }

    private static long getOffsetFromIndexEntry(String segment, ReadResult readResult) {
        int bytesRead = 0;
        ArrayList<BufferView> result = new ArrayList<>(1);
        while (bytesRead < NameUtils.INDEX_APPEND_EVENT_SIZE) {
            ReadResultEntry entry = readResult.next();
            switch (entry.getType()) {
            case Truncated:
                throw new SegmentTruncatedException(String.format("Segment %s has been truncated.", segment));
            case EndOfStreamSegment:
                throw new IllegalStateException(String.format("Unexpected size of index segment of type: %s was encountered for segment %s.", entry.getType(), segment));
            case Cache: // fallthrough
            case Storage:
            case Future:
                entry.requestContent(TIMEOUT);
                result.add(entry.getContent().join());
            }
        }
        return IndexEntry.fromBytes(BufferView.wrap(result)).getOffset();
    }


    private static Map.Entry<Long, Long> getSegmentLength(StreamSegmentStore store, String segment, long startIdx) {
        SegmentProperties segmentProperties = store.getStreamSegmentInfo(segment, TIMEOUT).join();
        return new AbstractMap.SimpleEntry<>(startIdx, segmentProperties.getLength());
    }
}