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
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * A Process for all index segment related operations.
 */
@Slf4j
public final class IndexRequestProcessor {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final int ENTRY_SIZE = 24; //TODO obtain from property.

    static final class SearchFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public SearchFailedException(String message) {
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
     * @throws SearchFailedException if the index segment is of unexpected size or if the search fails.
     */
    public static long locateOffsetForSegment(StreamSegmentStore store, String segment, long targetOffset, boolean greater) throws SearchFailedException {
        return applySearch(store, segment, targetOffset, greater).getValue();
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
     * @throws SearchFailedException if the index segment is of unexpected size or if the search fails.
     */
    public static long locateOffsetForIndexSegment(StreamSegmentStore store, String segment, long targetOffset, boolean greater) throws SearchFailedException {
        return applySearch(store, segment, targetOffset, greater).getKey() * ENTRY_SIZE;
    }


    private static Map.Entry<Long, Long> applySearch(StreamSegmentStore store, String segment, long targetOffset, boolean greater) {
        String indexSegmentName = NameUtils.getIndexSegmentName(segment);

        //Fetch start and end idx.
        SegmentProperties properties = store.getStreamSegmentInfo(indexSegmentName, TIMEOUT).join();
        long startIdx = properties.getStartOffset() / ENTRY_SIZE;
        long endIdx = properties.getLength() / ENTRY_SIZE;

        return SortUtils.newtonianSearch(idx -> {
            if (startIdx == endIdx) {
                return startIdx;
            }
            ReadResult result = store.read(indexSegmentName, idx * ENTRY_SIZE, ENTRY_SIZE, TIMEOUT).join();
            ReadResultEntry firstElement = result.next();
            // TODO deal with element which is split over multiple entries.
            switch (firstElement.getType()) {
                case Cache: // fallthrough
                case Storage:
                    BufferView content = firstElement.getContent().join();
                    IndexEntry entry = IndexEntry.fromBytes(content);
                    return entry.getOffset();
                case Future:
                case EndOfStreamSegment:
                case Truncated:
                default:
                    throw new SearchFailedException("Index segment was of unexpected size: " + firstElement.getType());
            }
        }, startIdx, endIdx > 0 ? endIdx - 1 : 0, targetOffset, greater);
    }


}