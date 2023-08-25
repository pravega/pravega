/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.segmentstore.server.host.handler;

import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.MultiKeyLatestItemSequentialProcessor;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.NameUtils.isTransactionSegment;
import static io.pravega.shared.NameUtils.isTransientSegment;

/**
 * Process incoming Append request for index segment.
 */
@Slf4j
public class IndexAppendProcessor {
    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final StreamSegmentStore store;
    private final MultiKeyLatestItemSequentialProcessor<String, Long> appendProcessor;

    public IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store) {
        this.store = store;
        appendProcessor = new MultiKeyLatestItemSequentialProcessor<>(this::handleIndexAppend, indexSegmentUpdateExecutor);
    }

    /**
     * Appends index segment on a separate thread.
     * @param segmentName  segment name.
     * @param indexSegmentEventSize  Index segment event size.
     *                               Value 0 indicates the segment is not a Stream Segment.
     */
    protected void processAppend(String segmentName, long indexSegmentEventSize) {
        // Not updating index segment for transient and transaction type.
        if (isTransientSegment(segmentName) || isTransactionSegment(segmentName)) {
            return;
        }
        if (indexSegmentEventSize != NameUtils.INDEX_APPEND_EVENT_SIZE) {
            log.debug("The data received for index segment append is not of desired size. Segment: {}, Actual: {}, Desired: {}",
                    getIndexSegmentName(segmentName), indexSegmentEventSize, NameUtils.INDEX_APPEND_EVENT_SIZE);
            return;
        }
        appendProcessor.updateItem(segmentName, indexSegmentEventSize);
    }

    private void handleIndexAppend(String segmentName, long indexSegmentEventSize) {
        store.getStreamSegmentInfo(segmentName, TIMEOUT)
                .thenAccept(info -> {
                    long eventCount = info.getAttributes().get(EVENT_COUNT) != null ? info.getAttributes().get(EVENT_COUNT) : 0;
                    ByteBufWrapper byteBuff = getIndexAppendBuf(info.getLength(), eventCount);
                    AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                            new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.ReplaceIfGreater, eventCount));
                    store.append(getIndexSegmentName(segmentName), byteBuff, attributes, TIMEOUT)
                            .thenAccept(v -> log.trace("Index segment append successful for segment {} ", getIndexSegmentName(segmentName)))
                            .exceptionally(e -> {
                                log.warn("Index segment append failed for segment {} due to ", getIndexSegmentName(segmentName), e);
                                return null;
                            });

                })
                .exceptionally(ex -> {
                    log.warn("Exception occured while fetching SegmentInfo for segment: {}", segmentName, ex);
                    return null;
                });
    }

    private ByteBufWrapper getIndexAppendBuf(Long eventLength, Long eventCount) {
        IndexEntry indexEntry = new IndexEntry(eventLength, eventCount, System.currentTimeMillis());
        return new ByteBufWrapper(Unpooled.wrappedBuffer( indexEntry.toBytes().getCopy()));
    }

}
