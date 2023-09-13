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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.DelayedProcessor;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.NameUtils.isTransactionSegment;
import static io.pravega.shared.NameUtils.isTransientSegment;
import static io.pravega.shared.NameUtils.isUserStreamSegment;

/**
 * Process incoming Append request for index segment.
 */
@Slf4j
public class IndexAppendProcessor implements AutoCloseable {
    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final StreamSegmentStore store;
    private final Duration delay;
    private final DelayedProcessor<QueuedSegment> appendProcessor;

    @Data
    private static class QueuedSegment implements DelayedProcessor.Item {
        private final String segmentName;

        @Override
        public String key() {
            return segmentName;
        }

    }
    
    @VisibleForTesting
    IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store, Duration delay) {
        this.store = store;
        this.delay = delay;
        this.appendProcessor = new DelayedProcessor<QueuedSegment>(this::handleIndexAppend, this.delay,
                                                                   indexSegmentUpdateExecutor, "IndexAppendProcessor");
    }

    public IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store) {
        this(indexSegmentUpdateExecutor, store, Duration.ofMillis(10));
    }

    /**
     * Appends index segment on a separate thread.
     * @param segmentName  segment name.
     * @param indexSegmentEventSize  Index segment event size.
     *                               Value 0 indicates the segment is not a Stream Segment.
     */
    protected void processAppend(String segmentName, long indexSegmentEventSize) {
        // Not updating index segment for transient and transaction type.
        if (isTransientSegment(segmentName) || isTransactionSegment(segmentName) || !isUserStreamSegment(segmentName)) {
            return;
        }
        if (indexSegmentEventSize != NameUtils.INDEX_APPEND_EVENT_SIZE) {
            log.debug("The data received for index segment append is not of desired size. Segment: {}, Actual: {}, Desired: {}",
                    getIndexSegmentName(segmentName), indexSegmentEventSize, NameUtils.INDEX_APPEND_EVENT_SIZE);
            return;
        }
        appendProcessor.process(new QueuedSegment(segmentName));
    }

    private CompletableFuture<Void> handleIndexAppend(QueuedSegment segment) {
        return store.getStreamSegmentInfo(segment.segmentName, TIMEOUT).thenCompose(info -> {
            long eventCount = info.getAttributes().get(EVENT_COUNT) != null ? info.getAttributes().get(EVENT_COUNT) : 0;
            ByteBufWrapper byteBuff = getIndexAppendBuf(info.getLength(), eventCount);
            AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.ReplaceIfGreater, eventCount));
            return store.append(getIndexSegmentName(segment.segmentName), byteBuff, attributes, TIMEOUT);
        }).thenRun(() -> {
            log.trace("Index segment append successful for segment {} ", getIndexSegmentName(segment.segmentName));
        }).exceptionally(ex -> {
            log.warn(
                "Index segment append failed for segment {} due to ", getIndexSegmentName(segment.segmentName), ex);
            return null;
        });
    }

    private ByteBufWrapper getIndexAppendBuf(Long eventLength, Long eventCount) {
        IndexEntry indexEntry = new IndexEntry(eventLength, eventCount, System.currentTimeMillis());
        return new ByteBufWrapper(Unpooled.wrappedBuffer( indexEntry.toBytes().getCopy()));
    }

    @Override
    public void close() {
        appendProcessor.shutdown();
    }
    
    @VisibleForTesting
    void runRemainingAndClose() {
        for (QueuedSegment segment : appendProcessor.shutdown()) {
            handleIndexAppend(segment);
        }
    }
}
