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
import io.pravega.common.concurrent.LatestItemSequentialProcessor;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.isTransactionSegment;
import static io.pravega.shared.NameUtils.isTransientSegment;

/**
 * Process incoming Append request for index segment.
 */
@Slf4j
public class IndexAppendProcessor {
    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final ScheduledExecutorService indexSegmentUpdateExecutor;
    private final StreamSegmentStore store;
    private final LatestItemSequentialProcessor<IndexAppend> latestItemProcessor;

    public IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store) {
        this.indexSegmentUpdateExecutor = indexSegmentUpdateExecutor;
        this.store = store;
        latestItemProcessor = new LatestItemSequentialProcessor<>(this::handleIndexAppend, indexSegmentUpdateExecutor);
    }

    protected void processAppend(IndexAppend indexAppend) {
        // Not updating index segment for transient and transaction type.
        if (isTransientSegment(indexAppend.getIndexSegment()) || isTransactionSegment(indexAppend.getIndexSegment())) {
            return;
        }
       if (indexAppend.getBufferWrapper().getLength() != indexAppend.getMaxEventSize()) {
            log.debug("The data received for index segment: {} append is not of desired size Actual: {}, Desired: {}",
                    indexAppend.getIndexSegment(), indexAppend.getBufferWrapper().getLength(), indexAppend.getMaxEventSize());
            return;
        }
        latestItemProcessor.updateItem(indexAppend);
    }

    private void handleIndexAppend(IndexAppend indexAppend) {
        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
               new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.ReplaceIfGreater, indexAppend.getEventCount()));
        store.append(indexAppend.getIndexSegment(), indexAppend.getBufferWrapper(), attributes, TIMEOUT)
                .thenAccept(v -> log.info("Index segment append successful for segment {} ", indexAppend.getIndexSegment()))
                .exceptionally(e -> {
                    log.warn("Index segment append failed for segment {} due to ", indexAppend.getIndexSegment(), e);
                    return null;
                });
    }

    protected ByteBufWrapper getIndexAppendBuf(Long offset, Long eventCount) {
        IndexEntry indexEntry = new IndexEntry(offset, eventCount, System.currentTimeMillis());
        return new ByteBufWrapper(Unpooled.wrappedBuffer( indexEntry.toBytes().getCopy()));
    }
}
