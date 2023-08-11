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

import com.google.common.base.Preconditions;
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
import static io.pravega.shared.NameUtils.getIndexSegmentName;
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

    public IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store) {
        this.indexSegmentUpdateExecutor = indexSegmentUpdateExecutor;
        this.store = store;
    }

    protected void processAppend(IndexAppend indexAppend) {
        // Not updating index segment for transient and transaction type.
        if (isTransientSegment(indexAppend.getSegment()) || isTransactionSegment(indexAppend.getSegment())) {
            return;
        }
        LatestItemSequentialProcessor<IndexAppend> latestItemProcessor = new LatestItemSequentialProcessor<>(this::handleIndexAppend, indexSegmentUpdateExecutor);
        latestItemProcessor.updateItem(indexAppend);
    }

    private void handleIndexAppend(IndexAppend indexAppend) {
        //TODO : Update the index segment Attribute as needed going forward. If eventcount for main segment is null then we are passing it as 0 in index segment.
        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                //TODO: ReplaceIfGreater
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, 1));
        ByteBufWrapper bufferWrapper = getIndexAppendBuf(indexAppend.getOffsetLength(), indexAppend.getEventCount()); // main event count
        // TODO: verify if this check is required or not
        Preconditions.checkArgument( bufferWrapper.getLength() == indexAppend.getMaxEventSize(), "The data received for index segment append is not of desired size");
        //TODO: verify if this append is what was expected and not the one with length variant(conditional append)
        store.append(getIndexSegmentName(indexAppend.getSegment()), bufferWrapper, attributes, TIMEOUT)
                .thenAccept(v -> log.info("Index segment append successful for segment {} ", indexAppend.getSegment()))
                .exceptionally(e -> {
                    log.warn("Index segment append failed for segment {} due to ", indexAppend.getSegment(), e);
                    return null;
                });
    }

    private ByteBufWrapper getIndexAppendBuf(Long eventLength, Long eventCount) {
        IndexEntry indexEntry = new IndexEntry(eventLength, eventCount, System.currentTimeMillis());
        return new ByteBufWrapper(Unpooled.wrappedBuffer( indexEntry.toBytes().getCopy()));
    }
}
