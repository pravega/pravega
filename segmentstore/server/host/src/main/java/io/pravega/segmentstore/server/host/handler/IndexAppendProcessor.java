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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.LatestItemSequentialProcessor;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.NameUtils.getIndexSegmentName;

@Slf4j
public class IndexAppendProcessor implements AutoCloseable {
    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final ScheduledExecutorService indexSegmentUpdateExecutor;
    private final StreamSegmentStore store;

    public IndexAppendProcessor(ScheduledExecutorService indexSegmentUpdateExecutor, StreamSegmentStore store) {
        this.indexSegmentUpdateExecutor = indexSegmentUpdateExecutor;
        this.store = store;
    }

    protected void processAppend(String segmentName, int eventCount) {
        IndexInfo indexInfo = new IndexInfo(segmentName, eventCount);
        LatestItemSequentialProcessor<IndexInfo> latestItemProcessor = new LatestItemSequentialProcessor<>(this::handleIndexAppend, indexSegmentUpdateExecutor);
        latestItemProcessor.updateItem(indexInfo);
    }

    private void handleIndexAppend(IndexInfo indexInfo) {
        //TODO : Update the index segment Attribute as needed going forward.
        // Keeping Event count of index segment same as main segment event count, we will not be updating index segment for each main segment append.
        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, indexInfo.getEventCount()));
        String segmentName = indexInfo.getSegmentName();
        store.getStreamSegmentInfo(segmentName, TIMEOUT)
                .thenCompose(info -> store.append(getIndexSegmentName(segmentName), getIndexAppendBuf(info.getLength(), info.getAttributes().get(EVENT_COUNT)), attributes, TIMEOUT))
                .whenComplete((length, ex) -> {
                    if (ex == null) {
                        log.trace("Index segment append successful for Segment : {} with length {} ", segmentName, length);
                    } else {
                        log.warn("Index segment append failed for segment {} due to ", segmentName, ex);
                    }
                });
    }

    private ByteBufWrapper getIndexAppendBuf(Long eventLength, Long eventCount) {
        IndexAppend indexAppend = new IndexAppend(eventLength, eventCount, System.currentTimeMillis());
        return new ByteBufWrapper(Unpooled.wrappedBuffer( indexAppend.toBytes()));
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(indexSegmentUpdateExecutor);
    }

    @Data
    private class IndexInfo {
        final String segmentName;
        final int eventCount;
    }

}