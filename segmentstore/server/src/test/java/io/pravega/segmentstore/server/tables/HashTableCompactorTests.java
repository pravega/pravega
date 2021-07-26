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
package io.pravega.segmentstore.server.tables;

import com.google.common.collect.ImmutableMap;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMock;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Unit tests for the {@link HashTableCompactor} class.
 */
public class HashTableCompactorTests extends TableCompactorTestBase {
    private static final KeyHasher KEY_HASHER = KeyHashers.DEFAULT_HASHER;

    @Override
    protected TestContext createContext(int maxCompactionLength) {
        return new HashTestContext(maxCompactionLength);
    }

    private class HashTestContext extends TestContext {
        final TestConnector writerConnector;
        final IndexWriter indexWriter;
        @Getter
        final HashTableCompactor compactor;

        HashTestContext(int maxCompactLength) {
            super();
            this.indexWriter = new IndexWriter(KEY_HASHER, executorService());
            this.writerConnector = new TestConnector(this.segment, this.serializer, KEY_HASHER, maxCompactLength);
            val config = new TableCompactor.Config(this.writerConnector.getMaxCompactionSize());
            this.compactor = new HashTableCompactor(this.segment, config, this.indexWriter, this.writerConnector.keyHasher, executorService());
        }

        @Override
        protected boolean hasDelayedIndexing() {
            return true;
        }

        @Override
        protected boolean areDeletesSerialized() {
            return true;
        }

        @Override
        protected void setSegmentState(long compactionOffset, long indexOffset, long entryCount, long totalEntryCount, long utilizationThreshold) {
            this.segmentMetadata.updateAttributes(ImmutableMap.<AttributeId, Long>builder()
                    .put(TableAttributes.COMPACTION_OFFSET, compactionOffset)
                    .put(TableAttributes.INDEX_OFFSET, indexOffset)
                    .put(TableAttributes.ENTRY_COUNT, entryCount)
                    .put(TableAttributes.TOTAL_ENTRY_COUNT, totalEntryCount)
                    .put(TableAttributes.MIN_UTILIZATION, utilizationThreshold)
                    .build());
        }

        @Override
        protected void setLastIndexedOffset(long offset) {
            this.segmentMetadata.updateAttributes(ImmutableMap.<AttributeId, Long>builder()
                    .put(TableAttributes.INDEX_OFFSET, offset)
                    .build());
        }

        @Override
        protected void index(BucketUpdate.KeyUpdate keyUpdate, long previousOffset, int length) {
            val b = this.indexWriter.groupByBucket(this.segment, Collections.singleton(keyUpdate), this.timer).join()
                    .stream().findFirst().get();
            if (previousOffset >= 0) {
                b.withExistingKey(new BucketUpdate.KeyInfo(keyUpdate.getKey(), previousOffset, previousOffset));
            }

            this.indexWriter.updateBuckets(this.segment, Collections.singleton(b.build()), keyUpdate.getOffset(), keyUpdate.getOffset() + length, 1, TIMEOUT).join();
        }

        @Override
        public void close() {
            super.close();
            this.writerConnector.close();
        }
    }

    @Getter
    @RequiredArgsConstructor
    private static class TestConnector implements TableWriterConnector {
        private final SegmentMock segment;
        private final EntrySerializer serializer;
        private final KeyHasher keyHasher;
        private final int maxCompactLength;

        @Override
        public SegmentMetadata getMetadata() {
            return this.segment.getMetadata();
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return CompletableFuture.completedFuture(this.segment);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset, int processedSizeBytes) {
            throw new UnsupportedOperationException("not needed");
        }

        @Override
        public int getMaxCompactionSize() {
            return this.maxCompactLength;
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }
}
