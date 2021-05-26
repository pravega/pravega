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
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.val;

/**
 * Unit tests for the {@link FixedKeyLengthTableCompactor} class.
 */
public class FixedKeyLengthTableCompactorTests extends TableCompactorTestBase {
    @Override
    protected TestContext createContext(int maxCompactionLength) {
        return new FixedKeyLengthTestContext(maxCompactionLength);
    }

    private class FixedKeyLengthTestContext extends TestContext {
        @Getter
        final FixedKeyLengthTableCompactor compactor;

        FixedKeyLengthTestContext(int maxCompactLength) {
            super();
            val config = new TableCompactor.Config(maxCompactLength);
            this.compactor = new FixedKeyLengthTableCompactor(this.segment, config, executorService());
        }

        @Override
        protected boolean hasDelayedIndexing() {
            return false;
        }

        @Override
        protected boolean areDeletesSerialized() {
            return false;
        }

        @Override
        protected void setSegmentState(long compactionOffset, long indexOffset, long entryCount, long totalEntryCount, long utilizationThreshold) {
            // IndexOffset is irrelevant for FixedKeyLengthTableSegmentLayout.
            // EntryCount is not an attribute for FixedKeyLengthTableSegmentLayout - it is derived based on number of extended attributes.
            this.segmentMetadata.updateAttributes(ImmutableMap.<AttributeId, Long>builder()
                    .put(TableAttributes.COMPACTION_OFFSET, compactionOffset)
                    .put(TableAttributes.TOTAL_ENTRY_COUNT, totalEntryCount)
                    .put(TableAttributes.MIN_UTILIZATION, utilizationThreshold)
                    .build());

            // Clear all extended attributes.
            val toRemove = this.segmentMetadata.getAttributes((id, value) -> !Attributes.isCoreAttribute(id)).keySet().stream()
                    .collect(Collectors.toMap(id -> id, id -> Attributes.NULL_ATTRIBUTE_VALUE));
            this.segmentMetadata.updateAttributes(toRemove);

            val toAdd = IntStream.range(0, (int) entryCount).boxed()
                    .collect(Collectors.toMap(i -> AttributeId.randomUUID(), i -> (long) i));
            this.segmentMetadata.updateAttributes(toAdd);
        }

        @Override
        protected void setLastIndexedOffset(long offset) {
            // No-op for FixedKeyLengthTableSegmentLayout.
        }

        @Override
        protected void index(BucketUpdate.KeyUpdate keyUpdate, long previousOffset, int length) {
            val attributes = new AttributeUpdateCollection();
            val attributeId = AttributeId.from(keyUpdate.getKey().getCopy());
            if (keyUpdate.isDeleted()) {
                // Deletions do not write anything to the segment, so we don't update the TOTAL_ENTRY_COUNT attribute.
                attributes.add(new AttributeUpdate(attributeId, AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE));
            } else {
                // Increment TOTAL_ENTRY_COUNT.
                attributes.add(new AttributeUpdate(TableAttributes.TOTAL_ENTRY_COUNT, AttributeUpdateType.Accumulate, 1));

                // Index the entry. No need for DynamicAttributeUpdate here - we already know the offset.
                attributes.add(new AttributeUpdate(attributeId, AttributeUpdateType.Replace, keyUpdate.getOffset()));
            }

            this.segment.updateAttributes(attributes, TIMEOUT).join();
        }

        @Override
        public void close() {
            super.close();
        }
    }
}
