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
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link FixedKeyLengthTableCompactor} class.
 */
public class FixedKeyLengthTableCompactorTests extends TableCompactorTestBase {
    @Override
    protected TestContext createContext(int maxCompactionLength) {
        return new FixedKeyLengthTestContext(maxCompactionLength);
    }

    /**
     * Tests the case when a compaction executes concurrently with one of compact-copied keys being updated. This is a
     * scenario specific to the Fixed-Key-Length Table Segment as the indexing is done at the time of the update
     * and not in the background (and hence in sequence).
     */
    @Test
    public void testCompactionConcurrentUpdate() {
        @Cleanup
        val context = createContext(KEY_COUNT * UPDATE_ENTRY_LENGTH);
        val rnd = new Random(0);

        // Generate keys.
        val keys = new ArrayList<BufferView>();
        val expectedValues = new HashMap<BufferView, BufferView>();
        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = new byte[KEY_LENGTH];
            rnd.nextBytes(key);
            keys.add(new ByteArraySegment(key));
        }

        // Set utilization threshold to 76% so that we may trigger a compaction when we update half the keys once.
        context.setSegmentState(0, 0, 0, 0, 76);

        // Insert all the keys ...
        for (val key : keys) {
            expectedValues.put(key, updateKey(key, context, rnd));
        }

        //... then update the second half. This should require a compaction which results in a copy.
        for (int i = keys.size() / 2; i < keys.size(); i++) {
            expectedValues.put(keys.get(i), updateKey(keys.get(i), context, rnd));
        }

        val originalLength = context.segmentMetadata.getLength();
        val c = context.getCompactor();
        Assert.assertEquals("Unexpected number of unique entries pre-compaction.",
                expectedValues.size(), (long) c.getUniqueEntryCount().join());
        Assert.assertEquals("Unexpected number of total entries pre-compaction.",
                expectedValues.size() + expectedValues.size() / 2, IndexReader.getTotalEntryCount(context.segmentMetadata));
        Assert.assertTrue("Expecting a compaction to be required.", c.isCompactionRequired().join());

        context.segment.setBeforeAppendCallback(() -> {
            // This callback is invoked while the compactor is running; it is after it has read and processed all candidates
            // and immediately before the conditional append it performs is about to be executed.
            // We can now update one of the keys that are copied with a new value.
            context.segment.setBeforeAppendCallback(null); // Make sure we don't end up in an infinite loop here.
            val firstKey = keys.get(0);
            expectedValues.put(firstKey, updateKey(firstKey, context, rnd));
        });
        c.compact(new TimeoutTimer(TIMEOUT)).join();

        // We should now verify that the compaction did eventually succeed and that all the keys have the correct (expected) values.
        AssertExtensions.assertGreaterThan("Segment length did not change.", originalLength, context.segmentMetadata.getLength());
        AssertExtensions.assertGreaterThan("No compaction occurred.", 0, IndexReader.getCompactionOffset(context.segmentMetadata));
        Assert.assertEquals("Unexpected number of unique entries post-compaction.",
                expectedValues.size(), (long) c.getUniqueEntryCount().join());
        Assert.assertEquals("Unexpected number of total entries post-compaction.",
                expectedValues.size(), IndexReader.getTotalEntryCount(context.segmentMetadata));

        // Read all the entries from the segment and validate that they are as expected.
        val actualValues = new HashMap<BufferView, BufferView>();
        context.segment.attributeIterator(AttributeId.Variable.minValue(KEY_LENGTH), AttributeId.Variable.maxValue(KEY_LENGTH), TIMEOUT).join()
                .forEachRemaining(attributeValues -> {
                    for (val av : attributeValues) {
                        val reader = BufferView.wrap(context.segment.read(av.getValue(), UPDATE_ENTRY_LENGTH, TIMEOUT)
                                .readRemaining(UPDATE_ENTRY_LENGTH, TIMEOUT))
                                .getBufferViewReader();
                        try {
                            val e = AsyncTableEntryReader.readEntryComponents(reader, av.getValue(), context.serializer);
                            Assert.assertEquals("Mismatch keys.", av.getKey().toBuffer(), e.getKey());
                            actualValues.put(e.getKey(), e.getValue());
                        } catch (SerializationException ex) {
                            throw new CompletionException(ex);
                        }

                    }
                }, executorService()).join();

        AssertExtensions.assertMapEquals("Unexpected entries in the segment after compaction.", expectedValues, actualValues);
    }

    private BufferView updateKey(BufferView key, TestContext context, Random rnd) {
        byte[] valueData = new byte[VALUE_LENGTH];
        rnd.nextBytes(valueData);
        val value = new ByteArraySegment(valueData);

        // Serialize and append it to the segment, then index it.
        val entry = TableEntry.unversioned(key, value);
        val serialization = context.serializer.serializeUpdate(Collections.singleton(entry));
        val offset = context.segment.append(serialization, null, TIMEOUT).join();

        val keyUpdate = new BucketUpdate.KeyUpdate(key, offset, offset, false);
        context.index(keyUpdate, -1L, serialization.getLength());
        return value;
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
