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

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for {@link ContainerTableExtensionImpl} when using a {@link HashTableSegmentLayout} for its Table Segments.
 */
public class HashTableSegmentLayoutTests extends TableSegmentLayoutTestBase {
    private static final Duration THROTTLE_CHECK_TIMEOUT = Duration.ofMillis(500);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis() * 4, TimeUnit.MILLISECONDS);

    //region TableSegmentLayoutTestBase Implementation

    @Override
    protected int getKeyLength() {
        return MAX_KEY_LENGTH;
    }

    @Override
    protected void createSegment(TableContext context, String segmentName) {
        context.ext.createSegment(SEGMENT_NAME, SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
    }

    @Override
    protected Map<AttributeId, Long> getExpectedNewSegmentAttributes(TableContext context) {
        return context.ext.getConfig().getDefaultCompactionAttributes();
    }

    @Override
    protected boolean supportsDeleteIfEmpty() {
        return true;
    }

    @Override
    protected WriterTableProcessor createWriterTableProcessor(ContainerTableExtension ext, TableContext context) {
        val p = (WriterTableProcessor) ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(p);
        context.segment().setAppendCallback((offset, length) -> addToProcessor(offset, length, p));
        return p;
    }

    @Override
    protected boolean shouldExpectWriterTableProcessors() {
        return true;
    }

    @Override
    protected IteratorArgs createEmptyIteratorArgs() {
        return IteratorArgs
                .builder()
                .fetchTimeout(TIMEOUT)
                .continuationToken(new HashTableSegmentLayout.IteratorStateImpl(KeyHasher.MAX_HASH).serialize())
                .build();
    }

    @Override
    protected void checkTableAttributes(int totalUpdateCount, int totalRemoveCount, int uniqueKeyCount, TableContext context) {
        val attributes = context.segment().getInfo().getAttributes();
        val expectedTotalEntryCount = totalUpdateCount + totalRemoveCount;
        Assert.assertEquals(expectedTotalEntryCount, (long) attributes.get(TableAttributes.TOTAL_ENTRY_COUNT));
        Assert.assertEquals(uniqueKeyCount, (long) attributes.get(TableAttributes.ENTRY_COUNT));
    }

    @Override
    protected BufferView createRandomKey(TableContext context) {
        return createRandomKeyRandomLength(context);
    }

    @SneakyThrows(DataCorruptionException.class)
    private void addToProcessor(long offset, int length, WriterTableProcessor processor) {
        val op = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(new byte[length]), null);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(offset);
        processor.add(new CachedStreamSegmentAppendOperation(op));
    }

    //endregion

    /**
     * Tests the ability to perform unconditional updates using a single key at a time using a {@link KeyHasher} that is
     * very prone to collisions (in this case, it hashes everything to the same hash).
     */
    @Test
    public void testSingleUpdateUnconditionalCollisions() {
        testSingleUpdates(KeyHashers.CONSTANT_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates using a single key at a time using a {@link KeyHasher} that is
     * very prone to collisions (in this case, it hashes everything to the same hash).
     */
    @Test
    public void testSingleUpdateConditionalCollisions() {
        testSingleUpdates(KeyHashers.CONSTANT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to perform unconditional updates and removals using a {@link KeyHasher} that is very prone to
     * collisions.
     */
    @Test
    public void testBatchUpdatesUnconditionalWithCollisions() {
        testBatchUpdates(KeyHashers.COLLISION_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testBatchUpdatesConditionalWithCollisions() {
        testBatchUpdates(KeyHashers.COLLISION_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to update and access entries when compaction occurs using a {@link KeyHasher} that is very prone
     * to collisions.
     */
    @Test
    public void testCompactionWithCollisions() {
        testTableSegmentCompacted(KeyHashers.COLLISION_HASHER, this::check);
    }

    /**
     * Tests the ability to iterate over keys or entries using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testIteratorsWithCollisions() {
        testBatchUpdates(
                ITERATOR_BATCH_UPDATE_COUNT,
                ITERATOR_BATCH_UPDATE_SIZE,
                KeyHashers.COLLISION_HASHER,
                this::toUnconditionalTableEntry,
                this::toUnconditionalKey,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    /**
     * Tests the ability to resume operations after a recovery event. Scenarios include:
     * - Index is up-to-date ({@link TableAttributes#INDEX_OFFSET} equals Segment.Length.
     * - Index is not up-to-date ({@link TableAttributes#INDEX_OFFSET} is less than Segment.Length.
     */
    @Test
    public void testRecovery() throws Exception {
        // Generate a set of TestEntryData (List<TableEntry>, ExpectedResults.
        // Process each TestEntryData in turn.  After each time, re-create the Extension.
        // Verify gets are blocked on indexing. Then index, verify unblocked and then re-create the Extension, and verify again.
        val recoveryConfig = TableExtensionConfig.builder()
                .with(TableExtensionConfig.MAX_TAIL_CACHE_PREINDEX_BATCH_SIZE, (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * 11)
                .build();
        @Cleanup
        val context = new TableContext(recoveryConfig, executorService());

        // Create the Segment.
        context.ext.createSegment(SEGMENT_NAME, SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();

        // Close the initial extension, as we don't need it anymore.
        context.ext.close();

        // Generate test data (in update & remove batches).
        val data = generateTestData(context);

        // Process each such batch in turn.
        for (int i = 0; i < data.size(); i++) {
            val current = data.get(i);

            // First, add the updates from this iteration to the index, but do not flush them. The only long-term effect
            // of this is writing the data to the Segment.
            try (val ext = context.createExtension()) {
                val toUpdate = current.toUpdate
                        .entrySet().stream().map(e -> toUnconditionalTableEntry(e.getKey(), e.getValue(), 0))
                        .collect(Collectors.toList());
                ext.put(SEGMENT_NAME, toUpdate, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                val toRemove = current.toRemove.stream().map(k -> toUnconditionalKey(k, 0)).collect(Collectors.toList());
                ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            // Create a new instance of the extension (which simulates a recovery) and verify it exhibits the correct behavior.
            try (val ext = context.createExtension()) {
                // We should have unindexed data.
                long lastIndexedOffset = context.segment().getInfo().getAttributes().get(TableAttributes.INDEX_OFFSET);
                long segmentLength = context.segment().getInfo().getLength();
                AssertExtensions.assertGreaterThan("Expected some unindexed data.", lastIndexedOffset, segmentLength);

                boolean useProcessor = i % 2 == 0; // This ensures that last iteration uses the processor.

                // Verify get requests are blocked.
                val key1 = current.expectedEntries.keySet().stream().findFirst().orElse(null);
                val get1 = ext.get(SEGMENT_NAME, Collections.singletonList(key1), TIMEOUT);
                val getResult1 = get1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertEquals("Unexpected completion result for recovered get.",
                        current.expectedEntries.get(key1), getResult1.get(0).getValue());

                if (useProcessor) {
                    // Create, populate, and flush the processor.
                    @Cleanup
                    val processor = (WriterTableProcessor) ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
                    addToProcessor(lastIndexedOffset, (int) (segmentLength - lastIndexedOffset), processor);
                    processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    Assert.assertFalse("Unexpected result from WriterTableProcessor.mustFlush() after flushing.", processor.mustFlush());
                }
            }
        }

        // Verify final result. We create yet another extension here, and purposefully do not instantiate any writer processors;
        // we want to make sure the data are accessible even without that being created (since the indexing is all caught up).
        @Cleanup
        val ext2 = context.createExtension();
        check(data.get(data.size() - 1).expectedEntries, Collections.emptyList(), ext2);
    }


    /**
     * Tests throttling.
     */
    @Test
    public void testThrottling() throws Exception {
        final int keyLength = 256;
        final int valueLength = 1024 - keyLength;
        final int unthrottledCount = 9;

        // We set up throttling such that we allow 'unthrottledCount' through, but block (throttle) on the next one.
        val config = TableExtensionConfig.builder()
                .with(TableExtensionConfig.MAX_UNINDEXED_LENGTH, unthrottledCount * (keyLength + valueLength + EntrySerializer.HEADER_LENGTH))
                .build();

        val s = new EntrySerializer();
        @Cleanup
        val context = new TableContext(config, executorService());

        // Create the Segment and set up the WriterTableProcessor.
        context.ext.createSegment(SEGMENT_NAME, SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);

        // Update. We expect all these keys to go through.
        val expectedEntries = new HashMap<BufferView, BufferView>();
        val allEntries = new ArrayList<TableEntry>();
        for (int i = 0; i < unthrottledCount; i++) {
            val toUpdate = TableEntry.unversioned(createRandomData(keyLength, context), createRandomData(valueLength, context));
            context.ext.put(SEGMENT_NAME, Collections.singletonList(toUpdate), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            expectedEntries.put(toUpdate.getKey().getKey(), toUpdate.getValue());
            allEntries.add(toUpdate);
        }

        // Make sure everything we have so far checks out.
        check(expectedEntries, Collections.emptyList(), context.ext);

        // Attempt to add one more entry. This one should be blocked.
        val throttledEntry = TableEntry.unversioned(createRandomData(keyLength, context), createRandomData(valueLength, context));
        val throttledUpdate = context.ext.put(SEGMENT_NAME, Collections.singletonList(throttledEntry), TIMEOUT);
        expectedEntries.put(throttledEntry.getKey().getKey(), throttledEntry.getValue());
        allEntries.add(throttledEntry);
        AssertExtensions.assertThrows(
                "Not expected throttled update to have been accepted.",
                () -> throttledUpdate.get(THROTTLE_CHECK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof TimeoutException);

        // Simulate moving the first update through the pipeline. Add it to the WriterTableProcessor and flush it down.
        int firstEntryLength = s.getUpdateLength(allEntries.get(0));
        addToProcessor(0, firstEntryLength, processor);
        processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // The throttled update should now be unblocked.
        throttledUpdate.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Final check and delete.
        check(expectedEntries, Collections.emptyList(), context.ext);
        checkIterators(expectedEntries, context.ext);

        context.ext.deleteSegment(SEGMENT_NAME, false, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

}
