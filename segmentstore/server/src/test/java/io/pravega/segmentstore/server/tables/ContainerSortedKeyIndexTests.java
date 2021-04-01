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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.TableStoreMock;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ContainerSortedKeyIndex} and {@link SegmentSortedKeyIndexImpl}.
 */
public class ContainerSortedKeyIndexTests extends ThreadPooledTestSuite {
    private static final long SEGMENT_ID = 1L;
    private static final SegmentProperties NON_SORTED_INFO = StreamSegmentInformation.builder().name("S").build();
    private static final SegmentProperties SORTED_INFO = StreamSegmentInformation.builder().name("S")
            .attributes(Collections.singletonMap(TableAttributes.SORTED, Attributes.BOOLEAN_TRUE)).build();
    private static final int BATCH_COUNT = 100;
    private static final int MAX_ITEMS_PER_BATCH = 10;
    private static final int MAX_KEY_SIZE = 128;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests {@link ContainerSortedKeyIndex#isSortedTableSegment}.
     */
    @Test
    public void testIsSortedTableSegment() {
        Assert.assertFalse(ContainerSortedKeyIndex.isSortedTableSegment(NON_SORTED_INFO));
        Assert.assertFalse(ContainerSortedKeyIndex.isSortedTableSegment(StreamSegmentInformation.builder()
                .name("S").attributes(Collections.singletonMap(TableAttributes.SORTED, Attributes.BOOLEAN_FALSE)).build()));
        Assert.assertTrue(ContainerSortedKeyIndex.isSortedTableSegment(SORTED_INFO));
    }

    /**
     * Tests that {@link ContainerSortedKeyIndex#getSortedKeyIndex} returns {@link SegmentSortedKeyIndex#noop()} for a
     * non-sorted segment.
     */
    @Test
    public void testGetSortedKeyIndexNonSorted() {
        val context = new TestContext();
        val nonSorted = context.containerIndex.getSortedKeyIndex(SEGMENT_ID, NON_SORTED_INFO);
        nonSorted.includeTailCache(Collections.singletonMap(new ByteArraySegment(new byte[]{1}), new CacheBucketOffset(1, false)));
        val iterator = nonSorted.iterator(nonSorted.getIteratorRange(null, null), TIMEOUT).getNext().join();
        Assert.assertNull(iterator);
    }

    /**
     * Tests {@link SegmentSortedKeyIndex#includeTailUpdate}, {@link SegmentSortedKeyIndex#includeTailCache} and
     * {@link ContainerSortedKeyIndex#notifyIndexOffsetChanged}.
     */
    @Test
    public void testIncludeTailUpdate() {
        val context = new TestContext();
        val testItems = generateTestData();

        for (val t : testItems) {
            context.segmentIndex.includeTailUpdate(t.batch, t.batchOffset);
            val allKeys = context.getAllKeys();
            AssertExtensions.assertListEquals("", t.expectedItems, allKeys, BufferView::equals);
        }
    }

    /**
     * Tests {@link SegmentSortedKeyIndex#includeTailCache} and {@link ContainerSortedKeyIndex#notifyIndexOffsetChanged}.
     */
    @Test
    public void testIncludeTailCache() {
        val context = new TestContext();
        val testItems = generateTestData(BATCH_COUNT, 0.25); // We want to have some items left at the end.

        // Index the keys by their offsets.
        val map = new HashMap<BufferView, CacheBucketOffset>();
        testItems.forEach(t ->
                t.batch.getItems().stream().filter(item -> !SortedKeyIndexDataSource.INTERNAL_TRANSLATOR.isInternal(item.getKey()))
                        .forEach(item -> map.put(item.getKey().getKey(),
                                new CacheBucketOffset(t.batchOffset + item.getOffset(), t.batch.isRemoval()))));

        // Check includeTailCache.
        context.segmentIndex.includeTailCache(map);
        val expectedItems = testItems.get(testItems.size() - 1).expectedItems;
        val allKeys = context.getAllKeys();
        AssertExtensions.assertListEquals("After calling includeTailCache().", expectedItems, allKeys, BufferView::equals);

        // Check notifyOffsetChanged. It's easier to check here since we have already indexed the keys by offsets.
        // Sort surviving keys by offset.
        val keysWithOffsets = map.entrySet().stream()
                .filter(e -> !e.getValue().isRemoval())
                .sorted(Comparator.comparingLong(e -> e.getValue().getSegmentOffset()))
                .collect(Collectors.toList());
        Assert.assertFalse("Test error: nothing to test.", keysWithOffsets.isEmpty());

        int firstIndex = 0;
        for (val t : testItems) {
            context.containerIndex.notifyIndexOffsetChanged(SEGMENT_ID, t.batchOffset);
            while (firstIndex < keysWithOffsets.size() && keysWithOffsets.get(firstIndex).getValue().getSegmentOffset() < t.batchOffset) {
                firstIndex++;
            }

            List<ArrayView> expectedKeys = Collections.emptyList();
            if (firstIndex < keysWithOffsets.size()) {
                expectedKeys = keysWithOffsets
                        .subList(firstIndex, keysWithOffsets.size())
                        .stream()
                        .map(e -> (ArrayView) e.getKey())
                        .sorted(SegmentSortedKeyIndexImpl.KEY_COMPARATOR)
                        .collect(Collectors.toList());
            }

            val actualKeys = context.getAllKeys();
            AssertExtensions.assertListEquals("After trimming to offset " + t.batchOffset, expectedKeys, actualKeys, BufferView::equals);
        }
    }

    /**
     * Tests {@link SegmentSortedKeyIndex#persistUpdate}.
     */
    @Test
    public void testPersistedKeys() {
        val context = new TestContext();
        val testItems = generateTestData();

        // Test using increasing batch sizes.
        int i = 0;
        int batchSize = 1;
        while (i < testItems.size()) {
            int next = Math.min(testItems.size(), i + batchSize);
            val bucketUpdates = testItems.subList(i, next).stream().map(TestItem::getBucketUpdate).collect(Collectors.toList());
            context.segmentIndex.persistUpdate(bucketUpdates, TIMEOUT).join();

            val expectedKeys = testItems.get(next - 1).expectedItems;
            val allKeys = context.getAllKeys();
            AssertExtensions.assertListEquals("", expectedKeys, allKeys, BufferView::equals);
            i = next;
            batchSize++;
        }
    }

    /**
     * Tests all methods (a mix of tail keys and persisted keys) - verifies iterators can be mixed correctly.
     */
    @Test
    public void testMixed() {
        val context = new TestContext();
        val testItems = generateTestData(2 * BATCH_COUNT, 0.4);

        // First, include the items as tail updates - this is how ContainerTableExtension works.
        for (val t : testItems) {
            context.segmentIndex.includeTailUpdate(t.batch, t.batchOffset);
        }

        // Then, include the result in the index and notify it that the last offset has changed.
        val expectedKeys = testItems.get(testItems.size() - 1).expectedItems;
        for (int i = 0; i < testItems.size(); i++) {
            val t = testItems.get(i);
            val bucketUpdate = t.getBucketUpdate();
            context.segmentIndex.persistUpdate(Collections.singleton(bucketUpdate), TIMEOUT).join();
            val actualKeys = context.getAllKeys();

            AssertExtensions.assertListEquals("When overlapping index " + i, expectedKeys, actualKeys, BufferView::equals);

            long lastIndexedOffset;
            if (i == testItems.size() - 1) {
                lastIndexedOffset = -1L;
                context.segmentIndex.updateSegmentIndexOffset(lastIndexedOffset); // Also invoke it here.
            } else {
                lastIndexedOffset = testItems.get(i + 1).batchOffset;
            }

            context.containerIndex.notifyIndexOffsetChanged(SEGMENT_ID, lastIndexedOffset);
            AssertExtensions.assertListEquals("After calling notifyIndexOffsetChanged for index " + i,
                    expectedKeys, actualKeys, BufferView::equals);
        }
    }

    /**
     * Tests iterators with custom arguments.
     */
    @Test
    public void testIteratorsPrefix() {
        val maxPrefixValue = 10;
        val context = new TestContext();
        // Add a prefix between 0 and maxPrefixValue to each key.
        Function<byte[], byte[]> addPrefix = array -> {
            val result = new byte[array.length + 1];
            result[0] = (byte) (array.length % maxPrefixValue);
            System.arraycopy(array, 0, result, 1, array.length);
            return result;
        };
        val testItems = generateTestData(2 * BATCH_COUNT, 0.4, addPrefix);

        // First half is persisted.
        int halfIndex = testItems.size() / 2;
        for (int i = 0; i < halfIndex; i++) {
            val t = testItems.get(i);
            val bucketUpdate = t.getBucketUpdate();
            context.segmentIndex.persistUpdate(Collections.singleton(bucketUpdate), TIMEOUT).join();
        }

        // Second half is not.
        context.containerIndex.notifyIndexOffsetChanged(SEGMENT_ID, testItems.get(halfIndex).batchOffset);
        for (int i = halfIndex; i < testItems.size(); i++) {
            context.segmentIndex.includeTailUpdate(testItems.get(i).batch, testItems.get(i).batchOffset);
        }

        val allExpectedItems = testItems.get(testItems.size() - 1).expectedItems;
        AssertExtensions.assertListEquals("Boundless iterator.", allExpectedItems, context.getAllKeys(), BufferView::equals);
        for (int i = 0; i < maxPrefixValue; i++) {
            // Check prefix iterators, without a start bound (initial iterators).
            byte prefixByte = (byte) i;
            val prefix = SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR.inbound(new ByteArraySegment(new byte[]{prefixByte}));
            val expectedItems = allExpectedItems.stream().filter(a -> isPrefixOf(toArrayView(prefix), a)).collect(Collectors.toList());
            val ir = context.segmentIndex.getIteratorRange(null, prefix);
            val actualItems = context.getKeys(context.segmentIndex.iterator(ir, TIMEOUT));
            AssertExtensions.assertListEquals("Iterator with prefix " + prefixByte,
                    expectedItems, actualItems, BufferView::equals);

            // Check prefix iterators, with a start bound (resumed iterators).
            for (int j = 0; j < expectedItems.size(); j++) {
                val fromExclusive = expectedItems.get(j);
                val expectedPartialResult = expectedItems.subList(j + 1, expectedItems.size());

                val partialRange = context.segmentIndex.getIteratorRange(fromExclusive, prefix);
                val partialResultItems = context.getKeys(context.segmentIndex.iterator(partialRange, TIMEOUT));

                AssertExtensions.assertListEquals("Resumed iterator with prefix " + prefixByte,
                        expectedPartialResult, partialResultItems, BufferView::equals);
            }
        }
    }

    private boolean isPrefixOf(ArrayView prefix, ArrayView array) {
        for (int i = 0; i < prefix.getLength(); i++) {
            if (prefix.get(i) != array.get(i)) {
                return false;
            }
        }
        return true;
    }

    private ArrayView toArrayView(BufferView b) {
        return b instanceof ArrayView ? (ArrayView) b : new ByteArraySegment(b.getCopy());
    }

    private List<TestItem> generateTestData() {
        return generateTestData(BATCH_COUNT, 0.5);
    }

    private List<TestItem> generateTestData(int batchCount, double removeProbability) {
        return generateTestData(batchCount, removeProbability, Function.identity());
    }

    private List<TestItem> generateTestData(int batchCount, double removeProbability, Function<byte[], byte[]> newKeyConverter) {
        val rnd = new Random(0);
        val result = new ArrayList<TestItem>();
        val currentItems = new TreeSet<ArrayView>(SegmentSortedKeyIndexImpl.KEY_COMPARATOR);
        long currentOffset = 0;
        for (int batchId = 0; batchId < batchCount; batchId++) {
            boolean isRemoval = !currentItems.isEmpty() && (rnd.nextDouble() < removeProbability);
            val batch = isRemoval ? TableKeyBatch.removal() : TableKeyBatch.update();
            val batchSize = Math.max(1, rnd.nextInt(MAX_ITEMS_PER_BATCH));
            val batchItems = new ArrayList<ArrayView>();
            val previousItems = new HashSet<>(currentItems);

            val shuffledItems = new ArrayList<>(currentItems);
            Collections.shuffle(shuffledItems, rnd);
            if (isRemoval) {
                int removeCount = Math.min(batchSize, shuffledItems.size());
                batchItems.addAll(shuffledItems.subList(0, removeCount));
                currentItems.removeAll(batchItems);
            } else {
                // Insert some duplicates.
                val duplicateCount = Math.min(batchSize / 2, shuffledItems.size());
                batchItems.addAll(shuffledItems.subList(0, duplicateCount));

                // Insert new items.
                val uniqueCount = batchSize - duplicateCount;
                for (int i = 0; i < uniqueCount; i++) {
                    byte[] key = new byte[1 + rnd.nextInt(MAX_KEY_SIZE)];
                    rnd.nextBytes(key);
                    key = newKeyConverter.apply(key);

                    // ContainerTableExtensionImpl translates the keys prior to sending them to the SortedKeyIndex.
                    // Simulate that behavior here.
                    batchItems.add(toArrayView(SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR.inbound(new ByteArraySegment(key))));
                }
                currentItems.addAll(batchItems);
            }

            // Compile the batch and store it, along with its offset and a snapshot of the data after it has been applied.
            batchItems.stream().map(TableKey::unversioned).forEach(k -> batch.add(k, UUID.randomUUID(), k.getKey().getLength()));

            // We also want to make sure that we can ignore our own keys.
            batchItems.stream().map(TableKey::unversioned)
                    .map(SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR::outbound)
                    .map(SortedKeyIndexDataSource.INTERNAL_TRANSLATOR::inbound)
                    .forEach(k -> batch.add(k, UUID.randomUUID(), k.getKey().getLength()));

            result.add(new TestItem(batch, currentOffset, new ArrayList<>(currentItems), previousItems));

            // Update the offset.
            currentOffset += batchItems.stream().mapToInt(ArrayView::getLength).sum();
        }

        return result;
    }

    @RequiredArgsConstructor
    private static class TestItem {
        final TableKeyBatch batch;
        final long batchOffset;
        final List<ArrayView> expectedItems;
        final Set<ArrayView> previousItems;

        BucketUpdate getBucketUpdate() {
            val result = BucketUpdate.forBucket(new TableBucket(UUID.randomUUID(), this.batchOffset));
            this.previousItems.forEach(key -> result.withExistingKey(new BucketUpdate.KeyInfo(key, 0, 0)));

            // When generating updates, it is important that the version matches the key offset. If the version is smaller
            // the BucketUpdate builder will ignore it as it will believe it was copied over by compaction.
            val uniqueKeys = new HashSet<BufferView>();
            for (val item : this.batch.getItems()) {
                val key = item.getKey().getKey();
                if (uniqueKeys.add(key)) {
                    result.withKeyUpdate(new BucketUpdate.KeyUpdate(item.getKey().getKey(),
                            this.batchOffset + item.getOffset(), this.batchOffset + item.getOffset(), this.batch.isRemoval()));
                }
            }

            return result.build();
        }
    }

    private class TestContext {
        final TableStoreMock mockStore;
        final SortedKeyIndexDataSource dataSource;
        final ContainerSortedKeyIndex containerIndex;
        final SegmentSortedKeyIndex segmentIndex;
        final SegmentSortedKeyIndex.IteratorRange fullRange;

        TestContext() {
            this.mockStore = new TableStoreMock(executorService());
            this.mockStore.createSegment(SORTED_INFO.getName(), SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
            this.dataSource = new SortedKeyIndexDataSource(this.mockStore::put, this.mockStore::remove, this.mockStore::get);
            this.containerIndex = new ContainerSortedKeyIndex(this.dataSource, executorService());
            this.segmentIndex = this.containerIndex.getSortedKeyIndex(SEGMENT_ID, SORTED_INFO);
            this.fullRange = this.segmentIndex.getIteratorRange(null, null);
        }

        List<ArrayView> getAllKeys() {
            return getKeys(this.segmentIndex.iterator(this.fullRange, TIMEOUT));
        }

        List<ArrayView> getKeys(AsyncIterator<List<BufferView>> iterator) {
            val result = new ArrayList<ArrayView>();
            iterator.forEachRemaining(items -> items.forEach(i -> result.add(toArrayView(i))), executorService()).join();
            return result;
        }
    }
}
