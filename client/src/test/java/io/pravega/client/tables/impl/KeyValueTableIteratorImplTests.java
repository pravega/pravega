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
package io.pravega.client.tables.impl;

import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableIterator;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link KeyValueTableIteratorImpl} class.
 */
public class KeyValueTableIteratorImplTests extends LeakDetectorTestSuite {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(4)
            .primaryKeyLength(8)
            .secondaryKeyLength(4)
            .build();
    private static final int TOTAL_KEY_LENGTH = DEFAULT_CONFIG.getTotalKeyLength();
    private static final KeyValueTableConfiguration NO_SK_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(DEFAULT_CONFIG.getPartitionCount())
            .primaryKeyLength(TOTAL_KEY_LENGTH)
            .secondaryKeyLength(0)
            .build();
    private static final TableEntryHelper DEFAULT_ENTRY_HELPER = new TableEntryHelper(mock(SegmentSelector.class), DEFAULT_CONFIG);
    private final Random random = new Random(0);

    /**
     * Tests the iterator when there is a single segment involved.
     */
    @Test
    public void testIteratorSingleSegment() {
        val pk = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength());
        val sk1 = newBuffer(DEFAULT_CONFIG.getSecondaryKeyLength());
        val sk2 = newBuffer(DEFAULT_CONFIG.getSecondaryKeyLength());
        val maxIterationSize = 10;

        val allEntries = IntStream.range(0, maxIterationSize)
                .mapToObj(i -> new AbstractMap.SimpleImmutableEntry<>(newBuffer(TOTAL_KEY_LENGTH), newBuffer(5)))
                .collect(Collectors.toList());

        val mockSegment = mock(TableSegment.class);
        when(mockSegment.keyIterator(any()))
                .thenAnswer(arg -> {
                    val iteratorArgs = (SegmentIteratorArgs) arg.getArgument(0);
                    checkSegmentIteratorArgs(iteratorArgs, pk, sk1, sk2, maxIterationSize);
                    val keys = allEntries.stream()
                            .map(e -> new TableSegmentKey(Unpooled.wrappedBuffer(e.getKey()), TableSegmentKeyVersion.NO_VERSION))
                            .collect(Collectors.toList());
                    return AsyncIterator.singleton(new IteratorItem<>(keys));
                });
        when(mockSegment.entryIterator(any()))
                .thenAnswer(arg -> {
                    val iteratorArgs = (SegmentIteratorArgs) arg.getArgument(0);
                    checkSegmentIteratorArgs(iteratorArgs, pk, sk1, sk2, maxIterationSize);
                    val entries = allEntries.stream()
                            .map(e -> new TableSegmentEntry(new TableSegmentKey(Unpooled.wrappedBuffer(e.getKey()), TableSegmentKeyVersion.NO_VERSION),
                                    Unpooled.wrappedBuffer(e.getValue())))
                            .collect(Collectors.toList());
                    return AsyncIterator.singleton(new IteratorItem<>(entries));
                });
        val selector = mock(SegmentSelector.class);
        when(selector.getKvt()).thenReturn(KVT);
        when(selector.getSegmentCount()).thenReturn(DEFAULT_CONFIG.getPartitionCount());
        val segmentRequests = new HashSet<ByteBuffer>();
        when(selector.getTableSegment(any()))
                .thenAnswer(arg -> {
                    val key = (ByteBuffer) arg.getArgument(0);
                    segmentRequests.add(key.duplicate());
                    return mockSegment;
                });
        val entryHelper = new TableEntryHelper(selector, DEFAULT_CONFIG);
        val b = new KeyValueTableIteratorImpl.Builder(DEFAULT_CONFIG, entryHelper, executorService())
                .maxIterationSize(maxIterationSize);

        // Everything up until here has been verified in the Builder unit tests (no need to do it again).
        // Create a Primary Key range iterator.
        val iterator = b.forPrimaryKey(pk, sk1, sk2);

        // Issue a Key iterator and then an Entry iterator, then collect all the results.
        val iteratorKeys = new ArrayList<TableKey>();
        iterator.keys().collectRemaining(ii -> iteratorKeys.addAll(ii.getItems())).join();
        val iteratorEntries = new ArrayList<TableEntry>();
        iterator.entries().collectRemaining(ii -> iteratorEntries.addAll(ii.getItems())).join();

        // Validate the results are as expected.
        Assert.assertEquals(allEntries.size(), iteratorKeys.size());
        Assert.assertEquals(allEntries.size(), iteratorEntries.size());
        for (int i = 0; i < allEntries.size(); i++) {
            val expected = allEntries.get(i);
            val actualKey = iteratorKeys.get(i);
            val actualEntry = iteratorEntries.get(i);

            Assert.assertEquals(Unpooled.wrappedBuffer(expected.getKey()), entryHelper.serializeKey(actualKey.getPrimaryKey(), actualKey.getSecondaryKey()));
            Assert.assertEquals(actualKey, actualEntry.getKey());
            Assert.assertEquals(expected.getValue(), actualEntry.getValue());
        }

        Assert.assertEquals("Only expecting 1 segment to be requested.", 1, segmentRequests.size());
    }

    @Test
    public void testGlobalIterators() {
        val pk1 = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength());
        val pk2 = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength());
        val maxIterationSize = 10;

        val c = new KeyValueTableIteratorImpl.TableKeyComparator();
        val allEntries = IntStream.range(0, maxIterationSize)
                .mapToObj(i -> new AbstractMap.SimpleImmutableEntry<>(newBuffer(TOTAL_KEY_LENGTH), newBuffer(5)))
                .sorted((e1, e2) -> c.compare(e1.getKey(), e2.getKey()))
                .collect(Collectors.toList());

        val mockSegment = mock(TableSegment.class);
        when(mockSegment.keyIterator(any()))
                .thenAnswer(arg -> {
                    val iteratorArgs = (SegmentIteratorArgs) arg.getArgument(0);
                    checkSegmentIteratorArgs(iteratorArgs, pk1, pk2, maxIterationSize);
                    val keys = allEntries.stream()
                            .map(e -> new TableSegmentKey(Unpooled.wrappedBuffer(e.getKey()), TableSegmentKeyVersion.NO_VERSION))
                            .collect(Collectors.toList());
                    return AsyncIterator.singleton(new IteratorItem<>(keys));
                });
        when(mockSegment.entryIterator(any()))
                .thenAnswer(arg -> {
                    val iteratorArgs = (SegmentIteratorArgs) arg.getArgument(0);
                    checkSegmentIteratorArgs(iteratorArgs, pk1, pk2, maxIterationSize);
                    val entries = allEntries.stream()
                            .map(e -> new TableSegmentEntry(new TableSegmentKey(Unpooled.wrappedBuffer(e.getKey()), TableSegmentKeyVersion.NO_VERSION),
                                    Unpooled.wrappedBuffer(e.getValue())))
                            .collect(Collectors.toList());
                    return AsyncIterator.singleton(new IteratorItem<>(entries));
                });
        val selector = mock(SegmentSelector.class);
        when(selector.getKvt()).thenReturn(KVT);
        when(selector.getSegmentCount()).thenReturn(DEFAULT_CONFIG.getPartitionCount());
        when(selector.getAllTableSegments())
                .thenAnswer(arg -> IntStream.range(0, DEFAULT_CONFIG.getPartitionCount()).mapToObj(i -> mockSegment).collect(Collectors.toList()));
        val entryHelper = new TableEntryHelper(selector, DEFAULT_CONFIG);
        val b = new KeyValueTableIteratorImpl.Builder(DEFAULT_CONFIG, entryHelper, executorService())
                .maxIterationSize(maxIterationSize);

        // Everything up until here has been verified in the Builder unit tests (no need to do it again).
        // Create a Primary Key range iterator.
        val iterator = b.forRange(pk1, pk2);

        // Issue a Key iterator and then an Entry iterator, then collect all the results.
        val iteratorKeys = new ArrayList<TableKey>();
        iterator.keys().collectRemaining(ii -> iteratorKeys.addAll(ii.getItems())).join();
        val iteratorEntries = new ArrayList<TableEntry>();
        iterator.entries().collectRemaining(ii -> iteratorEntries.addAll(ii.getItems())).join();

        // Validate the results are as expected.
        Assert.assertEquals(allEntries.size() * DEFAULT_CONFIG.getPartitionCount(), iteratorKeys.size());
        Assert.assertEquals(allEntries.size() * DEFAULT_CONFIG.getPartitionCount(), iteratorEntries.size());
        for (int i = 0; i < allEntries.size(); i++) {
            val expected = allEntries.get(i);
            for (int p = 0; p < DEFAULT_CONFIG.getPartitionCount(); p++) {
                val actualKey = iteratorKeys.get(i * DEFAULT_CONFIG.getPartitionCount() + p);
                val actualEntry = iteratorEntries.get(i * DEFAULT_CONFIG.getPartitionCount() + p);

                Assert.assertEquals(Unpooled.wrappedBuffer(expected.getKey()), entryHelper.serializeKey(actualKey.getPrimaryKey(), actualKey.getSecondaryKey()));
                Assert.assertEquals(actualKey, actualEntry.getKey());
                Assert.assertEquals(expected.getValue(), actualEntry.getValue());
            }
        }
    }

    private void checkSegmentIteratorArgs(SegmentIteratorArgs iteratorArgs, ByteBuffer pk, ByteBuffer sk1, ByteBuffer sk2, int maxIterationSize) {
        Assert.assertEquals(maxIterationSize, iteratorArgs.getMaxItemsAtOnce());
        Assert.assertEquals(TOTAL_KEY_LENGTH, iteratorArgs.getFromKey().readableBytes());
        Assert.assertEquals(TOTAL_KEY_LENGTH, iteratorArgs.getToKey().readableBytes());
        val fromPK = iteratorArgs.getFromKey().slice(0, DEFAULT_CONFIG.getPrimaryKeyLength()).nioBuffer();
        val fromSK = iteratorArgs.getFromKey().slice(DEFAULT_CONFIG.getPrimaryKeyLength(), DEFAULT_CONFIG.getSecondaryKeyLength()).nioBuffer();
        val toPK = iteratorArgs.getToKey().slice(0, DEFAULT_CONFIG.getPrimaryKeyLength()).nioBuffer();
        val toSK = iteratorArgs.getToKey().slice(DEFAULT_CONFIG.getPrimaryKeyLength(), DEFAULT_CONFIG.getSecondaryKeyLength()).nioBuffer();
        Assert.assertEquals(pk, fromPK);
        Assert.assertEquals(pk, toPK);
        Assert.assertEquals(sk1, fromSK);
        Assert.assertEquals(sk2, toSK);
    }

    private void checkSegmentIteratorArgs(SegmentIteratorArgs iteratorArgs, ByteBuffer pk1, ByteBuffer pk2, int maxIterationSize) {
        Assert.assertEquals(maxIterationSize, iteratorArgs.getMaxItemsAtOnce());
        Assert.assertEquals(TOTAL_KEY_LENGTH, iteratorArgs.getFromKey().readableBytes());
        Assert.assertEquals(TOTAL_KEY_LENGTH, iteratorArgs.getToKey().readableBytes());
        val fromPK = iteratorArgs.getFromKey().slice(0, DEFAULT_CONFIG.getPrimaryKeyLength()).nioBuffer();
        val fromSK = iteratorArgs.getFromKey().slice(DEFAULT_CONFIG.getPrimaryKeyLength(), DEFAULT_CONFIG.getSecondaryKeyLength()).nioBuffer();
        val toPK = iteratorArgs.getToKey().slice(0, DEFAULT_CONFIG.getPrimaryKeyLength()).nioBuffer();
        val toSK = iteratorArgs.getToKey().slice(DEFAULT_CONFIG.getPrimaryKeyLength(), DEFAULT_CONFIG.getSecondaryKeyLength()).nioBuffer();
        Assert.assertEquals(pk1, fromPK);
        Assert.assertEquals(pk2, toPK);

        for (int i = 0; i < DEFAULT_CONFIG.getSecondaryKeyLength(); i++) {
            Assert.assertEquals(0, fromSK.get(i));
            Assert.assertEquals(0xFF, toSK.get(i) & 0xFF);
        }
    }

    /**
     * Tests the {@link KeyValueTableIteratorImpl.MergeAsyncIterator} class.
     */
    @Test
    public void testMergeAsyncIterator() {
        val segmentCount = 5;
        val minItemsPerSegment = 11;
        val maxItemsPerSegment = 101;
        val iterationSize = 3;

        // Generate test data.
        val c = new KeyValueTableIteratorImpl.TableKeyComparator();
        val segmentIterators = new ArrayList<AsyncIterator<IteratorItem<TableKey>>>(); // Sorted.
        val expectedData = new ArrayList<TableKey>(); // Sorted.
        for (int i = 0; i < segmentCount; i++) {
            val count = minItemsPerSegment + random.nextInt(maxItemsPerSegment - minItemsPerSegment);
            val segmentBatches = new ArrayList<List<TableKey>>();

            // Generate segment contents.
            val segmentSortedContents = IntStream.range(0, count)
                    .mapToObj(x -> new TableKey(newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength()), newBuffer(DEFAULT_CONFIG.getSecondaryKeyLength())))
                    .sorted(c)
                    .collect(Collectors.toList());

            // Break it down into batches and create a "segment iterator" from them.
            int index = 0;
            while (index < count) {
                int batchCount = Math.min(iterationSize, count - index);
                segmentBatches.add(segmentSortedContents.subList(index, index + batchCount));
                index += batchCount;
            }
            segmentIterators.add(createAsyncIterator(segmentBatches));
            expectedData.addAll(segmentSortedContents);
        }

        expectedData.sort(c);

        // Create a merge iterator and collect its contents.
        val mergeIterator = new KeyValueTableIteratorImpl.MergeAsyncIterator<>(segmentIterators.iterator(), k -> k, iterationSize, executorService());
        val actualData = new ArrayList<TableKey>();
        mergeIterator.collectRemaining(ii -> {
            val expected = Math.min(iterationSize, expectedData.size() - actualData.size());
            Assert.assertEquals(expected, ii.getItems().size());
            return actualData.addAll(ii.getItems());
        });

        // Verify it returns the correct items.
        AssertExtensions.assertListEquals("", expectedData, actualData, TableKey::equals);
    }

    /**
     * Tests the {@link KeyValueTableIteratorImpl.PeekingIterator} class.
     */
    @Test
    public void testPeekingIterator() {
        val items = Arrays.asList(
                IntStream.range(0, 10).boxed().collect(Collectors.toList()),
                IntStream.range(10, 11).boxed().collect(Collectors.toList()),
                IntStream.range(11, 20).boxed().collect(Collectors.toList()));
        val sourceIterator = createAsyncIterator(items);

        Function<Integer, TableKey> toKey = i -> new TableKey(ByteBuffer.allocate(Integer.BYTES).putInt(0, i));

        // Collect items via the flattened iterator.
        val fi = new KeyValueTableIteratorImpl.PeekingIterator<>(sourceIterator, toKey);
        val actualItems = new ArrayList<Integer>();
        fi.advance().join();
        while (fi.hasNext()) {
            actualItems.add(fi.getCurrent().getItem());
            val expectedKey = toKey.apply(fi.getCurrent().getItem());
            Assert.assertEquals(expectedKey, fi.getCurrent().getKey());
            fi.advance().join();
        }

        Assert.assertNull(fi.getCurrent());

        // Compare against expected items (which we get by flattening the provided input ourselves).
        val expectedItems = items.stream().flatMap(List::stream).collect(Collectors.toList());
        AssertExtensions.assertListEquals("", expectedItems, actualItems, Integer::equals);
    }

    private <T> AsyncIterator<IteratorItem<T>> createAsyncIterator(List<List<T>> batchItems) {
        val itemsIterator = batchItems.iterator();
        return () -> {
            IteratorItem<T> result = itemsIterator.hasNext() ? new IteratorItem<>(itemsIterator.next()) : null;
            return CompletableFuture.completedFuture(result);
        };
    }

    /**
     * Tests the {@link KeyValueTableIteratorImpl.TableKeyComparator} class.
     */
    @Test
    public void testTableKeyComparator() {
        val c = new KeyValueTableIteratorImpl.TableKeyComparator();
        val buffers = IntStream.range(0, 256).mapToObj(i -> ByteBuffer.wrap(new byte[]{(byte) i})).collect(Collectors.toList());
        for (int i = 0; i < buffers.size(); i++) {
            for (int j = 0; j < buffers.size(); j++) {
                int expected = (int) Math.signum(j - i);
                int actual = (int) Math.signum(c.compare(buffers.get(j), buffers.get(i)));
                Assert.assertEquals("Unexpected comparison for " + i + " vs " + j, expected, actual);
            }
        }

        // Test individual keys.
        val key1 = new TableKey(buffers.get(0), buffers.get(1));
        val key2 = new TableKey(buffers.get(0), buffers.get(2));
        val key3 = new TableKey(buffers.get(1), buffers.get(1));
        Assert.assertEquals(0, c.compare(key1, key1));
        AssertExtensions.assertLessThan("", 0, c.compare(key1, key2));
        AssertExtensions.assertLessThan("", 0, c.compare(key1, key3));
        AssertExtensions.assertLessThan("", 0, c.compare(key2, key3));
        AssertExtensions.assertGreaterThan("", 0, c.compare(key2, key1));
        AssertExtensions.assertGreaterThan("", 0, c.compare(key3, key1));
        AssertExtensions.assertGreaterThan("", 0, c.compare(key3, key2));

        // Keys with no secondary keys.
        val key4 = new TableKey(buffers.get(0));
        val key5 = new TableKey(buffers.get(1));
        Assert.assertEquals(0, c.compare(key4, key4));
        AssertExtensions.assertLessThan("", 0, c.compare(key4, key5));
        AssertExtensions.assertGreaterThan("", 0, c.compare(key5, key4));
    }

    @Test
    public void testBuilderInvalidArguments() {
        val shortPk = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength() - 1);
        val longPk = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength() + 1);
        val goodPk = newBuffer(DEFAULT_CONFIG.getPrimaryKeyLength());
        val shortSk = newBuffer(DEFAULT_CONFIG.getSecondaryKeyLength() - 1);
        val longSk = newBuffer(DEFAULT_CONFIG.getSecondaryKeyLength() + 1);

        // forPrimaryKey(PK, SK1, SK2).
        val b = builder();
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too short Primary Key)",
                () -> b.forPrimaryKey(shortPk, null, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too long Primary Key)",
                () -> b.forPrimaryKey(longPk, null, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too long From Secondary Key)",
                () -> b.forPrimaryKey(goodPk, longSk, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too short From Secondary Key)",
                () -> b.forPrimaryKey(goodPk, shortSk, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too long To Secondary Key)",
                () -> b.forPrimaryKey(goodPk, null, shortSk),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too short To Secondary Key)",
                () -> b.forPrimaryKey(goodPk, null, shortSk),
                ex -> ex instanceof IllegalArgumentException);

        // forPrimaryKey(PK, SK-Prefix)
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too short Primary Key)",
                () -> b.forPrimaryKey(shortPk, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too short Primary Key)",
                () -> b.forPrimaryKey(shortPk, null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forPrimaryKey(Too long Secondary Key Prefix)",
                () -> b.forPrimaryKey(goodPk, longSk),
                ex -> ex instanceof IllegalArgumentException);

        // forRange.
        AssertExtensions.assertThrows(
                "forRange(Too short From Key)",
                () -> b.forRange(shortPk, goodPk),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "forRange(Too Long To Key)",
                () -> b.forRange(goodPk, longPk),
                ex -> ex instanceof IllegalArgumentException);

        // forPrefix.
        AssertExtensions.assertThrows(
                "forPrefix(Too long)",
                () -> b.forPrefix(longPk),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the {@link KeyValueTableIterator.Builder#forPrimaryKey(ByteBuffer, ByteBuffer, ByteBuffer)} and the
     * {@link KeyValueTableIterator.Builder#forPrimaryKey(ByteBuffer)} methods.
     */
    @Test
    public void testBuilderForPrimaryKeyRange() {
        testBuilderForPrimaryKeyRange(DEFAULT_CONFIG);
        testBuilderForPrimaryKeyRange(NO_SK_CONFIG);
    }

    private void testBuilderForPrimaryKeyRange(KeyValueTableConfiguration config) {
        val b = builder(config);
        val pk = newBuffer(config.getPrimaryKeyLength());

        // PK only (all secondary keys).
        val i1 = b.forPrimaryKey(pk);
        Assert.assertEquals(pk, i1.getFromPrimaryKey());
        Assert.assertEquals(pk, i1.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToSecondaryKey());
        Assert.assertTrue(i1.isSingleSegment());

        // PK + SK range.
        val exactSk1 = newBuffer(config.getSecondaryKeyLength());
        val exactSk2 = newBuffer(config.getSecondaryKeyLength());
        val i2 = b.forPrimaryKey(pk, exactSk1, exactSk2);
        Assert.assertEquals(pk, i2.getFromPrimaryKey());
        Assert.assertEquals(pk, i2.getToPrimaryKey());
        Assert.assertEquals(exactSk1, i2.getFromSecondaryKey());
        Assert.assertEquals(exactSk2, i2.getToSecondaryKey());
        Assert.assertTrue(i2.isSingleSegment());
    }

    /**
     * Tests {@link KeyValueTableIterator.Builder#forPrimaryKey(ByteBuffer, ByteBuffer)}.
     */
    @Test
    public void testBuilderForPrimaryKeyPrefix() {
        testBuilderForPrimaryKeyPrefix(DEFAULT_CONFIG);
        testBuilderForPrimaryKeyPrefix(NO_SK_CONFIG);
    }

    private void testBuilderForPrimaryKeyPrefix(KeyValueTableConfiguration config) {
        val b = builder(config);
        val pk = newBuffer(config.getPrimaryKeyLength());

        // Null prefix (all secondary keys).
        val i1 = b.forPrimaryKey(pk, null);
        Assert.assertEquals(pk, i1.getFromPrimaryKey());
        Assert.assertEquals(pk, i1.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToSecondaryKey());
        Assert.assertTrue(i1.isSingleSegment());

        // Empty prefix.
        val i2 = b.forPrimaryKey(pk, newBuffer(0));
        Assert.assertEquals(pk, i2.getFromPrimaryKey());
        Assert.assertEquals(pk, i2.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i2.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i2.getToSecondaryKey());
        Assert.assertTrue(i2.isSingleSegment());

        // Short prefix.
        if (config.getSecondaryKeyLength() > 0) {
            val shortSk = newBuffer(config.getSecondaryKeyLength() - 1);
            val i3 = b.forPrimaryKey(pk, shortSk);
            Assert.assertEquals(pk, i3.getFromPrimaryKey());
            Assert.assertEquals(pk, i3.getToPrimaryKey());
            checkKey(shortSk, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i3.getFromSecondaryKey());
            checkKey(shortSk, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i3.getToSecondaryKey());
            Assert.assertTrue(i3.isSingleSegment());
        }

        // Prefix is a single key.
        val exactSk = newBuffer(config.getSecondaryKeyLength());
        val i4 = b.forPrimaryKey(pk, exactSk);
        Assert.assertEquals(pk, i4.getFromPrimaryKey());
        Assert.assertEquals(pk, i4.getToPrimaryKey());
        Assert.assertEquals(exactSk, i4.getFromSecondaryKey());
        Assert.assertEquals(exactSk, i4.getToSecondaryKey());
        Assert.assertTrue(i4.isSingleSegment());
    }

    /**
     * Tests the {@link KeyValueTableIterator.Builder#forRange(ByteBuffer, ByteBuffer)} and the
     * {@link KeyValueTableIterator.Builder#all()} methods.
     */
    @Test
    public void testBuilderForRange() {
        testBuilderForRange(DEFAULT_CONFIG);
        testBuilderForRange(NO_SK_CONFIG);
    }

    private void testBuilderForRange(KeyValueTableConfiguration config) {
        val b = builder(config);
        val pk1 = newBuffer(config.getPrimaryKeyLength());
        val pk2 = newBuffer(config.getPrimaryKeyLength());

        // All keys in the table.
        val i1 = b.all();
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromPrimaryKey());
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToSecondaryKey());
        Assert.assertFalse(i1.isSingleSegment());

        // Specific PK range.
        val i2 = b.forRange(pk1, pk2);
        Assert.assertEquals(pk1, i2.getFromPrimaryKey());
        Assert.assertEquals(pk2, i2.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i2.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i2.getToSecondaryKey());
        Assert.assertFalse(i2.isSingleSegment());
    }

    /**
     * Tests the {@link KeyValueTableIterator.Builder#forPrefix(ByteBuffer)} method.
     */
    @Test
    public void testBuilderForPrefix() {
        testBuilderForPrefix(DEFAULT_CONFIG);
        testBuilderForPrefix(NO_SK_CONFIG);
    }

    private void testBuilderForPrefix(KeyValueTableConfiguration config) {
        val b = builder(config);

        // Null prefix.
        val i1 = b.forPrefix(null);
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromPrimaryKey());
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i1.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i1.getToSecondaryKey());
        Assert.assertFalse(i1.isSingleSegment());

        // Empty prefix.
        val i2 = b.forPrefix(newBuffer(0));
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i2.getFromPrimaryKey());
        checkKey(null, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i2.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i2.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i2.getToSecondaryKey());
        Assert.assertFalse(i2.isSingleSegment());

        // "Normal" prefix.
        val shortPk = newBuffer(config.getPrimaryKeyLength() - 2);
        val i3 = b.forPrefix(shortPk);
        checkKey(shortPk, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i3.getFromPrimaryKey());
        checkKey(shortPk, config.getPrimaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i3.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i3.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i3.getToSecondaryKey());
        Assert.assertFalse(i3.isSingleSegment());

        // Exact key.
        val exactPk = newBuffer(config.getPrimaryKeyLength());
        val i4 = b.forPrefix(exactPk);
        Assert.assertEquals(exactPk, i4.getFromPrimaryKey());
        Assert.assertEquals(exactPk, i4.getToPrimaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MIN_BYTE, i4.getFromSecondaryKey());
        checkKey(null, config.getSecondaryKeyLength(), KeyValueTableIteratorImpl.Builder.MAX_BYTE, i4.getToSecondaryKey());
        Assert.assertTrue(i4.isSingleSegment());
    }

    private void checkKey(ByteBuffer prefix, int length, byte padValue, ByteBuffer actual) {
        Assert.assertEquals(length, actual.remaining());
        int checkPos = 0;
        if (prefix != null) {
            checkPos = prefix.remaining();
            Assert.assertEquals(prefix, ByteBufferUtils.slice(actual, 0, prefix.remaining()));
        }

        while (checkPos < length) {
            val actualValue = actual.get(checkPos);
            Assert.assertEquals(padValue, actualValue);
            checkPos++;
        }
    }

    private ByteBuffer newBuffer(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return ByteBuffer.wrap(data);
    }

    private KeyValueTableIterator.Builder builder() {
        return builder(DEFAULT_CONFIG);
    }

    private KeyValueTableIteratorImpl.Builder builder(KeyValueTableConfiguration config) {
        return (KeyValueTableIteratorImpl.Builder) new KeyValueTableIteratorImpl.Builder(config, DEFAULT_ENTRY_HELPER, executorService())
                .maxIterationSize(10);
    }
}
