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
import java.util.HashSet;
import java.util.Random;
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
    private static final int TOTAL_KEY_LENGTH = DEFAULT_CONFIG.getPrimaryKeyLength() + DEFAULT_CONFIG.getSecondaryKeyLength();
    private static final KeyValueTableConfiguration NO_SK_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(DEFAULT_CONFIG.getPartitionCount())
            .primaryKeyLength(TOTAL_KEY_LENGTH)
            .secondaryKeyLength(0)
            .build();
    private static final TableEntryHelper DEFAULT_ENTRY_HELPER = new TableEntryHelper(mock(SegmentSelector.class), DEFAULT_CONFIG);
    private final Random random = new Random(0);

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
        val b = new KeyValueTableIteratorImpl.Builder(DEFAULT_CONFIG, entryHelper).maxIterationSize(maxIterationSize);

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
        return (KeyValueTableIteratorImpl.Builder) new KeyValueTableIteratorImpl.Builder(config, DEFAULT_ENTRY_HELPER).maxIterationSize(10);
    }
}
