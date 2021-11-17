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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.ReadResultMock;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link AsyncTableEntryReader} class.
 */
public class AsyncTableEntryReaderTests extends ThreadPooledTestSuite {
    private static final EntrySerializer SERIALIZER = new EntrySerializer();
    private static final int COUNT = 100;
    private static final long BASE_TIMEOUT_MILLIS = 10 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(BASE_TIMEOUT_MILLIS * 3);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    //region Reading Keys

    /**
     * Tests the ability to read a key.
     */
    @Test
    public void testReadKey() throws Exception {
        val testItems = generateTestItems();
        for (val e : testItems) {
            val keyReader = AsyncTableEntryReader.readKey(1L, SERIALIZER, new TimeoutTimer(TIMEOUT));
            Assert.assertEquals("Unexpected initial suggested read length.", AsyncTableEntryReader.INITIAL_READ_LENGTH, keyReader.getMaxReadAtOnce());
            @Cleanup
            val rr = new ReadResultMock(e.serialization, e.serialization.length, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());
            AssertExtensions.assertEventuallyEquals(true, () -> {
                int readerMaxReadAtOnce = keyReader.getMaxReadAtOnce();
                return Math.min(rr.getMaxResultLength(), readerMaxReadAtOnce != 0 ? readerMaxReadAtOnce : Integer.MAX_VALUE) == rr.getMaxReadAtOnce();
            }, 30 * 1000);

            // Get the result and compare it with the original key.
            val result = keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            val expectedVersion = e.isRemoval
                    ? TableKey.NOT_EXISTS
                    : (e.explicitVersion == TableKey.NO_VERSION) ? 1L : e.explicitVersion;
            Assert.assertEquals("Unexpected version.", expectedVersion, result.getVersion());
            Assert.assertEquals("Unexpected key read back.", new ByteArraySegment(e.key), result.getKey());
            Assert.assertEquals("Unexpected final suggested read length.", 0, keyReader.getMaxReadAtOnce());
        }
    }

    /**
     * Tests the ability to read an empty key (this should result in an exception).
     */
    @Test
    public void testReadEmptyKey() {
        val testItem = generateTestItem(new byte[0], new byte[0], false, false);

        // Start a new reader & processor for this key-serialization pair.
        val keyReader = AsyncTableEntryReader.readKey(1L, SERIALIZER, new TimeoutTimer(TIMEOUT));
        @Cleanup
        val rr = new ReadResultMock(testItem.serialization, testItem.serialization.length, 1);
        AsyncReadResultProcessor.process(rr, keyReader, executorService());

        AssertExtensions.assertThrows(
                "Unexpected behavior for empty key.",
                () -> keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof SerializationException);

        // When the result is done, whether with error or not, this should be set to 0.
        Assert.assertEquals("Unexpected final suggested read length.", 0, keyReader.getMaxReadAtOnce());
    }

    /**
     * Tests the ability to handle a case where the key could not be read before the read result was done.
     */
    @Test
    public void testReadKeyResultTooShort() {
        val testItems = generateTestItems();
        for (val e : testItems) {
            // Start a new reader & processor for this key-serialization pair.
            val keyReader = AsyncTableEntryReader.readKey(1L, SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.serialization, e.key.length - 1, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());

            AssertExtensions.assertThrows(
                    "Unexpected behavior for shorter read result.",
                    () -> keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    ex -> ex instanceof SerializationException);
        }
    }

    //endregion

    //region Reading Entries

    /**
     * Tests the ability to read a Table Entry for a matching key.
     */
    @Test
    public void testReadEntry() throws Exception {
        long keyVersion = 1L;
        val testItems = generateTestItems();
        for (val item : testItems) {
            // Start a new reader & processor for this key-serialization pair.
            val entryReader = AsyncTableEntryReader.readEntry(new ByteArraySegment(item.key), keyVersion, SERIALIZER, new TimeoutTimer(TIMEOUT));
            Assert.assertEquals("Unexpected initial suggested read length.", AsyncTableEntryReader.INITIAL_READ_LENGTH, entryReader.getMaxReadAtOnce());
            @Cleanup
            val rr = new ReadResultMock(item.serialization, item.serialization.length, 1);
            AsyncReadResultProcessor.process(rr, entryReader, executorService());
            Assert.assertEquals(Math.min(rr.getMaxResultLength(), entryReader.getMaxReadAtOnce()), rr.getMaxReadAtOnce());

            val result = entryReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Expecting a result.", result);
            Assert.assertEquals("Unexpected suggested read length after reading whole entry.", 0, entryReader.getMaxReadAtOnce());

            // Check key.
            val resultKey = result.getKey().getKey();
            Assert.assertEquals("Unexpected result key length.", item.key.length, resultKey.getLength());
            Assert.assertEquals("Unexpected result key.", new ByteArraySegment(item.key), resultKey);

            if (item.isRemoval) {
                // Verify there is no value and that the key has been properly set.
                Assert.assertEquals("Unexpected key version for non existing key.", TableKey.NOT_EXISTS, result.getKey().getVersion());
                Assert.assertNull("Not expecting a value for a removal.", result.getValue());
            } else {
                // Verify we have a value and that it matches.
                if (item.explicitVersion == TableKey.NO_VERSION) {
                    Assert.assertEquals("Unexpected key version for existing key.", keyVersion, result.getKey().getVersion());
                } else {
                    Assert.assertEquals("Unexpected (explicit) key version for existing key.", item.explicitVersion, result.getKey().getVersion());
                }
                Assert.assertNotNull("Expecting a value for non removal.", result.getValue());
                val resultValue = result.getValue();
                Assert.assertEquals("Unexpected value length.", item.value.length, resultValue.getLength());
                Assert.assertEquals("Unexpected result value", new ByteArraySegment(item.value), resultValue);
            }

            keyVersion++;
        }
    }

    /**
     * Tests the ability to not read a Table Entry if the sought key does not match.
     */
    @Test
    public void testReadEntryNoKeyMatch() throws Exception {
        val testItems = generateTestItems();
        for (int i = 0; i < testItems.size(); i++) {
            for (int j = 0; j < testItems.size(); j++) {
                if (i == j) {
                    // This case is tested in testReadEntry().
                    continue;
                }

                val searchKey = testItems.get(i).key;
                val searchData = testItems.get(j).serialization;

                val entryReader = AsyncTableEntryReader.readEntry(new ByteArraySegment(searchKey), 0L, SERIALIZER, new TimeoutTimer(TIMEOUT));
                @Cleanup
                val rr = new ReadResultMock(searchData, searchData.length, 1);
                AsyncReadResultProcessor.process(rr, entryReader, executorService());

                val result = entryReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                Assert.assertNull("Not expecting a result.", result);
            }
        }
    }

    /**
     * Tests the ability to handle a case where the key could not be read before the read result was done.
     */
    @Test
    public void testReadEntryResultTooShort() {
        val testItems = generateTestItems();
        for (val e : testItems) {
            // Start a new reader & processor for this key-serialization pair.
            val entryReader = AsyncTableEntryReader.readEntry(new ByteArraySegment(e.key), 0L, SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.serialization, e.serialization.length - 1, 1);
            AsyncReadResultProcessor.process(rr, entryReader, executorService());

            AssertExtensions.assertThrows(
                    "Unexpected behavior for shorter read result..",
                    () -> entryReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    ex -> ex instanceof SerializationException);
        }
    }

    /**
     * Tests the {@link AsyncTableEntryReader#readEntryComponents} method.
     */
    @Test
    public void testReadEntryComponents() throws Exception {
        val testItems = generateTestItems();
        val input = BufferView.wrap(testItems.stream().map(i -> new ByteArraySegment(i.serialization)).collect(Collectors.toList())).getBufferViewReader();
        long offset = 0;
        for (int i = 0; i < testItems.size(); i++) {
            val expected = testItems.get(i);
            val actual = AsyncTableEntryReader.readEntryComponents(input, offset, SERIALIZER);

            // Check Key.
            Assert.assertEquals("Unexpected key parsed at index " + i, new ByteArraySegment(expected.key), actual.getKey());

            Assert.assertEquals("Unexpected Header.isDeletion() at index " + i, expected.isRemoval, actual.getHeader().isDeletion());
            if (expected.isRemoval) {
                Assert.assertNull("Not expecting a value for a deletion at index " + i, actual.getValue());
                Assert.assertEquals("Unexpected Header.getEntryVersion() for removal at index " + i,
                        TableKey.NO_VERSION, actual.getHeader().getEntryVersion());
            } else {
                Assert.assertNotNull("Expecting a value for a non-deletion at index " + i, actual.getValue());
                Assert.assertEquals("Unexpected value parsed at index " + i, new ByteArraySegment(expected.value), actual.getValue());
                long expectedVersion = expected.explicitVersion == TableKey.NO_VERSION ? offset : expected.explicitVersion;
                Assert.assertEquals("Unexpected version at index " + i, expectedVersion, actual.getVersion());
            }

            offset += actual.getHeader().getTotalLength();
        }
    }

    //endregion

    //region Other Tests

    /**
     * Tests the ability of the {@link AsyncTableEntryReader} to auto-compact Key and Value buffers if they occupy less
     * than the predetermined amount of memory.
     */
    @Test
    public void testBufferCompaction() throws Exception {
        testBufferCompaction((k, version, s, timer) -> AsyncTableEntryReader.readKey(version, s, timer), k -> k, k -> null);
        testBufferCompaction(AsyncTableEntryReader::readEntry, TableEntry::getKey, TableEntry::getValue);
    }

    private <T> void testBufferCompaction(GetEntryReader<T> createReader, Function<T, TableKey> getKey, Function<T, BufferView> getValue) throws Exception {
        val keyLength = 3987; // Must be less than AsyncTableEntryReader.INITIAL_READ_LENGTH / 2 (to ease testing).
        val valueLength = 3123; // Must be less than AsyncTableEntryReader.INITIAL_READ_LENGTH / 2 (to ease testing)..
        val serializer = new EntrySerializer();

        // Generate a number of entries. We only care about the first one, but we want to ensure that we have enough other
        // data to force the ReadResult to try to read more.
        val testItems = generateTestItems(() -> keyLength, () -> valueLength);
        val entries = testItems.stream()
                .filter(i -> !i.isRemoval)
                .map(i -> TableEntry.unversioned(new ByteArraySegment(i.key), new ByteArraySegment(i.value)))
                .collect(Collectors.toList());

        // Search for the first Key/Entry. This makes it easier as we don't have to guess the versions, offsets, etc.
        val soughtEntry = entries.get(0);
        val segmentData = serializer.serializeUpdate(entries).getCopy();

        @Cleanup
        val readResultNoCompact = new ReadResultMock(segmentData, keyLength + valueLength + 20, keyLength + 200);
        val readerNoCompact = createReader.apply(soughtEntry.getKey().getKey(), 0L, serializer, new TimeoutTimer(TIMEOUT));
        testBufferCompaction(readerNoCompact, readResultNoCompact, getKey, getValue, false);

        @Cleanup
        val readResultWithCompact = new ReadResultMock(segmentData, segmentData.length, segmentData.length);
        val readerWithCompact = createReader.apply(soughtEntry.getKey().getKey(), 0L, serializer, new TimeoutTimer(TIMEOUT));
        testBufferCompaction(readerWithCompact, readResultWithCompact, getKey, getValue, true);
    }

    private <T> void testBufferCompaction(AsyncTableEntryReader<T> reader, ReadResultMock readResult, Function<T, TableKey> getKey,
                                          Function<T, BufferView> getValue, boolean expectCompaction) throws Exception {
        AsyncReadResultProcessor.process(readResult, reader, executorService());

        // Await for the result to complete, then extract the key and value.
        val readerResult = reader.getResult().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val soughtKey = getKey.apply(readerResult);
        val soughtValue = getValue.apply(readerResult);

        // Verify that MaxReadAtOnce is properly set (We do one for header+key, one for value(if any), and a zero-one at the end).

        // Verify that we are not holding on to more buffer than we need.
        val allocatedKeySize = soughtKey.getKey().getAllocatedLength();
        if (expectCompaction) {
            Assert.assertEquals("", soughtKey.getKey().getLength(), soughtKey.getKey().getAllocatedLength());
        } else {
            AssertExtensions.assertGreaterThanOrEqual("", soughtKey.getKey().getLength(), soughtKey.getKey().getAllocatedLength());
        }

        if (soughtValue != null) {
            val allocatedValueSize = soughtValue.getAllocatedLength();
            if (expectCompaction) {
                Assert.assertEquals("", soughtValue.getLength(), soughtValue.getAllocatedLength());
            } else {
                AssertExtensions.assertGreaterThanOrEqual("", soughtValue.getLength(), soughtValue.getAllocatedLength());
            }
        }
    }

    //endregion

    private static ArrayList<TestItem> generateTestItems() {
        val rnd = new Random(0);
        return generateTestItems(() -> Math.max(1, rnd.nextInt(100)), () -> rnd.nextInt(10));
    }

    private static ArrayList<TestItem> generateTestItems(Supplier<Integer> nextKeyLength, Supplier<Integer> nextValueLength) {
        val rnd = new Random(0);
        val result = new ArrayList<TestItem>();
        for (int i = 0; i < COUNT; i++) {
            byte[] key = new byte[nextKeyLength.get()];
            byte[] value = new byte[nextValueLength.get()];
            rnd.nextBytes(key);
            rnd.nextBytes(value);
            result.add(generateTestItem(key, value, i % 2 == 0, i % 5 == 0));
        }

        return result;
    }

    private static TestItem generateTestItem(byte[] key, byte[] value, boolean removal, boolean explicitVersion) {
        byte[] serialization;
        if (removal) {
            val keyData = TableKey.unversioned(new ByteArraySegment(key));
            serialization = SERIALIZER.serializeRemoval(Collections.singletonList(keyData)).getCopy();
            return new TestItem(key, value, removal, TableKey.NO_VERSION, serialization);
        } else {
            val entry = TableEntry.versioned(new ByteArraySegment(key), new ByteArraySegment(value), key.length);
            if (explicitVersion) {
                serialization = SERIALIZER.serializeUpdateWithExplicitVersion(Collections.singletonList(entry)).getCopy();
                return new TestItem(key, value, removal, entry.getKey().getVersion(), serialization);
            } else {
                serialization = SERIALIZER.serializeUpdate(Collections.singletonList(entry)).getCopy();
                return new TestItem(key, value, removal, TableKey.NO_VERSION, serialization);
            }
        }
    }

    @RequiredArgsConstructor
    private static class TestItem {
        final byte[] key;
        final byte[] value;
        final boolean isRemoval;
        final long explicitVersion;
        final byte[] serialization;
    }

    @FunctionalInterface
    private interface GetEntryReader<T> {
        AsyncTableEntryReader<T> apply(@Nullable BufferView soughtKey, long keyVersion, EntrySerializer serializer, TimeoutTimer timer);
    }
}
