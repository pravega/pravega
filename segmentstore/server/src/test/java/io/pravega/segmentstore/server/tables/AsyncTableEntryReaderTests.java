/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link AsyncTableEntryReader} class.
 */
public class AsyncTableEntryReaderTests extends ThreadPooledTestSuite {
    private static final EntrySerializer SERIALIZER = new EntrySerializer();
    private static final int COUNT = 100;

    private static final long BASE_TIMEOUT_MILLIS = 10 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(BASE_TIMEOUT_MILLIS * 3);

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
            val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.serialization, e.serialization.length, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());

            // Get the result and compare it with the original key.
            val result = keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            AssertExtensions.assertArrayEquals("Unexpected key read back.", e.key, 0, result.array(), result.arrayOffset(), e.key.length);
        }
    }

    /**
     * Tests the ability to read an empty key (this should result in an exception).
     */
    @Test
    public void testReadEmptyKey() {
        val testItem = generateTestItem(new byte[0], new byte[0], false);

        // Start a new reader & processor for this key-serialization pair.
        val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
        @Cleanup
        val rr = new ReadResultMock(testItem.serialization, testItem.serialization.length, 1);
        AsyncReadResultProcessor.process(rr, keyReader, executorService());

        AssertExtensions.assertThrows(
                "Unexpected behavior for empty key.",
                () -> keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof SerializationException);
    }

    /**
     * Tests the ability to handle a case where the key could not be read before the read result was done.
     */
    @Test
    public void testReadKeyResultTooShort() {
        val testItems = generateTestItems();
        for (val e : testItems) {
            // Start a new reader & processor for this key-serialization pair.
            val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.serialization, e.key.length - 1, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());

            AssertExtensions.assertThrows(
                    "Unexpected behavior for shorter read result..",
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
            @Cleanup
            val rr = new ReadResultMock(item.serialization, item.serialization.length, 1);
            AsyncReadResultProcessor.process(rr, entryReader, executorService());

            val result = entryReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Expecting a result.", result);

            // Check key.
            val resultKey = result.getKey().getKey();
            Assert.assertEquals("Unexpected result key length.", item.key.length, resultKey.getLength());
            AssertExtensions.assertArrayEquals("Unexpected result key.", item.key, 0,
                    resultKey.array(), resultKey.arrayOffset(), item.key.length);

            if (item.isRemoval) {
                // Verify there is no value and that the key has been properly set.
                Assert.assertEquals("Unexpected key version for non existing key.", TableKey.NOT_EXISTS, result.getKey().getVersion());
                Assert.assertNull("Not expecting a value for a removal.", result.getValue());
            } else {
                // Verify we have a value and that it matches.
                Assert.assertEquals("Unexpected key version for existing key.", keyVersion, result.getKey().getVersion());
                Assert.assertNotNull("Expecting a value for non removal.", result.getValue());
                val resultValue = result.getValue();
                Assert.assertEquals("Unexpected value length.", item.value.length, resultValue.getLength());
                AssertExtensions.assertArrayEquals("Unexpected result value", item.value, 0,
                        resultValue.array(), resultValue.arrayOffset(), item.value.length);
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

    //endregion

    private static ArrayList<TestItem> generateTestItems() {
        val rnd = new Random(0);
        val result = new ArrayList<TestItem>();
        for (int i = 0; i < COUNT; i++) {
            byte[] key = new byte[Math.max(1, rnd.nextInt(100))];
            byte[] value = new byte[rnd.nextInt(10)];
            rnd.nextBytes(key);
            rnd.nextBytes(value);
            result.add(generateTestItem(key, value, i % 2 == 0));
        }

        return result;
    }

    private static TestItem generateTestItem(byte[] key, byte[] value, boolean removal) {
        byte[] serialization;
        if (removal) {
            val keyData = TableKey.unversioned(new ByteArraySegment(key));
            serialization = new byte[SERIALIZER.getRemovalLength(keyData)];
            SERIALIZER.serializeRemoval(Collections.singletonList(keyData), serialization);
        } else {
            val entry = TableEntry.unversioned(new ByteArraySegment(key), new ByteArraySegment(value));
            serialization = new byte[SERIALIZER.getUpdateLength(entry)];
            SERIALIZER.serializeUpdate(Collections.singletonList(entry), serialization);
        }
        return new TestItem(key, value, removal, serialization);
    }

    @RequiredArgsConstructor
    private static class TestItem {
        final byte[] key;
        final byte[] value;
        final boolean isRemoval;
        final byte[] serialization;
    }
}
