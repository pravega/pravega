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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link AsyncTableEntryReader} class.
 */
public class AsyncTableEntryReaderTests extends ThreadPooledTestSuite {
    private static final EntrySerializer SERIALIZER = new EntrySerializer();
    private static final byte[] UPDATE_KEY = "ThisIsTheUpdateKey".getBytes();
    private static final byte[] UPDATE_KEY_EMPTY = "ThisIsTheUpdateKeyWithEmptyValue".getBytes();
    private static final byte[] REMOVE_KEY = "ThisIsTheRemoveKey".getBytes();
    private static final List<Map.Entry<byte[], byte[]>> TEST_DATA =
            Arrays.asList(
                    new AbstractMap.SimpleImmutableEntry<>(UPDATE_KEY, generateData(UPDATE_KEY, false)),
                    new AbstractMap.SimpleImmutableEntry<>(UPDATE_KEY_EMPTY, generateUpdateData(UPDATE_KEY_EMPTY, 0)),
                    new AbstractMap.SimpleImmutableEntry<>(REMOVE_KEY, generateData(REMOVE_KEY, true)));

    private static final long BASE_TIMEOUT_MILLIS = 10 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(BASE_TIMEOUT_MILLIS * 3);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    //region Reading Keys

    /**
     * Tests the ability to load a key.
     */
    @Test
    public void testRead() throws Exception {
        for (val e : TEST_DATA) {
            // Start a new reader & processor for this key-serialization pair.
            val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.getValue(), e.getValue().length, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());

            // Get the result and compare it with the original key.
            val result = keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            AssertExtensions.assertArrayEquals("Unexpected key read back.", e.getKey(), 0, result.array(), result.arrayOffset(), e.getKey().length);
        }
    }

    /**
     * Tests the ability to read an empty key (this should result in an exception).
     */
    @Test
    public void testReadEmptyKey() {
        val data = generateData(new byte[0], false);

        // Start a new reader & processor for this key-serialization pair.
        val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
        @Cleanup
        val rr = new ReadResultMock(data, data.length, 1);
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
    public void testReadResultTooShort() {
        for (val e : TEST_DATA) {
            // Start a new reader & processor for this key-serialization pair.
            val keyReader = AsyncTableEntryReader.readKey(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ReadResultMock(e.getValue(), e.getKey().length - 1, 1);
            AsyncReadResultProcessor.process(rr, keyReader, executorService());

            AssertExtensions.assertThrows(
                    "Unexpected behavior for shorter read result..",
                    () -> keyReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    ex -> ex instanceof SerializationException);
        }
    }

    //endregion

    //region Matching Keys

    /**
     * Tests the ability to match an existing key.
     */
    @Test
    public void testMatchAndReadValue() throws Exception {
        for (val e : TEST_DATA) {
            // Start a new matcher & processor for this key-serialization pair.
            val keyToMatch = new ByteArraySegment(e.getKey());
            val keyMatcher = AsyncTableEntryReader.matchKey(keyToMatch, SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val keyReadResult = new ReadResultMock(e.getValue(), e.getValue().length, 1);
            AsyncReadResultProcessor.process(keyReadResult, keyMatcher, executorService());

            // Get the result and verify.
            val header = keyMatcher.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("Expecting a valid result.", header);
            Assert.assertEquals("Unexpected value from getTotalLength().", e.getValue().length, header.getTotalLength());
            Assert.assertEquals("Unexpected value from getKeyLength().", keyToMatch.getLength(), header.getKeyLength());

            // Now read the value.
            if (header.isDeletion()) {
                AssertExtensions.assertThrows(
                        "readValue() accepted a negative (deletion) length.",
                        () -> AsyncTableEntryReader.readValue(header.getValueLength(), new TimeoutTimer(TIMEOUT)),
                        ex -> ex instanceof IllegalArgumentException);
                continue;
            }

            val valueReader = AsyncTableEntryReader.readValue(header.getValueLength(), new TimeoutTimer(TIMEOUT));
            val readData = header.getValueLength() == 0
                    ? new ByteArraySegment(new byte[0])
                    : new ByteArraySegment(e.getValue(), header.getValueOffset(), e.getValue().length - header.getValueOffset());
            @Cleanup
            val valueReadResult = new ReadResultMock(readData, header.getValueLength(), 1);
            AsyncReadResultProcessor.process(valueReadResult, valueReader, executorService());

            // Fetch the value.
            val value = valueReader.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertNotNull("No value could be read.", value);
            Assert.assertEquals("Unexpected value length.", header.getValueLength(), value.getLength()); // Values are the same as our keys.
            AssertExtensions.assertArrayEquals("Unexpected value.", e.getKey(), 0, value.array(), value.arrayOffset(), header.getValueLength());
        }
    }

    /**
     * Tests the ability to detect that a key does not match.
     */
    @Test
    public void testNoMatch() throws Exception {
        for (val e : TEST_DATA) {
            val keysToMatch = new ArrayList<ByteArraySegment>();
            keysToMatch.add(new ByteArraySegment(new byte[0]));
            keysToMatch.add(new ByteArraySegment(e.getKey(), 0, e.getKey().length - 1));
            keysToMatch.add(new ByteArraySegment(e.getKey(), 1, e.getKey().length - 1));
            byte[] extraByteKey = new byte[e.getKey().length + 1];
            System.arraycopy(e.getKey(), 0, extraByteKey, 0, e.getKey().length);
            keysToMatch.add(new ByteArraySegment(extraByteKey));

            for (val keyToMatch : keysToMatch) {
                // Start a new matcher & processor for this key-serialization pair.
                val keyMatcher = AsyncTableEntryReader.matchKey(keyToMatch, SERIALIZER, new TimeoutTimer(TIMEOUT));
                @Cleanup
                val rr = new ReadResultMock(e.getValue(), e.getValue().length, 1);
                AsyncReadResultProcessor.process(rr, keyMatcher, executorService());

                // Get the result and verify.
                val result = keyMatcher.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                Assert.assertNull("Not expecting a valid result.", result);
            }
        }
    }

    //endregion

    private static byte[] generateUpdateData(byte[] keyData, int valueLength) {
        val entry = TableEntry.unversioned(new ByteArraySegment(keyData), new ByteArraySegment(keyData, 0, valueLength));
        val result = new byte[SERIALIZER.getUpdateLength(entry)];
        SERIALIZER.serializeUpdate(Collections.singletonList(entry), result);
        return result;
    }

    private static byte[] generateData(byte[] keyData, boolean removal) {
        if (removal) {
            val key = TableKey.unversioned(new ByteArraySegment(keyData));
            val result = new byte[SERIALIZER.getRemovalLength(key)];
            SERIALIZER.serializeRemoval(Collections.singletonList(key), result);
            return result;
        } else {
            return generateUpdateData(keyData, keyData.length);
        }
    }
}
