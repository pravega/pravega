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
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;

/**
 * Unit tests for the {@link AsyncTableKeyBuilder} class.
 */
public class AsyncTableKeyBuilderTests extends ThreadPooledTestSuite {
    private static final EntrySerializer SERIALIZER = new EntrySerializer();
    private static final byte[] UPDATE_KEY = "ThisIsTheUpdateKey".getBytes();
    private static final byte[] REMOVE_KEY = "ThisIsTheRemoveKey".getBytes();
    private static final List<Map.Entry<byte[], byte[]>> TEST_DATA =
            Arrays.asList(new AbstractMap.SimpleImmutableEntry<>(UPDATE_KEY, generateData(UPDATE_KEY, false)),
                    new AbstractMap.SimpleImmutableEntry<>(REMOVE_KEY, generateData(REMOVE_KEY, true)));

    private static final long BASE_TIMEOUT_MILLIS = 10 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(BASE_TIMEOUT_MILLIS * 3);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability to load a key.
     */
    @Test
    public void testSimpleRead() throws Exception {
        for (val e : TEST_DATA) {
            // Start a new builder & processor for this key-serialization pair.
            val keyBuilder = new AsyncTableKeyBuilder(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ByteArrayReadResult(e.getValue(), e.getValue().length, 1);
            AsyncReadResultProcessor.process(rr, keyBuilder, executorService());

            // Get the result and compare it with the original key.
            val result = keyBuilder.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            AssertExtensions.assertArrayEquals("Unexpected key read back.", e.getKey(), 0, result.array(), result.arrayOffset(), e.getKey().length);
        }
    }

    /**
     * Tests the ability to read an empty key (this should result in an exception).
     */
    @Test
    public void testEmptyKey() {
        val data = generateData(new byte[0], false);

        // Start a new builder & processor for this key-serialization pair.
        val keyBuilder = new AsyncTableKeyBuilder(SERIALIZER, new TimeoutTimer(TIMEOUT));
        @Cleanup
        val rr = new ByteArrayReadResult(data, data.length, 1);
        AsyncReadResultProcessor.process(rr, keyBuilder, executorService());

        AssertExtensions.assertThrows(
                "Unexpected behavior for empty key.",
                () -> keyBuilder.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof SerializationException);
    }

    /**
     * Tests the ability to handle a case where the key could not be read before the read result was done.
     */
    @Test
    public void testReadResultTooShort() {
        for (val e : TEST_DATA) {
            // Start a new builder & processor for this key-serialization pair.
            val keyBuilder = new AsyncTableKeyBuilder(SERIALIZER, new TimeoutTimer(TIMEOUT));
            @Cleanup
            val rr = new ByteArrayReadResult(e.getValue(), e.getKey().length - 1, 1);
            AsyncReadResultProcessor.process(rr, keyBuilder, executorService());

            AssertExtensions.assertThrows(
                    "Unexpected behavior for shorter read result..",
                    () -> keyBuilder.getResult().get(BASE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                    ex -> ex instanceof SerializationException);
        }
    }

    private static byte[] generateData(byte[] keyData, boolean removal) {
        byte[] result;
        if (removal) {
            val key = TableKey.unversioned(new ByteArraySegment(keyData));
            result = new byte[SERIALIZER.getRemovalLength(key)];
            SERIALIZER.serializeRemoval(Collections.singletonList(key), result);
        } else {
            val entry = TableEntry.unversioned(new ByteArraySegment(keyData), new ByteArraySegment(keyData));
            result = new byte[SERIALIZER.getUpdateLength(entry)];
            SERIALIZER.serializeUpdate(Collections.singletonList(entry), result);
        }

        return result;
    }

    //region ByteArrayReadResult

    @RequiredArgsConstructor
    @Getter
    private static class ByteArrayReadResult implements ReadResult {
        private final byte[] data;
        private final int maxResultLength;
        private final int entryLength;
        private int consumedLength;
        private boolean closed;

        @Override
        public long getStreamSegmentStartOffset() {
            return 0;
        }

        @Override
        public void close() {
            this.closed = true;
        }

        @Override
        public boolean hasNext() {
            return this.consumedLength < this.maxResultLength;
        }

        @Override
        public ReadResultEntry next() {
            if (!hasNext()) {
                return null;
            }

            int offset = this.consumedLength;
            int length = Math.min(this.entryLength, this.maxResultLength - offset);
            this.consumedLength += length;
            return new Entry(offset, length);
        }

        @RequiredArgsConstructor
        @Getter
        private class Entry implements ReadResultEntry {
            private final long streamSegmentOffset;
            private final int requestedReadLength;
            private final CompletableFuture<ReadResultEntryContents> content = new CompletableFuture<>();

            @Override
            public ReadResultEntryType getType() {
                return ReadResultEntryType.Cache;
            }

            @Override
            public void requestContent(Duration timeout) {
                this.content.complete(new ReadResultEntryContents(
                        new ByteArrayInputStream(data, (int) this.streamSegmentOffset, this.requestedReadLength),
                        this.requestedReadLength));
            }
        }
    }

    //endregion
}
