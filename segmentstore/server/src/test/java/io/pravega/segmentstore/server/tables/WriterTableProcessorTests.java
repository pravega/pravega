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

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link WriterTableProcessor} class.
 */
public class WriterTableProcessorTests extends ThreadPooledTestSuite {
    private static final long SEGMENT_ID = 12345L;
    private static final String SEGMENT_NAME = "Segment";
    private static final long INITIAL_LAST_INDEXED_OFFSET = 1000L;
    private static final int MAX_KEY_LENGTH = 8 * 1024;
    private static final int MAX_VALUE_LENGTH = 1024; // We don't care about values that much here.
    private static final int UPDATE_COUNT = 10000;
    private static final double REMOVE_FRACTION = 0.3; // 30% of generated operations are removes.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the {@link WriterTableProcessor#add} method and other general (non-flush) methods.
     */
    @Test
    public void testAdd() throws Exception {
        @Cleanup
        val context = new TestContext();
        Assert.assertFalse("Unexpected value from isClosed.", context.processor.isClosed());
        Assert.assertFalse("Unexpected value from mustFlush.", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN when no data in.", Operation.NO_SEQUENCE_NUMBER, context.processor.getLowestUncommittedSequenceNumber());

        // Mismatched segment ids.
        AssertExtensions.assertThrows(
                "add() worked with wrong segment id.",
                () -> context.processor.add(new StreamSegmentAppendOperation(SEGMENT_ID + 1, new byte[0], null)),
                ex -> ex instanceof IllegalArgumentException);

        // Pre-last indexed offset.
        context.processor.add(generateRandomEntryAppend(0, context));
        Assert.assertFalse("Unexpected value from mustFlush after ignored add().", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN after ignored add().",
                Operation.NO_SEQUENCE_NUMBER, context.processor.getLowestUncommittedSequenceNumber());

        // Non-contiguous appends.
        val validAppend = generateRandomEntryAppend(INITIAL_LAST_INDEXED_OFFSET, context);
        context.processor.add(validAppend);
        Assert.assertTrue("Unexpected value from mustFlush after valid add().", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN after valid add().",
                validAppend.getSequenceNumber(), context.processor.getLowestUncommittedSequenceNumber());
        AssertExtensions.assertThrows(
                "add() allowed non-contiguous appends.",
                () -> context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset() + 1, context)),
                ex -> ex instanceof DataCorruptionException);

        // Delete the segment.
        context.metadata.markDeleted();
        Assert.assertFalse("Unexpected value from mustFlush after deletion.", context.processor.mustFlush());
        context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset(), context));
        Assert.assertEquals("Unexpected LUSN after ignored append due to deletion.",
                validAppend.getSequenceNumber(), context.processor.getLowestUncommittedSequenceNumber());

        // Close the processor and verify.
        context.processor.close();
        Assert.assertTrue("Unexpected value from isClosed after closing.", context.processor.isClosed());
        AssertExtensions.assertThrows(
                "add() worked after closing.",
                () -> context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset(), context)),
                ex -> ex instanceof ObjectClosedException);
    }

    /**
     * Tests the {@link WriterTableProcessor#flush} method, without any induced errors.
     */
    @Test
    public void testFlushNoErrors() throws Exception {
        // Generate a set of operations, each containing one or more entries. Each entry is an update or a remove.
        // Towards the beginning we have more updates than removes, then removes will prevail.
        @Cleanup
        val context = new TestContext();
        val expectedData = generateAndPopulateEntries(context);

        // TODO: finish up the test.
    }

    private Map<HashedArray, ArrayView> generateAndPopulateEntries(TestContext context) throws Exception {
        val result = new HashMap<HashedArray, ArrayView>();
        val allKeys = new ArrayList<HashedArray>(); // Need a list so we can efficiently pick removal candidates.
        for (int i = 0; i < UPDATE_COUNT; i++) {
            // We only generate a remove if we have something to remove.
            boolean remove = allKeys.size() > 0 && (context.random.nextDouble() < REMOVE_FRACTION);
            StreamSegmentAppendOperation append;
            if (remove) {
                val key = allKeys.get(context.random.nextInt(allKeys.size()));
                append = generateRawRemove(TableKey.unversioned(key), context.metadata.getLength(), context);
                result.remove(key);
                allKeys.remove(key);
            } else {
                // Generate a new Table Entry.
                byte[] keyData = new byte[Math.max(1, context.random.nextInt(MAX_KEY_LENGTH))];
                context.random.nextBytes(keyData);
                byte[] valueData = new byte[context.random.nextInt(MAX_VALUE_LENGTH)];
                context.random.nextBytes(valueData);
                val key = new HashedArray(keyData);
                val value = new ByteArraySegment(valueData);
                append = generateRawAppend(TableEntry.unversioned(key, value), context.metadata.getLength(), context);
                result.put(key, value);
                allKeys.add(key);
            }

            // Add to segment.
            context.metadata.setLength(context.metadata.getLength() + append.getLength());
            context.segmentMock.append(append.getData(), null, TIMEOUT).join();

            // Add to processor.
            context.processor.add(append);
        }

        return result;
    }

    private CachedStreamSegmentAppendOperation generateRandomEntryAppend(long offset, TestContext context) {
        return generateAppend(TableEntry.unversioned(new ByteArraySegment(new byte[1]), new ByteArraySegment(new byte[1])), offset, context);
    }

    private CachedStreamSegmentAppendOperation generateAppend(TableEntry entry, long offset, TestContext context) {
        return new CachedStreamSegmentAppendOperation(generateRawAppend(entry, offset, context));
    }

    private StreamSegmentAppendOperation generateRawAppend(TableEntry entry, long offset, TestContext context) {
        byte[] data = new byte[context.serializer.getUpdateLength(entry)];
        context.serializer.serializeUpdate(Collections.singletonList(entry), data);
        val append = new StreamSegmentAppendOperation(SEGMENT_ID, data, null);
        append.setSequenceNumber(context.nextSequenceNumber());
        append.setStreamSegmentOffset(offset);
        return append;
    }

    private StreamSegmentAppendOperation generateRawRemove(TableKey key, long offset, TestContext context) {
        byte[] data = new byte[context.serializer.getRemovalLength(key)];
        context.serializer.serializeRemoval(Collections.singletonList(key), data);
        val append = new StreamSegmentAppendOperation(SEGMENT_ID, data, null);
        append.setSequenceNumber(context.nextSequenceNumber());
        append.setStreamSegmentOffset(offset);
        return append;
    }

    private class TestContext implements AutoCloseable {
        final UpdateableSegmentMetadata metadata;
        final EntrySerializer serializer;
        final KeyHasher keyHasher;
        final SegmentMock segmentMock;
        final WriterTableProcessor processor;
        final Random random;
        final AtomicLong sequenceNumber;

        TestContext() {
            this(KeyHashers.DEFAULT_HASHER);
        }

        TestContext(KeyHasher hasher) {
            this.metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, 0);
            this.serializer = new EntrySerializer();
            this.keyHasher = hasher;
            this.segmentMock = new SegmentMock(executorService());
            this.random = new Random(0);
            this.sequenceNumber = new AtomicLong(0);
            initializeSegment();
            this.processor = new WriterTableProcessor(this.metadata, this.serializer, this.keyHasher, this::getSegment, executorService());
        }

        @Override
        public void close() {
            this.processor.close();
        }

        long nextSequenceNumber() {
            return this.sequenceNumber.incrementAndGet();
        }

        private void initializeSegment() {
            // Populate table-related attributes.
            val w = new IndexWriter(this.keyHasher, executorService());
            this.segmentMock.updateAttributes(w.generateInitialTableAttributes(), TIMEOUT).join();

            // Pre-populate the TABLE_INDEX_OFFSET.
            this.metadata.updateAttributes(Collections.singletonMap(Attributes.TABLE_INDEX_OFFSET, INITIAL_LAST_INDEXED_OFFSET));
            this.metadata.setLength(INITIAL_LAST_INDEXED_OFFSET);
            this.segmentMock.append(new byte[(int) INITIAL_LAST_INDEXED_OFFSET], null, TIMEOUT).join();
        }

        private CompletableFuture<DirectSegmentAccess> getSegment(String segmentName, Duration timeout) {
            Preconditions.checkArgument(segmentName.equals(SEGMENT_NAME));
            return CompletableFuture.supplyAsync(() -> this.segmentMock, executorService());
        }
    }
}
