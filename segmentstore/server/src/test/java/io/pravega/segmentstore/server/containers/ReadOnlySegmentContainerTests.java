/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReaderTests;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ReadOnlySegmentContainer class.
 */
public class ReadOnlySegmentContainerTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_LENGTH = 3 * ReadOnlySegmentContainer.MAX_READ_AT_ONCE_BYTES;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String SEGMENT_NAME = "Segment";
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the getStreamSegmentInfo() method.
     */
    @Test
    public void testGetStreamSegmentInfo() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();

        // Non-existent segment.
        AssertExtensions.assertThrows(
                "Unexpected exception when the segment does not exist.",
                () -> context.container.getStreamSegmentInfo(SEGMENT_NAME, false, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Create a segment, add some data, set some attributes, "truncate" it and then seal it.
        val storageInfo = context.storage.create(SEGMENT_NAME, TIMEOUT)
                .thenCompose(si -> context.storage.openWrite(si.getName()))
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT))
                .thenCompose(v -> context.storage.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT)).join();
        val expectedInfo = StreamSegmentInformation.from(storageInfo)
                .startOffset(storageInfo.getLength() / 2)
                .attributes(ImmutableMap.of(UUID.randomUUID(), 100L, Attributes.EVENT_COUNT, 1L))
                .build();
        context.stateStore.put(SEGMENT_NAME, new SegmentState(1, expectedInfo), TIMEOUT).join();

        // Fetch the SegmentInfo from the ReadOnlyContainer and verify it is as expected.
        val actual = context.container.getStreamSegmentInfo(SEGMENT_NAME, false, TIMEOUT).join();
        Assert.assertEquals("Unexpected Name.", expectedInfo.getName(), actual.getName());
        Assert.assertEquals("Unexpected Length.", expectedInfo.getLength(), actual.getLength());
        Assert.assertEquals("Unexpected Sealed status.", expectedInfo.isSealed(), actual.isSealed());
        Assert.assertEquals("Unexpected Start Offset.", expectedInfo.getStartOffset(), actual.getStartOffset());
        val expectedAttributes = Attributes.getCoreNonNullAttributes(expectedInfo.getAttributes());
        AssertExtensions.assertMapEquals("Unexpected Attributes.", expectedAttributes, actual.getAttributes());
    }

    /**
     * Tests the read() method.
     */
    @Test
    public void testRead() throws Exception {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();
        val writtenData = populate(SEGMENT_LENGTH, 0, context);
        val originalInfo = context.container.getStreamSegmentInfo(SEGMENT_NAME, false, TIMEOUT).join();
        @Cleanup
        val rr = context.container.read(SEGMENT_NAME, 0, writtenData.length, TIMEOUT).join();
        StreamSegmentStorageReaderTests.verifyReadResult(rr, originalInfo, 0, writtenData.length, writtenData);

        // Truncate half of the segment.
        val truncatedInfo = StreamSegmentInformation.from(originalInfo)
                .startOffset(originalInfo.getLength() / 2)
                .attributes(Collections.singletonMap(UUID.randomUUID(), 100L))
                .build();
        context.stateStore.put(SEGMENT_NAME, new SegmentState(1, truncatedInfo), TIMEOUT).join();

        // Verify that reading from a truncated offset doesn't work.
        @Cleanup
        val rrTruncated1 = context.container.read(SEGMENT_NAME, 0, writtenData.length, TIMEOUT).join();
        StreamSegmentStorageReaderTests.verifyReadResult(rrTruncated1, truncatedInfo, 0, 0, writtenData);

        // Verify that reading from a non-truncated offset does work.
        @Cleanup
        val rrTruncated2 = context.container.read(SEGMENT_NAME, truncatedInfo.getStartOffset(), writtenData.length, TIMEOUT).join();
        StreamSegmentStorageReaderTests.verifyReadResult(rrTruncated2, truncatedInfo, (int) truncatedInfo.getStartOffset(),
                (int) (writtenData.length - truncatedInfo.getStartOffset()), writtenData);
    }

    /**
     * Tests the read() method when the segment does not exist.
     */
    @Test
    public void testReadInexistentSegment() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();

        AssertExtensions.assertThrows(
                "Unexpected exception when the segment does not exist.",
                () -> context.container.read(SEGMENT_NAME, 0, 1, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Verifies that non-read operations are not supported.
     */
    @Test
    public void testUnsupportedOperations() {
        @Cleanup
        val context = new TestContext();
        context.container.startAsync().awaitRunning();
        assertUnsupported("createStreamSegment", () -> context.container.createStreamSegment(SEGMENT_NAME, null, TIMEOUT));
        assertUnsupported("append", () -> context.container.append(SEGMENT_NAME, new byte[1], null, TIMEOUT));
        assertUnsupported("append-offset", () -> context.container.append(SEGMENT_NAME, 0, new byte[1], null, TIMEOUT));
        assertUnsupported("sealStreamSegment", () -> context.container.sealStreamSegment(SEGMENT_NAME, TIMEOUT));
        assertUnsupported("truncateStreamSegment", () -> context.container.truncateStreamSegment(SEGMENT_NAME, 0, TIMEOUT));
        assertUnsupported("deleteStreamSegment", () -> context.container.deleteStreamSegment(SEGMENT_NAME, TIMEOUT));
        assertUnsupported("mergeTransaction", () -> context.container.mergeStreamSegment(SEGMENT_NAME, SEGMENT_NAME, TIMEOUT));
    }

    private byte[] populate(int length, int truncationOffset, TestContext context) {
        val rnd = new Random(0);
        byte[] data = new byte[length];
        rnd.nextBytes(data);

        val storageInfo = context.storage.create(SEGMENT_NAME, TIMEOUT)
                .thenCompose(si -> context.storage.openWrite(si.getName()))
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT))
                .thenCompose(v -> context.storage.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT)).join();
        val expectedInfo = StreamSegmentInformation.from(storageInfo)
                .startOffset(truncationOffset)
                .attributes(Collections.singletonMap(UUID.randomUUID(), 100L))
                .build();
        context.stateStore.put(SEGMENT_NAME, new SegmentState(1, expectedInfo), TIMEOUT).join();
        return data;
    }

    private void assertUnsupported(String name, Supplier<CompletableFuture<?>> toTest) {
        AssertExtensions.assertThrows(
                name + "did not throw expected exception.",
                toTest::get,
                ex -> ex instanceof UnsupportedOperationException);
    }

    private class TestContext implements AutoCloseable {
        final SegmentContainer container;
        final Storage storage;
        final AsyncMap<String, SegmentState> stateStore;
        private final InMemoryStorageFactory storageFactory;

        TestContext() {
            this.storageFactory = new InMemoryStorageFactory(executorService());
            this.container = new ReadOnlySegmentContainer(this.storageFactory, executorService());
            this.storage = this.storageFactory.createStorageAdapter();
            this.stateStore = new SegmentStateStore(this.storage, executorService());
        }

        @Override
        public void close() {
            this.container.close();
            this.storage.close();
            this.storageFactory.close();
        }
    }
}
