/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ContainerAttributeIndex and SegmentAttributeIndex classes.
 */
public class ContainerAttributeIndexTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 9999;
    private static final String SEGMENT_NAME = "Segment";
    private static final long SEGMENT_ID = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(10000);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability to record Attribute values successively without involving snapshots.
     */
    @Test
    public void testPutNoSnapshots() {
        final int attributeCount = 1000;
        final int batchSize = 10;
        final int repeats = 5;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext();
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        AtomicLong nextValue = new AtomicLong(0);
        val expectedValues = new HashMap<UUID, Long>();

        for (int r = 0; r < repeats; r++) {
            val updateBatch = new HashMap<UUID, Long>();
            for (UUID attributeId : attributes) {
                long value = nextValue.getAndIncrement();
                expectedValues.put(attributeId, value);
                updateBatch.put(attributeId, value);
                if (updateBatch.size() % batchSize == 0) {
                    idx.put(updateBatch, TIMEOUT).join();
                    updateBatch.clear();
                }
            }
        }

        Assert.assertEquals("Not expecting any snapshots so far.", 0, context.snapshotOffsets.size());
        checkIndex(idx, expectedValues);

        // 2. Reload index and verify it still has the correct values.
        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        checkIndex(idx2, expectedValues);
    }

    @Test
    public void testAutoSnapshots() {

    }

    @Test
    public void testLargeSnapshots() {

    }

    @Test
    public void testTruncatedSegment() {

    }

    @Test
    public void testConditionalUpdates() {

    }

    @Test
    public void testSeal() {
    }

    @Test
    public void testForSegmentNotExists() {

    }

    private void checkIndex(AttributeIndex index, Map<UUID, Long> expectedValues) {
        val actualValues = index.get(expectedValues.keySet(), TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes in index.", expectedValues, actualValues);
    }

    private void populateSegments(TestContext context) {
        val sm = context.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        sm.setLength(0);
        sm.setStorageLength(0);
    }

    private class TestContext implements AutoCloseable {
        final Storage storage;
        final UpdateableContainerMetadata containerMetadata;
        final ContainerAttributeIndex index;
        final List<SegmentAttributeIndex.WriteInfo> snapshotOffsets;

        TestContext() {
            this.storage = new InMemoryStorageFactory(executorService()).createStorageAdapter();
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.snapshotOffsets = Collections.synchronizedList(new ArrayList<>());
            this.index = new ContainerAttributeIndex(this.containerMetadata, this.storage, new TestOperationLog(), executorService());
        }

        @Override
        public void close() {
            this.storage.close();
        }

        private class TestOperationLog implements OperationLog {

            @Override
            public CompletableFuture<Void> add(Operation operation, Duration timeout) {
                if (!(operation instanceof UpdateAttributesOperation)) {
                    return Futures.failedFuture(new AssertionError("Unexpected operation: " + operation));
                }
                UpdateAttributesOperation u = (UpdateAttributesOperation) operation;
                long offset = u.getAttributeUpdates().stream()
                        .filter(au -> au.getAttributeId() == Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET)
                        .findFirst().orElse(null).getValue();
                int length = (int) u.getAttributeUpdates().stream()
                        .filter(au -> au.getAttributeId() == Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH)
                        .findFirst().orElse(null).getValue();
                snapshotOffsets.add(new SegmentAttributeIndex.WriteInfo(offset, length));
                return CompletableFuture.completedFuture(null);
            }

            //region Unimplemented methods

            @Override
            public CompletableFuture<Void> truncate(long upToSequence, Duration timeout) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Void> operationProcessingBarrier(Duration timeout) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CompletableFuture<Void> awaitOnline() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isOffline() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Service startAsync() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isRunning() {
                throw new UnsupportedOperationException();
            }

            @Override
            public State state() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Service stopAsync() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void awaitRunning() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void awaitRunning(long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void awaitTerminated() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void awaitTerminated(long timeout, TimeUnit unit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Throwable failureCause() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void addListener(Listener listener, Executor executor) {
                throw new UnsupportedOperationException();
            }

            //endregion
        }
    }
}
