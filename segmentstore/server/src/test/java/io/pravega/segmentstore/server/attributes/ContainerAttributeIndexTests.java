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
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
    private static final AttributeIndexConfig NO_SNAPSHOT_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, Integer.MAX_VALUE)
            .build();

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability to record Attribute values successively without involving snapshots.
     */
    @Test
    public void testNoSnapshots() {
        testRegularOperations(1000, 10, 5, NO_SNAPSHOT_CONFIG, false);
    }

    /**
     * Tests the ability to record Attribute values successively with small, frequent snapshots.
     */
    @Test
    public void testSmallSnapshots() {
        int batchSize = 7;
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, batchSize * 13)
                                         .build();
        testRegularOperations(100, batchSize, 5, config, true);
    }

    /**
     * Tests the ability to record Attribute values successively with large, infrequent snapshots.
     */
    @Test
    public void testLargeSnapshots() {
        int attributeCount = 5000;
        int batchSize = 17;
        int repeats = 3;
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, batchSize * repeats * attributeCount / 2)
                                         .build();
        testRegularOperations(attributeCount, batchSize, repeats, config, true);
    }

    /**
     * Tests the ability to Seal an Attribute Segment (create a final snapshot and disallow new changes).
     */
    @Test
    public void testSeal() {
        int attributeCount = 1000;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        val config = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 10)
                .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (UUID attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
            idx.put(attributeId, value, TIMEOUT).join();
        }

        // Check index before sealing.
        checkIndex(idx, expectedValues);

        idx.seal(TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Index allowed adding new values after being sealed.",
                () -> idx.put(UUID.randomUUID(), 1L, TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);

        // Check index again, after sealing.
        checkIndex(idx, expectedValues);
    }

    /**
     * Tests the case when Snapshots fail due to a Storage failure. This should prevent whatever operation triggered it
     * to completely fail and not record the data.
     */
    @Test
    public void testStorageFailure() {
        val attributeId = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, 100)
                                         .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        val idx = (SegmentAttributeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // 1. When writing normally
        context.storage.writeInterceptor = (streamSegmentName, offset, data, length, wrappedStorage) -> {
            throw new IntentionalException();
        };
        AssertExtensions.assertThrows(
                "put() worked with Storage failure.",
                () -> idx.put(attributeId, 0L, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertNull("A value was retrieved after a failed put().", idx.get(attributeId, TIMEOUT).join());

        // 2. When generating a snapshot.
        // Write as much as we can until we are about to create a snapshot (but don't do it yet).
        context.storage.writeInterceptor = null;
        while (!idx.shouldSnapshot()) {
            idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        }

        context.storage.writeInterceptor = (streamSegmentName, offset, data, length, wrappedStorage) -> {
            throw new IntentionalException();
        };
        AssertExtensions.assertThrows(
                "put()/snapshot worked with Storage failure.",
                () -> idx.put(attributeId, lastWrittenValue.get() + 1, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("An invalid value was retrieved after a failed put()/snapshot.",
                lastWrittenValue.get(), (long) idx.get(attributeId, TIMEOUT).join());

        AssertExtensions.assertThrows(
                "seal() worked with Storage failure.",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Tests the case when Snapshots fail due to OperationLogFailure. The snapshot itself must have recorded in the file,
     * but the Offset & Length attributes never updated on the main Segment.
     */
    @Test
    public void testOperationLogFailure() {
        val attributeId = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, 100)
                                         .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        val idx = (SegmentAttributeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Write as much as we can until we are about to create a snapshot (but don't do it yet).
        while (!idx.shouldSnapshot()) {
            idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        }

        Assert.assertEquals("Not expecting any snapshots so far.",
                0, context.lastSnapshotOffset(SEGMENT_ID) + context.lastSnapshotLength(SEGMENT_ID));

        // Verify the put() operation succeeds in this case and that the data is indeed inserted.
        context.operationLog.addInterceptor = op -> Futures.failedFuture(new IntentionalException());
        idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        Assert.assertEquals("Not expecting any snapshot locations to be recorded.",
                0, context.lastSnapshotOffset(SEGMENT_ID) + context.lastSnapshotLength(SEGMENT_ID));
        Assert.assertEquals("Expected value to be written even with OperationLog failure.",
                lastWrittenValue.get(), (long) idx.get(attributeId, TIMEOUT).join());

        AssertExtensions.assertThrows(
                "Expecting seal() to fail with OperationLogFailure.",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof IntentionalException);

        Assert.assertTrue("Expecting a snapshot to still be required.", idx.shouldSnapshot());

        // Clear the failures, and try again.
        context.operationLog.addInterceptor = null;
        idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Expected a snapshot location to be recorded after successful run.",
                0, context.lastSnapshotOffset(SEGMENT_ID) + context.lastSnapshotLength(SEGMENT_ID));
        Assert.assertEquals("Expected value to be written after successful run.",
                lastWrittenValue.get(), (long) idx.get(attributeId, TIMEOUT).join());
    }

    /**
     * Tests a scenario where there are multiple concurrent updates to the Attribute Segment, at least one of which involves
     * a Snapshot.
     */
    @Test
    public void testConditionalUpdates() {
        val attributeId = UUID.randomUUID();
        val attributeId2 = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, 100)
                                         .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        val idx = (SegmentAttributeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Write as much as we can until we are about to create a snapshot (but don't do it yet).
        context.storage.writeInterceptor = null;
        while (!idx.shouldSnapshot()) {
            idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        }

        // We intercept the next write (which should be the snapshot being written). When doing so, we write a new value
        // for the attribute behind the scenes - we will then verify that this attribute was preserved.
        AtomicBoolean intercepted = new AtomicBoolean(false);
        context.storage.writeInterceptor = (streamSegmentName, offset, data, length, wrappedStorage) -> {
            if (intercepted.compareAndSet(false, true)) {
                ArrayView s = idx.serialize(SegmentAttributeIndex.AttributeCollection
                        .builder()
                        .attributes(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()))
                        .build());
                return context.storage.write(idx.getAttributeSegmentHandle(), offset, s.getReader(), s.getLength(), TIMEOUT)
                                      .thenCompose(v -> Futures.failedFuture(new BadOffsetException(streamSegmentName, offset, offset)));
            } else {
                // Already intercepted.
                return CompletableFuture.completedFuture(null);
            }
        };

        // This call should trigger a snapshot. We want to use a different attribute so we can properly test the reconciliation
        // algorithm by validating the written value for the other attribute.
        idx.put(attributeId2, 0L, TIMEOUT).join();
        Assert.assertEquals("Unexpected value after reconciliation.", lastWrittenValue.get(), (long) idx.get(attributeId, TIMEOUT).join());
        Assert.assertEquals("Unexpected value for second attribute after reconciliation.", 0L, (long) idx.get(attributeId2, TIMEOUT).join());
    }

    /**
     * Tests reading from the Attribute Segment but while a truncation was in progress. Scenario:
     * 1. We have 2 snapshots and some data in between
     * 2. Main Segment metadata points to the first snapshot.
     * 3. We begin reading the first snapshot, but in the meantime, the metadata offsets were updated and the segment has been truncated
     * (to the second snapshot)
     * 4. The code should be able to handle and recover from this.
     */
    @Test
    public void testTruncatedSegment() {
        val attributeId = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val snapshotTriggerSize = 100;
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.SNAPSHOT_TRIGGER_SIZE, snapshotTriggerSize)
                                         .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, snapshotTriggerSize / 10)
                                         .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        val idx = (SegmentAttributeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Write as much as we can until we are about to create a snapshot (but don't do it yet).
        for (int i = 0; i < 2; i++) {
            idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
            while (!idx.shouldSnapshot()) {
                idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
            }
        }

        // The next put() call should generate a snapshot. Fail its OperationLog add() call, so that the snapshot does get
        // generated, but not recorded.
        AtomicLong lastSnapshotOffset = new AtomicLong();
        context.operationLog.addInterceptor = op -> {
            // Record the last snapshot offset. The length shouldn't have changed, or matter.
            lastSnapshotOffset.set(((UpdateAttributesOperation) op).getAttributeUpdates().stream()
                                                                   .filter(au -> au.getAttributeId() == Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET)
                                                                   .findFirst().orElse(null).getValue());
            return Futures.failedFuture(new IntentionalException());
        };
        idx.put(attributeId, lastWrittenValue.incrementAndGet(), TIMEOUT).join();
        context.operationLog.addInterceptor = null; // No more OperationLog errors.

        AssertExtensions.assertGreaterThan("Expected a second snapshot to have been generated.",
                context.lastSnapshotOffset(SEGMENT_ID), lastSnapshotOffset.get());

        // After the first read was initiated, we finally update the main segment's metadata attributes for snapshot location
        // and we truncate the segment (to somewhere after the first snapshot).
        AtomicBoolean intercepted = new AtomicBoolean(false);
        context.storage.readInterceptor = (segmentName, offset, wrappedStorage) -> {
            if (intercepted.compareAndSet(false, true)) {
                context.containerMetadata
                        .getStreamSegmentMetadata(SEGMENT_ID)
                        .updateAttributes(Collections.singletonMap(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, lastSnapshotOffset.get()));

                return context.storage.openWrite(StreamSegmentNameUtils.getAttributeSegmentName(SEGMENT_NAME))
                                      .thenCompose(h -> context.storage.truncate(h, lastSnapshotOffset.get(), TIMEOUT));
            } else {
                // We only do this once.
                return CompletableFuture.completedFuture(null);
            }
        };

        // Finally, verify we can read the data we want to.
        val value = idx.get(attributeId, TIMEOUT).join();
        Assert.assertEquals("Unexpected value read.", lastWrittenValue.get(), (long) value);
        Assert.assertTrue("No interception done.", intercepted.get());
    }

    /**
     * Verifies we cannot create indices for Deleted Segments.
     */
    @Test
    public void testSegmentNotExists() {
        @Cleanup
        val context = new TestContext(NO_SNAPSHOT_CONFIG);
        populateSegments(context);

        // Create one index before main segment deletion and make sure the attribute file is there.
        val idx1 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx1.put(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT).join();

        // Delete Segment.
        context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID).markDeleted();

        // Verify relevant operations cannot proceed.
        AssertExtensions.assertThrows(
                "put() worked on deleted segment.",
                () -> idx1.put(Collections.singletonMap(UUID.randomUUID(), 2L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "get() worked on deleted segment.",
                () -> idx1.get(UUID.randomUUID(), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "seal() worked on deleted segment.",
                () -> idx1.seal(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Verify we cannot create new indices.
        AssertExtensions.assertThrows(
                "forSegment() worked on deleted segment.",
                () -> context.index.forSegment(SEGMENT_ID, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
        Assert.assertFalse("Attribute segment exists in Storage after a deletion was detected.",
                context.storage.exists(StreamSegmentNameUtils.getAttributeSegmentName(SEGMENT_NAME), TIMEOUT).join());
    }

    private void testRegularOperations(int attributeCount, int batchSize, int repeats, AttributeIndexConfig config, boolean expectSnapshots) {
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // We verify the correctness of the index after every notification that a snapshot was created.
        AtomicLong lastSnapshotEndOffset = new AtomicLong(0);
        val updateBatch = new HashMap<UUID, Long>();
        Runnable commitBatch = () -> {
            idx.put(updateBatch, TIMEOUT).join();
            updateBatch.clear();
            long offset = context.lastSnapshotOffset(SEGMENT_ID);
            int length = context.lastSnapshotLength(SEGMENT_ID);
            long lastEnd = lastSnapshotEndOffset.getAndSet(offset + length);
            if (lastEnd != offset + length) {
                // Verify we were expecting snapshots and that they were in order.
                Assert.assertTrue("Not expecting any snapshots for this test.", expectSnapshots);
                AssertExtensions.assertGreaterThan("Expected the snapshot to have a positive length.", 0, length);
                AssertExtensions.assertGreaterThan("Expected snapshot offsets to be increasing.", lastEnd, offset);

                // Verify the index right after a snapshot.
                checkIndex(idx, expectedValues);
            }
        };

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (int r = 0; r < repeats; r++) {
            for (UUID attributeId : attributes) {
                long value = nextValue.getAndIncrement();
                expectedValues.put(attributeId, value);
                updateBatch.put(attributeId, value);
                if (updateBatch.size() % batchSize == 0) {
                    commitBatch.run();
                }
            }
        }

        // Commit any leftovers.
        if (updateBatch.size() > 0) {
            commitBatch.run();
        }

        checkIndex(idx, expectedValues);

        // 2. Reload index and verify it still has the correct values.
        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        checkIndex(idx2, expectedValues);

        // 3. Remove all values.
        idx2.remove(expectedValues.keySet(), TIMEOUT).join();
        expectedValues.replaceAll((key, v) -> SegmentMetadata.NULL_ATTRIBUTE_VALUE);
        checkIndex(idx2, expectedValues);
    }

    private void checkIndex(AttributeIndex index, Map<UUID, Long> expectedValues) {
        val actual = index.get(expectedValues.keySet(), TIMEOUT).join();
        val expected = expectedValues.entrySet().stream()
                                     .filter(e -> e.getValue() != SegmentMetadata.NULL_ATTRIBUTE_VALUE)
                                     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        AssertExtensions.assertMapEquals("Unexpected attributes in index.", expected, actual);
    }

    private void populateSegments(TestContext context) {
        val sm = context.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        sm.setLength(0);
        sm.setStorageLength(0);
    }

    private class TestContext implements AutoCloseable {
        final InMemoryStorage memoryStorage;
        final TestStorage storage;
        final UpdateableContainerMetadata containerMetadata;
        final ContainerAttributeIndex index;
        final TestOperationLog operationLog;

        TestContext(AttributeIndexConfig config) {
            this.memoryStorage = new InMemoryStorage();
            this.memoryStorage.initialize(1);
            this.storage = new TestStorage(new RollingStorage(this.memoryStorage, config.getAttributeSegmentRollingPolicy()), executorService());
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.operationLog = new TestOperationLog();
            this.index = new ContainerAttributeIndex(this.containerMetadata, this.storage, this.operationLog, config, executorService());
        }

        long lastSnapshotOffset(long segmentId) {
            return this.containerMetadata.getStreamSegmentMetadata(segmentId).getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, 0L);
        }

        int lastSnapshotLength(long segmentId) {
            return (int) (long) this.containerMetadata.getStreamSegmentMetadata(segmentId).getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, 0L);
        }

        @Override
        public void close() {
            this.storage.close();
            this.memoryStorage.close();
        }

        private class TestStorage extends AsyncStorageWrapper {
            private final SyncStorage wrappedStorage;
            private WriteInterceptor writeInterceptor;
            private ReadInterceptor readInterceptor;

            TestStorage(SyncStorage syncStorage, Executor executor) {
                super(syncStorage, executor);
                this.wrappedStorage = syncStorage;
            }

            @Override
            public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
                WriteInterceptor wi = this.writeInterceptor;
                if (wi != null) {
                    return wi.apply(handle.getSegmentName(), offset, data, length, this.wrappedStorage)
                             .thenCompose(v -> super.write(handle, offset, data, length, timeout));
                } else {
                    return super.write(handle, offset, data, length, timeout);
                }
            }

            @Override
            public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
                ReadInterceptor ri = this.readInterceptor;
                if (ri != null) {
                    return ri.apply(handle.getSegmentName(), offset, this.wrappedStorage)
                             .thenCompose(v -> super.read(handle, offset, buffer, bufferOffset, length, timeout));
                } else {
                    return super.read(handle, offset, buffer, bufferOffset, length, timeout);
                }
            }
        }

        private class TestOperationLog implements OperationLog {
            private Function<Operation, CompletableFuture<Void>> addInterceptor;

            @Override
            public CompletableFuture<Void> add(Operation operation, Duration timeout) {
                val ai = this.addInterceptor;
                if (ai != null) {
                    return ai.apply(operation);
                } else {
                    return CompletableFuture.runAsync(() -> {
                        if (!(operation instanceof UpdateAttributesOperation)) {
                            throw new AssertionError("Unexpected operation: " + operation);
                        }

                        UpdateAttributesOperation u = (UpdateAttributesOperation) operation;
                        containerMetadata.getStreamSegmentMetadata(u.getStreamSegmentId())
                                         .updateAttributes(u.getAttributeUpdates().stream()
                                                            .collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue)));
                    }, executorService());
                }
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

    @FunctionalInterface
    interface WriteInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, long offset, InputStream data, int length, SyncStorage wrappedStorage);
    }

    @FunctionalInterface
    interface ReadInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, long offset, SyncStorage wrappedStorage);
    }
}
