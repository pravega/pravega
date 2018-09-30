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

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.UpdateListener;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ContainerTableExtensionImpl} class.
 */
public class ContainerTableExtensionImplTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1;
    private static final long SEGMENT_ID = 2L;
    private static final String SEGMENT_NAME = "TableSegment";
    private static final int MAX_KEY_LENGTH = 512;
    private static final int MAX_VALUE_LENGTH = 256;
    private static final int UPDATE_COUNT = 10000;
    private static final int UPDATE_BATCH_SIZE = 689;
    private static final double REMOVE_FRACTION = 0.3; // 30% of generated operations are removes.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the ability to create and delete TableSegments.
     */
    @Test
    public void testCreateDelete() {
        @Cleanup
        val context = new TestContext();
        val nonTableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.createSegmentMetadata());
        Assert.assertTrue("Not expecting any Writer Table Processors for non-table segment.", nonTableSegmentProcessors.isEmpty());

        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertNotNull("Segment not created", context.segment());
        val tableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.segment().getMetadata());
        Assert.assertFalse("Expecting Writer Table Processors for table segment.", tableSegmentProcessors.isEmpty());

        context.ext.deleteSegment(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertNull("Segment not deleted", context.segment());
    }

    /**
     * Tests the ability to perform unconditional updates and removals using a {@link KeyHasher} that is not prone to
     * collisions.
     */
    @Test
    public void testUnconditionalUpdates() {
        testUnconditionalUpdatesWithHasher(KeyHashers.DEFAULT_HASHER);
    }

    /**
     * Tests the ability to perform unconditional updates and removals using a {@link KeyHasher} that is very prone to
     * collisions.
     */
    @Test
    public void testUnconditionalUpdatesCollisions() {
        testUnconditionalUpdatesWithHasher(KeyHashers.COLLISION_HASHER);
    }

    /**
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is not prone to collisions.
     */
    @Test
    public void testConditionalUpdates() {
        testConditionalUpdatesWithHasher(KeyHashers.DEFAULT_HASHER);
    }

    /**
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testConditionalUpdatesWithCollisions() {
        testConditionalUpdatesWithHasher(KeyHashers.COLLISION_HASHER);
    }

    /**
     * Tests the ability to resume operations after a recovery event. Scenarios include:
     * - Index is up-to-date ({@link Attributes#TABLE_INDEX_OFFSET} equals Segment.Length.
     * - Index is not up-to-date ({@link Attributes#TABLE_INDEX_OFFSET} is less than Segment.Length.
     */
    @Test
    public void testRecovery() {
        // Generate a set of TestEntryData (List<TableEntry>, ExpectedResults.
        // Process each TestEntryData in turn.  After each time, re-create the Extension.
        // Verify gets are blocked on indexing. Then index, verify unblocked and then re-create the Extension, and verify again.
        // TODO: implement; maybe merge with testUnconditionalUpdatesWithHasher.
    }

    /**
     * Verifies that the methods that are not yet implemented are not implemented by accident without unit tests.
     * This test should be removed once every method tested in it is implemented.
     */
    @Test
    public void testUnimplementedMethods() {
        @Cleanup
        val context = new TestContext();
        AssertExtensions.assertThrows(
                "merge() is implemented.",
                () -> context.ext.merge(SEGMENT_NAME, SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "seal() is implemented.",
                () -> context.ext.seal(SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "keyIterator() is implemented.",
                () -> context.ext.keyIterator(SEGMENT_NAME, null, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "entryIterator() is implemented.",
                () -> context.ext.entryIterator(SEGMENT_NAME, null, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "registerListener() is implemented.",
                () -> context.ext.registerListener(UpdateListener.builder().build(), TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "unregisterListener() is implemented.",
                () -> context.ext.unregisterListener(UpdateListener.builder().build()),
                ex -> ex instanceof UnsupportedOperationException);
    }


    private void testUnconditionalUpdatesWithHasher(KeyHasher keyHasher) {
        @Cleanup
        val context = new TestContext(keyHasher);

        // Generate a set of TestEntryData (List<TableEntry>, ExpectedResults.
        // Process each TestEntryData in turn. Each time, verify result.
        // At the end, wait for the data to be indexed, flush the cache and verify result.

        val data = generateTestData(context);
        //TODO: left here
    }


    private void testConditionalUpdatesWithHasher(KeyHasher keyHasher) {
        @Cleanup
        val context = new TestContext(keyHasher);
        // Generate a set of TestEntryData (List<TableEntry>, ExpectedResults.
        // Process each TestEntryData in turn. Each time, try with bad input, then good input, then verify result.
        // At the end, wait for the data to be indexed, flush the cache and verify result.
        // TODO: implement; maybe merge with testUnconditionalUpdatesWithHasher
    }

    private ArrayList<TestBatchData> generateTestData(TestContext context) {
        val result = new ArrayList<TestBatchData>();
        int count = 0;
        while (count < UPDATE_COUNT) {
            int batchSize = Math.min(UPDATE_BATCH_SIZE, UPDATE_COUNT - count);
            TestBatchData prev = result.isEmpty() ? null : result.get(result.size() - 1);
            result.add(generateAndPopulateEntriesBatch(batchSize, prev, context));
            count += batchSize;
        }

        return result;
    }


    private TestBatchData generateAndPopulateEntriesBatch(int batchSize, TestBatchData previous, TestContext context) {
        val expectedEntries = previous == null
                ? new HashMap<HashedArray, HashedArray>()
                : new HashMap<>(previous.expectedEntries);

        val removalCandidates = previous == null
                ? new ArrayList<HashedArray>()
                : new ArrayList<>(expectedEntries.keySet()); // Need a list so we can efficiently pick removal candidates.

        val toUpdate = new HashMap<HashedArray, HashedArray>();
        val toRemove = new ArrayList<HashedArray>();

        for (int i = 0; i < batchSize; i++) {
            // We only generate a remove if we have something to remove.
            boolean remove = removalCandidates.size() > 0 && (context.random.nextDouble() < REMOVE_FRACTION);
            if (remove) {
                val key = removalCandidates.get(context.random.nextInt(removalCandidates.size()));
                toRemove.add(key);
                removalCandidates.remove(key);
            } else {
                // Generate a new Table Entry.
                byte[] keyData = new byte[Math.max(1, context.random.nextInt(MAX_KEY_LENGTH))];
                context.random.nextBytes(keyData);
                byte[] valueData = new byte[context.random.nextInt(MAX_VALUE_LENGTH)];
                context.random.nextBytes(valueData);
                HashedArray key = new HashedArray(keyData);
                toUpdate.put(key, new HashedArray(valueData));
                removalCandidates.add(key);
            }
        }

        return new TestBatchData(toUpdate, toRemove, expectedEntries);
    }

    //region Helper Classes

    private class TestContext implements AutoCloseable {
        final KeyHasher hasher;
        final MockSegmentContainer container;
        final InMemoryCacheFactory cacheFactory;
        final CacheManager cacheManager;
        final ContainerTableExtensionImpl ext;
        final Random random;

        TestContext() {
            this(KeyHashers.DEFAULT_HASHER);
        }

        TestContext(KeyHasher hasher) {
            this.hasher = hasher;
            this.container = new MockSegmentContainer(() -> new SegmentMock(createSegmentMetadata(), executorService()));
            this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            this.ext = new ContainerTableExtensionImpl(this.container, this.cacheFactory, this.cacheManager, executorService());
            this.random = new Random(0);
        }

        @Override
        public void close() {
            this.ext.close();
            this.cacheManager.close();
            this.cacheFactory.close();
            this.container.close();
        }

        UpdateableSegmentMetadata createSegmentMetadata() {
            return new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);
        }

        SegmentMock segment() {
            return this.container.segment.get();
        }
    }

    private class MockSegmentContainer implements SegmentContainer {
        private final AtomicReference<SegmentMock> segment;
        private final Supplier<SegmentMock> segmentCreator;
        private final AtomicBoolean closed;

        MockSegmentContainer(Supplier<SegmentMock> segmentCreator) {
            this.segmentCreator = segmentCreator;
            this.segment = new AtomicReference<>();
            this.closed = new AtomicBoolean();
        }

        @Override
        public int getId() {
            return CONTAINER_ID;
        }

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> forSegment(String segmentName, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }

            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            return CompletableFuture.supplyAsync(() -> segment, executorService());
        }

        @Override
        public CompletableFuture<Void> createStreamSegment(String segmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
            if (this.segment.get() != null) {
                return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
            }

            return CompletableFuture
                    .runAsync(() -> {
                        SegmentMock segment = this.segmentCreator.get();
                        Assert.assertTrue(this.segment.compareAndSet(null, segment));
                    }, executorService())
                    .thenCompose(v -> this.segment.get().updateAttributes(attributes == null ? Collections.emptyList() : attributes, timeout));
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String segmentName, Duration timeout) {
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }
            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            Assert.assertTrue(this.segment.compareAndSet(segment, null));
            return CompletableFuture.completedFuture(null);
        }

        //region Not Implemented Methods

        @Override
        public Collection<SegmentProperties> getActiveSegments() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service startAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public State state() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service stopAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Throwable failureCause() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void addListener(Listener listener, Executor executor) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<SegmentProperties> mergeStreamSegment(String targetSegmentName, String sourceSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isOffline() {
            throw new UnsupportedOperationException("Not Expected");
        }

        //endregion
    }

    @RequiredArgsConstructor
    private class TestBatchData {
        /**
         * A Map of Keys to Values that need updating.
         */
        final Map<HashedArray, HashedArray> toUpdate;

        /**
         * A Collection of unique Keys that need removal.
         */
        final Collection<HashedArray> toRemove;

        /**
         * The expected result after toUpdate and toRemove have been applied. Key->Value.
         */
        final Map<HashedArray, HashedArray> expectedEntries;

    }
    //endregion
}
