/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Function;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * Tests for testing bootstrap functionality with {@link SystemJournal}.
 * The test scenarios are executed by creating journal entries, writing chunks and making metadata updates directly
 * without actually creating {@link ChunkedSegmentStorage} instances.
 */
public class SystemJournalOperationsTests extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(3000);
    private static final int CONTAINER_ID = 42;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    protected ChunkMetadataStore createMetadataStore() {
        return new InMemoryMetadataStore(executorService());
    }

    protected ChunkStorage createChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    /// region Test scenarios.
    private TestAction[][] getSimpleScenarioActions(TestContext testContext, String testSegmentName) {
        return new TestAction[][]{
                new TestAction[]{
                        new AddChunkAction(testSegmentName, 1),
                        new TimeAction(testContext.getTimeForCycles(1)),
                        new AddChunkAction(testSegmentName, 2),
                        new AddChunkAction(testSegmentName, 3),
                        new TimeAction(testContext.getTimeForCycles(1)),
                        new AddChunkAction(testSegmentName, 4),
                },
                new TestAction[]{
                        new TruncateAction(testSegmentName, 0),
                        new TimeAction(testContext.getTimeForCycles(1)),
                        new TruncateAction(testSegmentName, 4),
                        new AddChunkAction(testSegmentName, 5),
                        new TruncateAction(testSegmentName, 6),
                        new TimeAction(testContext.getTimeForCycles(1)),
                        new TruncateAction(testSegmentName, 8),
                }
        };
    }
    /// end region

    /**
     * Test following simple scenario.
     * 1. Add 4 chunks to system tests.
     * 2. Bootstrap a new instance.
     * 3. Validate.
     * 4. Truncate.
     * 5. bootstrap a new instance.
     * 6. Validate.
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        val testContext = new TestContext(CONTAINER_ID);
        val testSegmentName = testContext.segmentNames[0];
        @Cleanup
        val instance =  new TestInstance(testContext, 1);
        instance.bootstrap();
        instance.validate();
        // Add 4 chunks
        instance.append(testSegmentName, "A", 0, 1);
        instance.append(testSegmentName, "B", 1, 2);
        instance.append(testSegmentName, "C", 3, 3);
        instance.append(testSegmentName, "D", 6, 4);

        // Bootstrap.
        @Cleanup
        val instance2 =  new TestInstance(testContext, 2);
        instance2.bootstrap();

        // Validate.
        instance2.validate();
        TestUtils.checkSegmentBounds(instance2.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkSegmentLayout(instance2.metadataStore, testSegmentName, new long[] { 1, 2, 3, 4});
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, instance2.metadataStore, testSegmentName);
        val segmentMetadata = TestUtils.getSegmentMetadata(instance2.metadataStore, testSegmentName);
        Assert.assertEquals("A", segmentMetadata.getFirstChunk());
        Assert.assertEquals("D", segmentMetadata.getLastChunk());
        Assert.assertEquals(0, segmentMetadata.getFirstChunkStartOffset());
        Assert.assertEquals(6, segmentMetadata.getLastChunkStartOffset());

        // Truncate
        instance2.truncate(testSegmentName, 4);

        // Bootstrap a new instance.
        @Cleanup
        val instance3 =  new TestInstance(testContext, 3);
        instance3.bootstrap();
        instance3.validate();

        // Validate.
        TestUtils.checkSegmentBounds(instance3.metadataStore, testSegmentName, 4, 10);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, instance3.metadataStore, testSegmentName);
        val segmentMetadata2 = TestUtils.getSegmentMetadata(instance3.metadataStore, testSegmentName);
        Assert.assertEquals("C", segmentMetadata2.getFirstChunk());
        Assert.assertEquals("D", segmentMetadata2.getLastChunk());
        Assert.assertEquals(3, segmentMetadata2.getFirstChunkStartOffset());
        Assert.assertEquals(6, segmentMetadata2.getLastChunkStartOffset());
    }

    /**
     * Test following simple scenario.
     * 1. Add 2 chunks to system tests.
     * 2. Trigger checkpoint.
     * 3. Add another 2 chunks to system tests.
     * 4. Bootstrap a new instance.
     * 5. Validate.
     * 6. Truncate 2 times.
     * 7. Trigger checkpoint.
     * 8. Truncate another 2 times.
     * 9. bootstrap a new instance.
     * 10. Validate.
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenarioWithSnapshots() throws Exception {
        val testContext = new TestContext(CONTAINER_ID);
        testContext.setConfig(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxJournalRecordsPerSnapshot(2)
                .selfCheckEnabled(true)
                .build());

        val testSegmentName = testContext.segmentNames[0];

        @Cleanup
        val instance =  new TestInstance(testContext, 1);
        instance.bootstrap();
        instance.validate();

        // Add 2 chunks.
        instance.append(testSegmentName, "A", 0, 1);
        instance.append(testSegmentName, "B", 1, 2);

        // Trigger checkpoint.
        testContext.addTime(testContext.config.getJournalSnapshotCheckpointFrequency().toMillis() + 1);

        // Add another 2 chunks.
        instance.append(testSegmentName, "C", 3, 3);
        instance.append(testSegmentName, "D", 6, 4);

        // Bootstrap new instance.
        @Cleanup
        val instance2 =  new TestInstance(testContext, 2);
        instance2.bootstrap();
        instance2.validate();

        // Validate.
        TestUtils.checkSegmentBounds(instance2.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkSegmentLayout(instance2.metadataStore, testSegmentName, new long[] { 1, 2, 3, 4});
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, instance2.metadataStore, testSegmentName);
        val segmentMetadata = TestUtils.getSegmentMetadata(instance2.metadataStore, testSegmentName);
        Assert.assertEquals("A", segmentMetadata.getFirstChunk());
        Assert.assertEquals("D", segmentMetadata.getLastChunk());
        Assert.assertEquals(0, segmentMetadata.getFirstChunkStartOffset());
        Assert.assertEquals(6, segmentMetadata.getLastChunkStartOffset());

        // Truncate 2 times.
        instance2.truncate(testSegmentName, 1);
        instance2.truncate(testSegmentName, 2);

        // Trigger checkpoint.
        testContext.addTime(testContext.config.getJournalSnapshotCheckpointFrequency().toMillis() + 1);

        // Truncate another 2 times.
        instance2.truncate(testSegmentName, 3);
        instance2.truncate(testSegmentName, 4);

        // Bootstrap new instance.
        @Cleanup
        val instance3 =  new TestInstance(testContext, 3);
        instance3.bootstrap();
        instance3.validate();

        // Validate.
        TestUtils.checkSegmentBounds(instance3.metadataStore, testSegmentName, 4, 10);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, instance3.metadataStore, testSegmentName);
        val segmentMetadata2 = TestUtils.getSegmentMetadata(instance3.metadataStore, testSegmentName);
        Assert.assertEquals("C", segmentMetadata2.getFirstChunk());
        Assert.assertEquals("D", segmentMetadata2.getLastChunk());
        Assert.assertEquals(3, segmentMetadata2.getFirstChunkStartOffset());
        Assert.assertEquals(6, segmentMetadata2.getLastChunkStartOffset());

        // Bootstrap new instance.
        @Cleanup
        val instance4 =  new TestInstance(testContext, 4);
        instance4.bootstrap();
        instance4.validate();
        TestUtils.checkSegmentBounds(instance3.metadataStore, testSegmentName, 4, 10);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, instance3.metadataStore, testSegmentName);
    }

    /**
     * Test following simple scenario.
     * 1. Add 2 chunks to system tests.
     * 2. Trigger checkpoint.
     * 3. Add another 2 chunks to system tests.
     * 4. Bootstrap a new instance.
     * 5. Validate.
     * 6. Truncate 2 times.
     * 7. Trigger checkpoint.
     * 8. Truncate another 2 times.
     * 9. bootstrap a new instance.
     * 10. Validate.
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenarioWithActions() throws Exception {
        @Cleanup
        val testContext = new TestContext(CONTAINER_ID);
        testContext.setConfig(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxJournalRecordsPerSnapshot(2)
                .selfCheckEnabled(true)
                .build());
        val testSegmentName = testContext.segmentNames[0];
        testScenario(testContext, getSimpleScenarioActions(testContext, testSegmentName));
    }

    @Test
    public void testSimpleScenarioWithFlakyWrites() throws Exception {
        testScenarioWithFlakyWrites(this::testScenario, this::getSimpleScenarioActions);
    }

    @Test
    public void testSimpleScenarioWithFlakyWritesAfter() throws Exception {
        testScenarioWithFlakyWritesAfter(this::testScenario, this::getSimpleScenarioActions);
    }

    @Test
    public void testSimpleScenarioWithFlakyReadsAndWrites() throws Exception {
        testScenarioWithFlakyReadsAndWrites(this::testScenario, this::getSimpleScenarioActions);
    }

    @Test
    public void testSimpleScenarioWithFlakyReads() throws Exception {
        testWithFlakyReads(this::testScenario, this::getSimpleScenarioActions);
    }

    /**
     * Test truncate at various offsets.
     * @throws Exception Exception if any.
     */
    @Test
    public void testTruncateVariousOffsets() throws Exception {
        int maxChunkSize = 3;
        int numberOfChunks = 4;
        for (int i = 0; i < numberOfChunks; i++) {
            for (int j = 0; j < maxChunkSize; j++) {
                @Cleanup
                val testContext = new TestContext(CONTAINER_ID);
                testContext.setConfig(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .maxJournalRecordsPerSnapshot(2)
                        .selfCheckEnabled(true)
                        .build());
                val testSegmentName = testContext.segmentNames[0];
                val truncateAt = i * maxChunkSize + j;
                testTruncate(testContext, testSegmentName, maxChunkSize, numberOfChunks, truncateAt);
            }
        }
    }

    /**
     * Basic truncate scenarios.
     * @throws Exception Exception if any.
     */
    @Test
    public void testBaseTruncate() throws Exception {
        testTruncate(1, 2, 1);
        testTruncate(1, 4, 2);

        testTruncate(3, 2, 1);
        testTruncate(3, 4, 3);
    }

    private void testTruncate(TestContext testContext, String testSegmentName, int chunkSize, int chunkCount, int truncateAt) throws Exception {
        val sizes = new int[chunkCount];
        Arrays.fill(sizes, chunkSize);
        testTruncate(testContext, testSegmentName, sizes, truncateAt);
    }

    private void testTruncate(int chunkSize, int chunkCount, int truncateAt) throws Exception {
        @Cleanup
        val testContext = new TestContext(CONTAINER_ID);
        testContext.setConfig(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxJournalRecordsPerSnapshot(2)
                .selfCheckEnabled(true)
                .build());
        val testSegmentName = testContext.segmentNames[0];
        val sizes = new int[chunkCount];
        Arrays.fill(sizes, chunkSize);
        testTruncate(testContext, testSegmentName, sizes, truncateAt);
    }

    /**
     * Test truncate after adding some chunks.
     * @throws Exception Exception if any.
     */
    private void testTruncate(TestContext testContext, String testSegmentName, int[] chunkSizes, int truncateAt) throws Exception {
        TestAction[] additions = new TestAction[chunkSizes.length];
        for (int i = 0; i < chunkSizes.length; i++) {
            additions[i] = new AddChunkAction(testSegmentName, chunkSizes[i]);
        }
        testScenario(testContext,
                new TestAction[][] {
                    additions,
                    new TestAction[] { new TruncateAction(testSegmentName, truncateAt)}
                });
    }

    void testScenario(ChunkStorage chunkStorage, TestScenarioProvider scenarioProvider) throws Exception {
        @Cleanup
        val testContext = new TestContext(CONTAINER_ID, chunkStorage);
        testContext.setConfig(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxJournalRecordsPerSnapshot(2)
                .selfCheckEnabled(true)
                .build());
        val testSegmentName = testContext.segmentNames[0];
        val scenario = scenarioProvider.getScenario(testContext, testSegmentName);
        testScenario(testContext, scenario);
    }

    /**
     * Tests a scenario for given set of test actions.
     * @throws Exception Exception if any.
     */
    int testScenario(TestContext testContext, String segmentName, TestScenarioProvider scenarioProvider) throws Exception {
        val testSegmentName = testContext.segmentNames[0];
        val scenario = scenarioProvider.getScenario(testContext, testSegmentName);
        return testScenario(testContext, scenario);
    }

    /**
     * Tests a scenario for given set of test actions.
     * @throws Exception Exception if any.
     */
    int testScenario(TestContext testContext, TestAction[][] actions) throws Exception {
        int chunkId = 0;
        int epoch = 1;
        val segmentBounds = new HashMap<String, SegmentBounds>();
        Arrays.stream(SystemJournal.getChunkStorageSystemSegments(testContext.containerId))
                .forEach( segment -> segmentBounds.put(segment, new SegmentBounds(0, 0)));

        for (int i = 0; i < actions.length; i++) {
            @Cleanup
            val instance = new TestInstance(testContext, epoch);
            instance.bootstrap();
            instance.validate();
            for (String segment: SystemJournal.getChunkStorageSystemSegments(testContext.containerId)) {
                val bounds = segmentBounds.get(segment);
                TestUtils.checkSegmentBounds(instance.metadataStore, segment, bounds.startOffset, bounds.length);
            }

            for (int j = 0; j < actions[i].length; j++) {
                if (actions[i][j] instanceof AddChunkAction) {
                    val action = (AddChunkAction) actions[i][j];
                    val bounds = segmentBounds.get(action.segmentName);
                    instance.append(action.segmentName, "Chunk" + chunkId, bounds.length, action.chunkLength);
                    bounds.length += action.chunkLength;
                    chunkId++;
                }
                if (actions[i][j] instanceof TruncateAction) {
                    val action = (TruncateAction) actions[i][j];
                    val bounds = segmentBounds.get(action.segmentName);
                    instance.truncate(action.segmentName, action.offset);
                    bounds.startOffset = action.offset;
                }
                if (actions[i][j] instanceof TimeAction) {
                    val action = (TimeAction) actions[i][j];
                    testContext.addTime(action.timeToAdd);
                }
            }
            epoch++;
        }
        @Cleanup
        val instance = new TestInstance(testContext, epoch++);
        instance.bootstrap();
        instance.validate();
        return chunkId;
    }

    void testScenarioWithFlakyWrites(TestMethod test, TestScenarioProvider scenarioProvider) throws Exception {
        val primes = new int[] { 2, 3, 5, 7, 11, 13, 17};
        for (val prime : primes) {
            FlakyChunkStorage flakyChunkStorage = new FlakyChunkStorage(executorService());
            flakyChunkStorage.flakyPredicates.add(FlakinessPredicate.builder()
                    .method("doWrite.before")
                    .matchPredicate(n -> n % prime == 0)  // Fail every other
                    .matchRegEx("_sysjournal")
                    .action(() -> {
                        throw new IOException("Intentional");
                    })
                    .build());
            test.test(flakyChunkStorage, scenarioProvider);
        }
    }

    void testScenarioWithFlakyReadsAndWrites(TestMethod test, TestScenarioProvider scenarioProvider) throws Exception {
        val primes = new int[] {5, 7, 11, 13, 17};
        for (val prime1 : primes) {
            for (val prime2 : primes) {
                FlakyChunkStorage flakyChunkStorage = new FlakyChunkStorage(executorService());
                flakyChunkStorage.flakyPredicates.add(FlakinessPredicate.builder()
                        .method("doRead.before")
                        .matchPredicate(n -> n % prime1 == 0)  // Fail every other
                        .matchRegEx("_sysjournal")
                        .action(() -> {
                            throw new IOException("Intentional");
                        })
                        .build());
                flakyChunkStorage.flakyPredicates.add(FlakinessPredicate.builder()
                        .method("doWrite.before")
                        .matchPredicate(n -> n % prime2 == 0)  // Fail every other
                        .matchRegEx("_sysjournal")
                        .action(() -> {
                            throw new IOException("Intentional");
                        })
                        .build());
                test.test(flakyChunkStorage, scenarioProvider);
            }
        }
    }

    void testScenarioWithFlakyWritesAfter(TestMethod test, TestScenarioProvider scenarioProvider) throws Exception {
        val primes = new int[] { 2, 3, 5, 7, 11, 13, 17};
        for (val prime : primes) {
            FlakyChunkStorage flakyChunkStorage = new FlakyChunkStorage(executorService());
            flakyChunkStorage.flakyPredicates.add(FlakinessPredicate.builder()
                    .method("doWrite.after")
                    .matchPredicate(n -> n % prime == 0)  // Fail every other
                    .matchRegEx("_sysjournal")
                    .action(() -> {
                        throw new IOException("Intentional");
                    })
                    .build());
            test.test(flakyChunkStorage, scenarioProvider);
        }
    }

    void testWithFlakyReads(TestMethod test, TestScenarioProvider scenarioProvider) throws Exception {
        val primes = new int[] {3, 5, 7, 11, 13, 17};
        for (val prime : primes) {
            FlakyChunkStorage flakyChunkStorage = new FlakyChunkStorage(executorService());
            flakyChunkStorage.flakyPredicates.add(FlakinessPredicate.builder()
                    .method("doRead.before")
                    .matchPredicate(n -> n % prime == 0)  // Fail every other
                    .matchRegEx("_sysjournal")
                    .action(() -> {
                        throw new IOException("Intentional");
                    })
                    .build());
            test.test(flakyChunkStorage, scenarioProvider);
        }
    }


    /**
     * Represents a test method.
     */
    interface TestMethod {
        void test(ChunkStorage chunkStorage, TestScenarioProvider scenarioProvider) throws Exception;
    }

    /**
     * Represents a test method.
     */
    interface TestScenarioProvider {
        TestAction[][] getScenario(TestContext testContext, String testSegmentName);
    }

    @Builder
    @Data
    static class SegmentBounds {
        int length;
        int startOffset;
    }

    /**
     * Represents a test action on a segment.
     */
    static abstract class TestAction {
    }

    /**
     * Represents a test action on a segment.
     */
    @RequiredArgsConstructor
    static abstract class TestSegmentAction extends TestAction {
        @NonNull
        @Getter
        final String segmentName;
    }

    /**
     * Represents addition of chunk.
     */
    static class AddChunkAction extends TestSegmentAction {
        @Getter
        final int chunkLength;
        AddChunkAction(String name, int chunkLength) {
            super(name);
            this.chunkLength = chunkLength;
        }
    }

    /**
     * Represents truncation of chunk.
     */
    static class TruncateAction extends TestSegmentAction {
        @Getter
        final int offset;
        TruncateAction(String name, int offset) {
            super(name);
            this.offset = offset;
        }
    }

    /**
     * Represents moving test clock forward.
     */
    static class TimeAction extends TestAction {
        @Getter
        final long timeToAdd;
        TimeAction(long timeToAdd) {
            this.timeToAdd = timeToAdd;
        }
    }

    /**
     * Expected chunk info.
     * This is used during validation.
     */
    @Builder
    @Data
    static class ExpectedChunkInfo {
        String name;
        long metadataLength;
        long storageLength;
        long addedAtOffset;
    }

    /**
     * Expected segment info.
     * This is used during validation.
     */
    @Builder
    @Data
    static class ExpectedSegmentInfo {
        String name;
        long startOffset;
        long length;
        long lastChunkStartOffset;
        long firstChunkStartOffset;
    }

    /**
     * Test context for the test.
     */
    @Data
    class TestContext implements AutoCloseable {
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        ChunkStorage chunkStorage;
        HashMap<String, ArrayList<ExpectedChunkInfo>> expectedChunks = new HashMap<>();
        HashMap<String, ExpectedSegmentInfo> expectedSegments = new HashMap<>();
        String[] segmentNames;
        int containerId;
        long time = System.currentTimeMillis();

        TestContext(int containerId) throws Exception {
            this(containerId, createChunkStorage());
        }

        TestContext(int containerId, ChunkStorage chunkStorage) throws Exception {
            this.chunkStorage = chunkStorage;
            this.containerId = containerId;
            this.segmentNames = SystemJournal.getChunkStorageSystemSegments(containerId);
            for (String segment: segmentNames) {
                expectedSegments.put(segment, ExpectedSegmentInfo.builder()
                        .name(segment)
                        .build());
                expectedChunks.put(segment, new ArrayList<>());
            }
        }

        void addTime(long toAdd) {
            time += toAdd;
        }

        long getTimeForCycles(int i) {
            return config.getJournalSnapshotCheckpointFrequency().toMillis() * i + 1;
        }

        @Override
        public void close() throws Exception {
            chunkStorage.close();
        }
    }

    /**
     * Represents a test container instance.
     */
    @Data
    class TestInstance implements AutoCloseable {
        TestContext testContext;
        ChunkMetadataStore metadataStore;
        GarbageCollector garbageCollector;
        SegmentRollingPolicy policy;
        SystemJournal systemJournal;
        long epoch;

        TestInstance(TestContext testContext, long epoch) {
            this.testContext = testContext;
            this.epoch = epoch;
            this.metadataStore = createMetadataStore();
            this.garbageCollector = new GarbageCollector(testContext.containerId,
                    testContext.chunkStorage,
                    metadataStore,
                    testContext.config,
                    executorService());
            systemJournal = new SystemJournal(testContext.containerId, testContext.chunkStorage,
                    metadataStore, garbageCollector, () -> testContext.getTime(), testContext.config);
        }

        void bootstrap() throws Exception {
            systemJournal.bootstrap(epoch).get();
        }

        /**
         * Append a chunk.
         */
        void append(String segmentName, String chunkName, int offset, int length) throws Exception {
            append(segmentName, chunkName, offset, length, length);
        }

        /**
         * Append a chunk.
         */
        void append(String segmentName, String chunkName, int offset, int metadataLength, int storageLength) throws Exception {
            val segmentInfo = testContext.expectedSegments.get(segmentName);
            val list = testContext.expectedChunks.get(segmentName);

            // NOTE : Sequence of operation below exactly simulates the way operations are performed by ChunkedSegmentStorage.
            // Changing this sequence will result in incorrect behavior.
            // Create a chunk.
            testContext.chunkStorage.createWithContent(chunkName, storageLength, new ByteArrayInputStream(new byte[storageLength])).get();
            String oldChunkName = null;

            // Update the previous last chunk.
            if (list.size() > 0) {
                val lastChunkInfo = list.get(list.size() - 1);
                oldChunkName = lastChunkInfo.name;
            }

            // Commit to journal
            boolean done = false;
            while (!done) {
                try {
                    systemJournal.commitRecord(SystemJournal.ChunkAddedRecord.builder()
                            .newChunkName(chunkName)
                            .oldChunkName(oldChunkName)
                            .offset(offset)
                            .segmentName(segmentName)
                            .build());
                    done = true;
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    // FlakyChunkStorage may throw exception.
                }
            }

            // Update the data in metadata store.
            try (val txn = metadataStore.beginTransaction(false, segmentName)) {
                val segment = (SegmentMetadata) txn.get(segmentName).get();
                segment.setLastChunkStartOffset(offset);
                val lastChunk = (ChunkMetadata) txn.get(segment.getLastChunk()).get();
                segment.setLastChunk(chunkName);
                segment.setLength(segment.getLength() + metadataLength);
                segment.setChunkCount(segment.getChunkCount() + 1);
                // Adjust first and last chunk info as required.
                if (null != lastChunk) {
                    lastChunk.setNextChunk(chunkName);
                    txn.update(lastChunk);
                }
                if (null == segment.getFirstChunk()) {
                    segment.setFirstChunk(chunkName);
                }

                val newChunk = ChunkMetadata.builder()
                        .length(metadataLength)
                        .name(chunkName)
                        .status(1)
                        .build();
                // Validate
                segment.checkInvariants();

                // change and commit.
                txn.create(newChunk);
                txn.markPinned(newChunk);
                txn.update(segment);
                txn.commit();
            }

            // Add new chunk ro expected chunks list.
            list.add(ExpectedChunkInfo.builder()
                    .addedAtOffset(offset)
                    .storageLength(storageLength)
                    .name(chunkName)
                    .metadataLength(metadataLength)
                    .build());
            segmentInfo.setLength(offset + metadataLength);
            segmentInfo.setLastChunkStartOffset(offset);

            // Validate.
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, metadataStore, segmentName);
        }

        /**
         * Truncate.
         */
        void truncate(String segmentName, int offset) throws Exception {
            val list = testContext.expectedChunks.get(segmentName);
            val segmentInfo = testContext.expectedSegments.get(segmentName);

            // Figure out what chunks to delete.
            int toDelete = 0;
            boolean found = false;
            for (int i = 0; i < list.size(); i++) {
                val chunkInfo = list.get(i);
                toDelete = i;
                if (chunkInfo.getAddedAtOffset() + chunkInfo.getMetadataLength() > offset) {
                    found = true;
                    break;
                }
            }
            // Now delete the chunks.
            var deletedList = new ArrayList<String>();
            if (found) {
                // Delete chunks
                for (int i = 0; i < toDelete; i++) {
                    val deletedInfo = list.remove(0);
                    deletedList.add(deletedInfo.name);
                }

                // Update the expected data.
                segmentInfo.setStartOffset(offset);
                segmentInfo.setFirstChunkStartOffset(list.get(0).getAddedAtOffset());
                boolean done = false;
                while (!done) {
                    try {
                        systemJournal.commitRecord(SystemJournal.TruncationRecord.builder()
                                .offset(offset)
                                .startOffset(list.get(0).addedAtOffset)
                                .segmentName(segmentName)
                                .firstChunkName(list.get(0).name)
                                .build());
                        done = true;
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        // FlakyChunkStorage may throw exception.
                    }
                }

                // Update the metadata.
                try (val txn = metadataStore.beginTransaction(false, segmentName)) {
                    val segment = (SegmentMetadata) txn.get(segmentName).get();
                    for (val chunkToDelete : deletedList) {
                        txn.delete(chunkToDelete);
                    }

                    segment.setStartOffset(offset);
                    if (list.size() > 0) {
                        segment.setFirstChunk(list.get(0).name);
                        segment.setFirstChunkStartOffset(list.get(0).addedAtOffset);

                    } else {
                        segment.setFirstChunk(null);
                        segment.setFirstChunkStartOffset(offset);
                    }
                    segment.setChunkCount(list.size());
                    segment.checkInvariants();
                    txn.update(segment);
                    txn.commit();
                }
                // Finally delete chunks.
                for (val chunkToDelete : deletedList) {
                    testContext.chunkStorage.delete(ChunkHandle.writeHandle(chunkToDelete)).get();
                }
                TestUtils.checkChunksExistInStorage(testContext.chunkStorage, metadataStore, segmentName);
            }
        }

        /**
         * Validates the metadata against expected results.
         */
        void validate() throws Exception {
            Assert.assertEquals(1, systemJournal.getCurrentSnapshotIndex());
            Assert.assertEquals(0, systemJournal.getCurrentFileIndex());
            for (val expectedSegmentInfo : testContext.expectedSegments.values()) {
                // Check segment metadata.
                val expectedChunkInfoList =  testContext.expectedChunks.get(expectedSegmentInfo.name);
                val segmentMetadata = TestUtils.getSegmentMetadata(metadataStore, expectedSegmentInfo.name);

                val chunkList = TestUtils.getChunkList(metadataStore, expectedSegmentInfo.name);
                Assert.assertEquals(expectedSegmentInfo.startOffset, segmentMetadata.getStartOffset());
                Assert.assertEquals(expectedSegmentInfo.length, segmentMetadata.getLength());
                Assert.assertEquals(expectedSegmentInfo.firstChunkStartOffset, segmentMetadata.getFirstChunkStartOffset());

                Assert.assertEquals(expectedSegmentInfo.lastChunkStartOffset, segmentMetadata.getLastChunkStartOffset());
                Assert.assertEquals(expectedChunkInfoList.size(), segmentMetadata.getChunkCount());
                Assert.assertEquals(expectedChunkInfoList.size(), chunkList.size());
                Assert.assertEquals(epoch, segmentMetadata.getOwnerEpoch());

                // Check chunks.
                if (0 != expectedChunkInfoList.size()) {
                    Assert.assertEquals(expectedChunkInfoList.get(0).name, segmentMetadata.getFirstChunk());
                    Assert.assertEquals(expectedChunkInfoList.get(expectedChunkInfoList.size() - 1).name, segmentMetadata.getLastChunk());
                    Assert.assertEquals(expectedChunkInfoList.get(0).addedAtOffset, segmentMetadata.getFirstChunkStartOffset());
                    Assert.assertEquals(expectedChunkInfoList.get(expectedChunkInfoList.size() - 1).addedAtOffset, segmentMetadata.getLastChunkStartOffset());

                    Assert.assertEquals(expectedChunkInfoList.get(expectedChunkInfoList.size() - 1).addedAtOffset, segmentMetadata.getLastChunkStartOffset());

                    long offset = segmentMetadata.getFirstChunkStartOffset();
                    int i = 0;
                    for (val expectedChunkInfo : expectedChunkInfoList) {
                        val actual = chunkList.get(i);
                        Assert.assertEquals(expectedChunkInfo.name, actual.getName());
                        Assert.assertEquals(expectedChunkInfo.metadataLength, actual.getLength());
                        Assert.assertEquals(expectedChunkInfo.addedAtOffset, offset);
                        offset += expectedChunkInfo.metadataLength;
                        i++;
                    }
                    Assert.assertEquals(expectedSegmentInfo.length, offset);
                }

            }
        }

        @Override
        public void close() throws Exception {
            garbageCollector.close();
            metadataStore.close();
        }
    }

    /**
     * Defines the flaky behavior for the FlakyChunkStorage.
     */
    @Builder
    @Data
    @RequiredArgsConstructor
    static class FlakinessPredicate {
        @NonNull
        final String matchRegEx;
        @NonNull
        final String method;
        @NonNull
        final Function<Integer, Boolean> matchPredicate;
        @NonNull
        final Callable action;
    }

    /**
     * {@link ChunkStorage} implementation that fails predictably based on provided list of {@link FlakinessPredicate}.
     */
    static class FlakyChunkStorage extends InMemoryChunkStorage {
        /**
         * Predicate to evaluate against each invocation.
         */
        final ArrayList<FlakinessPredicate> flakyPredicates = new ArrayList<>();

        /**
         * Keeps track of invocations.
         */
        final HashMap<String, Integer> invokeCounts = new HashMap<>();

        FlakyChunkStorage(Executor executor) {
            super(executor);
        }

        FlakinessPredicate getMatchingPredicate(ChunkHandle handle, String method, int invocationCount) {
            return flakyPredicates.stream()
                    .filter(predicate -> predicate.method.equals(method)
                            && handle.getChunkName().contains(predicate.matchRegEx)
                            && predicate.matchPredicate.apply(invocationCount))
                    .findFirst()
                    .orElse(null);
        }

        private void intercept(ChunkHandle handle, String method) throws ChunkStorageException {
            int invocationCount = 0;
            if (!invokeCounts.containsKey(method)) {
                invokeCounts.put(method, 0);
            }
            invocationCount = invokeCounts.get(method);
            invokeCounts.put(method, invocationCount + 1);
            val predicate = getMatchingPredicate(handle, method, invocationCount);
            if (null != predicate) {
                try {
                    predicate.action.call();
                } catch (ChunkStorageException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ChunkStorageException(handle.getChunkName(), "Intentional Failure", e);
                }
            }
        }

        @Override
        protected int doWrite(ChunkHandle handle, long offset, int length, InputStream data) throws ChunkStorageException {
            intercept(handle, "doWrite.before");
            val ret = super.doWrite(handle, offset, length, data);
            intercept(handle, "doWrite.after");
            return ret;
        }

        @Override
        protected int doRead(ChunkHandle handle, long fromOffset, int length, byte[] buffer, int bufferOffset) throws ChunkStorageException {
            intercept(handle, "doRead.before");
            val ret = super.doRead(handle, fromOffset, length, buffer, bufferOffset);
            intercept(handle, "doRead.after");
            return ret;
        }
    }

    /**
     * Runs {@link SystemJournalOperationsTests} for Non-appendable storage.
     */
    public static class NonAppendableChunkStorageSystemJournalOperationsTests extends SystemJournalOperationsTests {
        @Before
        public void before() throws Exception {
            super.before();
        }

        @After
        public void after() throws Exception {
            super.after();
        }

        protected ChunkStorage getChunkStorage() throws Exception {
            val chunkStorage = new InMemoryChunkStorage(executorService());
            chunkStorage.setShouldSupportAppend(false);
            return chunkStorage;
        }
    }
}
