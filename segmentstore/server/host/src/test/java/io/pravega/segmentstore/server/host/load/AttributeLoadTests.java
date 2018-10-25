/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.load;

import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import lombok.Cleanup;
import lombok.val;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Load tests for Extended Attributes. Verify various scenarios and output Index Sizes and Insert/Gets times for each one.
 * This is marked as @Ignore since these are not real unit tests (no correctness checking), they take a long time to execute
 * and make heavy use of local disks.
 */
@Ignore
public class AttributeLoadTests extends ThreadPooledTestSuite {
    private static final String OUTPUT_DIR_NAME = "/tmp/pravega/attribute_load_test";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SEGMENT_ROLLING_SIZE = 4 * 1024 * 1024;
    private static final int REPORT_EVERY = 500 * 1000;
    private static final int UPDATE_INSERT_BATCH_SIZE = 1000 * 1000;
    private static final int[] BATCH_SIZES = new int[]{10, 100, 1000, 1000 * 1000};
    private static final Predicate<Integer> TEST_GET = batchSize -> batchSize <= 1000;

    @AfterClass
    public static void tearDownSuite() {
        // Clean up any leftovers.
        FileHelpers.deleteFileOrDirectory(new File(OUTPUT_DIR_NAME));
    }

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    //region Insert (and Get)

    @Test
    public void testInsert4K1M() {
        final int pageSize = 4 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testInsert(pageSize, count, batchSize, TEST_GET.test(batchSize));
        }
    }

    @Test
    public void testInsert4K10M() {
        final int pageSize = 4 * 1024;
        final int count = 10 * 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testInsert(pageSize, count, batchSize, TEST_GET.test(batchSize));
        }
    }

    @Test
    public void testInsert8K1M() {
        final int pageSize = 8 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testInsert(pageSize, count, batchSize, TEST_GET.test(batchSize));
        }
    }

    @Test
    public void testInsert16K1M() {
        final int pageSize = 16 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testInsert(pageSize, count, batchSize, TEST_GET.test(batchSize));
        }
    }

    //endregion

    //region Update

    @Test
    public void testUpdate4K1M() {
        final int pageSize = 4 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testUpdate(pageSize, count, batchSize);
        }
    }

    @Test
    public void testUpdate8K1M() {
        final int pageSize = 8 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testUpdate(pageSize, count, batchSize);
        }
    }

    @Test
    public void testUpdate16K1M() {
        final int pageSize = 16 * 1024;
        final int count = 1000 * 1000;
        for (int batchSize : BATCH_SIZES) {
            testUpdate(pageSize, count, batchSize);
        }
    }

    //endregion

    //region Helpers

    private void testInsert(int pageSize, int attributeCount, int batchSize, boolean testGet) {
        System.out.println(String.format("Started: PageSize = %s, Count = %s, Batch = %s ...", pageSize, attributeCount, batchSize));

        String segmentName = String.format("page_%s_count_%s_batch_%s_insert/segment", pageSize, attributeCount, batchSize);
        val attributeConfig = AttributeIndexConfig.builder()
                                                  .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, pageSize)
                                                  .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, SEGMENT_ROLLING_SIZE)
                                                  .build();
        @Cleanup
        val context = new TestContext(segmentName, attributeConfig);
        val idx = context.index.forSegment(context.segmentId, TIMEOUT).join();

        // Insert in batches.
        Timer insertTimer = new Timer();
        AtomicInteger lastReport = new AtomicInteger();
        executeInBatches(attributeCount, batchSize, false, (count, batch) -> {
            idx.put(batch, TIMEOUT).join();
            if (count - lastReport.get() >= REPORT_EVERY) {
                System.out.println(String.format("\tInserted %s.", count));
                lastReport.set(count);
            }
        });

        long insertElapsedNanos = insertTimer.getElapsedNanos();

        // Report insert status.
        val info = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        long theoreticalDataSize = (long) attributeCount * (RevisionDataOutput.UUID_BYTES + Long.BYTES);
        long actualDataSize = info.getLength();
        double excess = calculateExcessPercentage(actualDataSize, theoreticalDataSize);
        long insertElapsedSeconds = insertElapsedNanos / 1000 / 1000 / 1000;
        double insertPerBatchMillis = insertElapsedNanos / 1000.0 / 1000 / ((double) attributeCount / batchSize);
        System.out.println(String.format("(INSERT) PageSize = %s, Count = %s, Batch = %s, IndexSize = %.1fMB/%.1fMB, Excess = %.1f%%, Time = %ds/%.2fms",
                pageSize,
                attributeCount,
                batchSize,
                (double) actualDataSize / 1024 / 1024,
                (double) theoreticalDataSize / 1024 / 1024,
                excess,
                insertElapsedSeconds,
                insertPerBatchMillis));

        if (testGet) {
            testGet(attributeCount, batchSize, pageSize, idx);
        }
    }

    private void testUpdate(int pageSize, int attributeCount, int batchSize) {
        System.out.println(String.format("Started (UPDATE): PageSize = %s, Count = %s, Batch = %s ...", pageSize, attributeCount, batchSize));

        String segmentName = String.format("page_%s_count_%s_batch_%s_update/segment", pageSize, attributeCount, batchSize);
        val attributeConfig = AttributeIndexConfig.builder()
                                                  .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, pageSize)
                                                  .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, SEGMENT_ROLLING_SIZE)
                                                  .build();
        @Cleanup
        val context = new TestContext(segmentName, attributeConfig);
        val idx = context.index.forSegment(context.segmentId, TIMEOUT).join();

        // Bulk upload.
        executeInBatches(attributeCount, UPDATE_INSERT_BATCH_SIZE, false, (count, batch) -> idx.put(batch, TIMEOUT).join());
        val insertInfo = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        long theoreticalDataSize = (long) attributeCount * (RevisionDataOutput.UUID_BYTES + Long.BYTES);
        long insertDataSize = insertInfo.getLength();
        double insertExcess = calculateExcessPercentage(insertDataSize, theoreticalDataSize);

        Timer updateTimer = new Timer();
        AtomicInteger lastReport = new AtomicInteger();
        executeInBatches(attributeCount, batchSize, true, (count, batch) -> {
            idx.put(batch, TIMEOUT).join();
            if (count - lastReport.get() >= REPORT_EVERY) {
                System.out.println(String.format("\tUpdated %s.", count));
                lastReport.set(count);
            }
        });
        long updateElapsedNanos = updateTimer.getElapsedNanos();

        // Report status.
        val updateInfo = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        theoreticalDataSize = theoreticalDataSize * 2; // Old way would have us double our size.
        long updateDataSize = updateInfo.getLength();
        double updateExcess = calculateExcessPercentage(updateDataSize, theoreticalDataSize) - insertExcess;
        long updateElapsedSeconds = updateElapsedNanos / 1000 / 1000 / 1000;
        double updatePerBatchMillis = updateElapsedNanos / 1000.0 / 1000 / ((double) attributeCount / batchSize);
        System.out.println(String.format("(UPDATE) PageSize = %s, Count = %s, Batch = %s, IndexSize = %.1fMB/%.1fMB, Excess = %.1f%%, Time = %ds/%.2fms",
                pageSize,
                attributeCount,
                batchSize,
                (double) updateDataSize / 1024 / 1024,
                (double) theoreticalDataSize / 1024 / 1024,
                updateExcess,
                updateElapsedSeconds,
                updatePerBatchMillis));
    }

    private void testGet(int attributeCount, int batchSize, int pageSize, AttributeIndex idx) {
        // Get in batches.
        Timer getTimer = new Timer();
        executeInBatches(attributeCount, batchSize, false, (count, batch) -> idx.get(batch.keySet(), TIMEOUT).join());

        // Report get status.
        long getElapsedNanos = getTimer.getElapsedNanos();
        long getElapsedSeconds = getElapsedNanos / 1000 / 1000 / 1000;
        double getPerBatchMillis = getElapsedNanos / 1000.0 / 1000 / ((double) attributeCount / batchSize);
        System.out.println(String.format("(GET) PageSize = %s, Count = %s, Batch = %s, Time = %ds/%.2fms",
                pageSize,
                attributeCount,
                batchSize,
                getElapsedSeconds,
                getPerBatchMillis));
    }

    private void executeInBatches(int attributeCount, int batchSize, boolean random, BiConsumer<Integer, Map<UUID, Long>> processBatch) {
        int count = 0;
        int batchId = 0;
        Random rnd = random ? new Random(0) : null;
        while (count < attributeCount) {
            int bs = Math.min(batchSize, attributeCount - count);
            val batch = new HashMap<UUID, Long>(bs);
            for (int indexInBatch = 0; indexInBatch < bs; indexInBatch++) {
                val key = random ? getRandomKey(attributeCount, rnd) : getKey(count + indexInBatch);
                val value = getValue(batchId, indexInBatch, batchSize);
                batch.put(key, value);
            }

            processBatch.accept(count, batch);
            count += bs;
            batchId++;
        }
    }

    private UUID getKey(int count) {
        return new UUID(count, count);
    }

    private UUID getRandomKey(int attributeCount, Random rnd) {
        int r = rnd.nextInt(attributeCount);
        return new UUID(r, r);
    }

    private long getValue(int batchId, int indexInBatch, int batchSize) {
        return (long) (batchId * batchSize + indexInBatch);
    }

    private double calculateExcessPercentage(long actualDataSize, long theoreticalDataSize) {
        return (double) (actualDataSize - theoreticalDataSize) / theoreticalDataSize * 100;
    }

    //endregion

    //region TestContext

    private class TestContext implements AutoCloseable {
        final Storage storage;
        final UpdateableContainerMetadata containerMetadata;
        final ContainerAttributeIndex index;
        final CacheFactory cacheFactory;
        final CacheManager cacheManager; // Not used, but required by the constructor.
        final long segmentId;
        final String attributeSegmentName;

        TestContext(String segmentName, AttributeIndexConfig config) {
            val storageConfig = FileSystemStorageConfig.builder()
                                                       .with(FileSystemStorageConfig.ROOT, OUTPUT_DIR_NAME)
                                                       .build();
            val storageFactory = new FileSystemStorageFactory(storageConfig, executorService());
            this.storage = storageFactory.createStorageAdapter();
            this.containerMetadata = new MetadataBuilder(0).build();
            this.cacheFactory = new NoOpCacheFactory();
            //this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            val factory = new ContainerAttributeIndexFactoryImpl(config, this.cacheFactory, this.cacheManager, executorService());
            this.index = factory.createContainerAttributeIndex(this.containerMetadata, this.storage);

            // Setup the segment in the metadata.
            this.segmentId = 0L;
            this.attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(segmentName);
            this.containerMetadata.mapStreamSegmentId(segmentName, this.segmentId);

            // Cleanup any existing data.
            cleanup();
        }

        private void cleanup() {
            Futures.exceptionallyExpecting(
                    this.storage.openWrite(containerMetadata.getStreamSegmentMetadata(segmentId).getName())
                                .thenCompose(handle -> this.storage.delete(handle, TIMEOUT)),
                    ex -> ex instanceof StreamSegmentNotExistsException,
                    null).join();
            Futures.exceptionallyExpecting(
                    this.storage.openWrite(attributeSegmentName)
                                .thenCompose(handle -> this.storage.delete(handle, TIMEOUT)),
                    ex -> ex instanceof StreamSegmentNotExistsException,
                    null).join();
        }

        @Override
        public void close() {
            this.index.close();
            this.cacheFactory.close();
            this.cacheManager.close();

            // We generate a lot of data, we should cleanup before exiting.
            cleanup();
            this.storage.close();
        }
    }

    //endregion

    //region NoOpCache

    private static class NoOpCacheFactory implements CacheFactory {

        @Override
        public Cache getCache(String id) {
            return new NoOpCache();
        }

        @Override
        public void close() {
            // This method intentionally left blank.
        }
    }

    private static class NoOpCache implements Cache {

        @Override
        public String getId() {
            return "NoOp";
        }

        @Override
        public void insert(Key key, byte[] data) {
            // This method intentionally left blank.
        }

        @Override
        public void insert(Key key, ByteArraySegment data) {
            // This method intentionally left blank.
        }

        @Override
        public byte[] get(Key key) {
            return null;
        }

        @Override
        public void remove(Key key) {
            // This method intentionally left blank.
        }

        @Override
        public void close() {
            // This method intentionally left blank.
        }
    }

    //endregion
}
