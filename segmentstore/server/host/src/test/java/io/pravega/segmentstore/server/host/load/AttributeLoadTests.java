/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.load;

import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.shared.NameUtils;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.NoOpCache;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
    private static final String OUTPUT_DIR_NAME = System.getProperty("java.io.tmp", "/tmp") + "/pravega/attribute_load_test";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SEGMENT_ROLLING_SIZE = 16 * 1024 * 1024;
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

    @Test
    public void testInsert32K1M() {
        final int pageSize = Short.MAX_VALUE;
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

    @Test
    public void testUpdate32K1M() {
        final int pageSize = Short.MAX_VALUE;
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
            idx.update(batch, TIMEOUT).join();
            if (count - lastReport.get() >= REPORT_EVERY) {
                System.out.println(String.format("\tInserted %s.", count));
                lastReport.set(count);
            }
        });

        long insertElapsedNanos = insertTimer.getElapsedNanos();

        // Report insert status.
        val info = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        long startOffset = getStartOffset(info, context);
        long theoreticalDataSize = (long) attributeCount * (RevisionDataOutput.UUID_BYTES + Long.BYTES);
        long actualDataSize = info.getLength() - startOffset;
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
        executeInBatches(attributeCount, UPDATE_INSERT_BATCH_SIZE, false, (count, batch) -> idx.update(batch, TIMEOUT).join());
        val insertInfo = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        long startOffset = getStartOffset(insertInfo, context);
        long theoreticalDataSize = (long) attributeCount * (RevisionDataOutput.UUID_BYTES + Long.BYTES);
        long insertDataSize = insertInfo.getLength() - startOffset;
        double insertExcess = calculateExcessPercentage(insertDataSize, theoreticalDataSize);

        Timer updateTimer = new Timer();
        AtomicInteger lastReport = new AtomicInteger();
        executeInBatches(attributeCount, batchSize, true, (count, batch) -> {
            idx.update(batch, TIMEOUT).join();
            if (count - lastReport.get() >= REPORT_EVERY) {
                System.out.println(String.format("\tUpdated %s.", count));
                lastReport.set(count);
            }
        });
        long updateElapsedNanos = updateTimer.getElapsedNanos();

        // Report status.
        val updateInfo = context.storage.getStreamSegmentInfo(context.attributeSegmentName, TIMEOUT).join();
        startOffset = getStartOffset(updateInfo, context);
        theoreticalDataSize = theoreticalDataSize * 2; // Old way would have us double our size.
        long updateDataSize = updateInfo.getLength() - startOffset;
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

    private void executeInBatches(int attributeCount, int batchSize, boolean random, BiConsumer<Integer, Map<AttributeId, Long>> processBatch) {
        int count = 0;
        int batchId = 0;
        Random rnd = random ? new Random(0) : null;
        while (count < attributeCount) {
            int bs = Math.min(batchSize, attributeCount - count);
            val batch = new HashMap<AttributeId, Long>(bs);
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

    private long getStartOffset(SegmentProperties segmentInfo, TestContext context) {
        if (segmentInfo.getLength() == 0) {
            return 0;
        }

        long offset = 0;
        val handle = context.storage.openRead(segmentInfo.getName()).join();
        byte[] buffer = new byte[1];
        while (offset < segmentInfo.getLength()) {
            try {
                context.storage.read(handle, offset, buffer, 0, buffer.length, TIMEOUT).join();
                break; // If we make it here, we stumbled across a valid offset.
            } catch (Exception ex) {
                if (!(Exceptions.unwrap(ex) instanceof StreamSegmentTruncatedException)) {
                    throw ex;
                }
            }
            offset += SEGMENT_ROLLING_SIZE;
        }

        return offset - SEGMENT_ROLLING_SIZE;
    }

    private AttributeId getKey(int count) {
        return AttributeId.uuid(count, count);
    }

    private AttributeId getRandomKey(int attributeCount, Random rnd) {
        int r = rnd.nextInt(attributeCount);
        return AttributeId.uuid(r, r);
    }

    private long getValue(int batchId, int indexInBatch, int batchSize) {
        return batchId * batchSize + indexInBatch;
    }

    private double calculateExcessPercentage(long actualDataSize, long theoreticalDataSize) {
        return (double) (actualDataSize - theoreticalDataSize) / theoreticalDataSize * 100;
    }

    //endregion

    //region TestContext

    private class TestContext implements AutoCloseable {
        final CacheStorage cacheStorage;
        final Storage storage;
        final UpdateableContainerMetadata containerMetadata;
        final ContainerAttributeIndex index;
        final CacheManager cacheManager; // Not used, but required by the constructor.
        final long segmentId;
        final String attributeSegmentName;

        TestContext(String segmentName, AttributeIndexConfig config) {
            val storageConfig = FileSystemStorageConfig.builder()
                                                       .with(FileSystemStorageConfig.ROOT, OUTPUT_DIR_NAME)
                                                       .build();
            val storageFactory = new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, storageConfig, executorService());
            this.storage = storageFactory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService()));
            this.containerMetadata = new MetadataBuilder(0).build();
            this.cacheStorage = new NoOpCache();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            val factory = new ContainerAttributeIndexFactoryImpl(config, this.cacheManager, executorService());
            this.index = factory.createContainerAttributeIndex(this.containerMetadata, this.storage);

            // Setup the segment in the metadata.
            this.segmentId = 0L;
            this.attributeSegmentName = NameUtils.getAttributeSegmentName(segmentName);
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
            this.cacheManager.close();

            // We generate a lot of data, we should cleanup before exiting.
            cleanup();
            this.storage.close();
            this.cacheStorage.close();
        }
    }

    //endregion
}
