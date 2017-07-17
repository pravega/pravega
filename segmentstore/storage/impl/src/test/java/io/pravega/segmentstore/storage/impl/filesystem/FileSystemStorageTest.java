/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.IdempotentStorageTestBase;
import io.pravega.segmentstore.storage.impl.StroageMetricsBase;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for FileSystemStorage.
 */
public class FileSystemStorageTest extends IdempotentStorageTestBase {
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private FileSystemStorageFactory storageFactory;
    private StroageMetricsBase metrics;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        MetricsConfig metricsConfig = MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build();
        MetricsProvider.initialize(metricsConfig);
        this.adapterConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
        metrics = new FileSystemStorageMetrics();
        this.storageFactory = new FileSystemStorageFactory(adapterConfig, this.executorService(), metrics);
    }

    @After
    public void tearDown() {
        FileHelpers.deleteFileOrDirectory(baseDir);
        baseDir = null;
    }

    //region Write tests with metrics checks
    /**
     * Tests the write() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Override
    @Test(timeout = 30000)
    public void testWrite() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, TIMEOUT).join();

            long expectedMetricsSize = metrics.writeBytes.get();
            long expectedMetricsSuccesses = metrics.writeLatency.toOpStatsData().getNumSuccessfulEvents();
            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createHandle(segmentName + "_1", false, DEFAULT_EPOCH), 0,
                            new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            Assert.assertEquals("writeBytes should not change in case of unsuccessful writes",
                    expectedMetricsSize, metrics.writeBytes.get());
            Assert.assertEquals("writeLatency should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, metrics.writeLatency.toOpStatsData().getNumSuccessfulEvents());

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                expectedMetricsSize += writeData.length;
                expectedMetricsSuccesses += 1;
                Assert.assertEquals("writeLatency should increase the count of successful events in case of successful writes",
                        expectedMetricsSuccesses, metrics.writeLatency.toOpStatsData().getNumSuccessfulEvents());
                Assert.assertEquals("writeBytes should increase by the size of successful writes",
                        expectedMetricsSize, metrics.writeBytes.get());

                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);
            Assert.assertEquals("writeBytes should not change in case of unsuccessful writes",
                    expectedMetricsSize, metrics.writeBytes.get());
            Assert.assertEquals("writeLatency should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, metrics.writeLatency.toOpStatsData().getNumSuccessfulEvents());
            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
            Assert.assertEquals("writeBytes should not change in case of unsuccessful writes",
                    expectedMetricsSize, metrics.writeBytes.get());
            Assert.assertEquals("writeLatency should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, metrics.writeLatency.toOpStatsData().getNumSuccessfulEvents());
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        return this.storageFactory.createStorageAdapter();
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            return FileSystemSegmentHandle.readHandle(segmentName);
        } else {
            return FileSystemSegmentHandle.writeHandle(segmentName);
        }
    }


}
