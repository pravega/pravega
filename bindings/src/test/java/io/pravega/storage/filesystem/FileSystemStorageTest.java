/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.storage.IdempotentStorageTestBase;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.test.common.AssertExtensions.assertSuppliedFutureThrows;

/**
 * Unit tests for FileSystemStorage.
 */
public class FileSystemStorageTest extends IdempotentStorageTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        MetricsConfig metricsConfig = MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build();
        MetricsProvider.initialize(metricsConfig);
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();
        this.adapterConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
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

            long expectedMetricsSize = FileSystemMetrics.WRITE_BYTES.get();
            long expectedMetricsSuccesses = FileSystemMetrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents();
            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertSuppliedFutureThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertSuppliedFutureThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createInexistentSegmentHandle(s, false), 0,
                            new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            Assert.assertEquals("WRITE_BYTES should not change in case of unsuccessful writes",
                    expectedMetricsSize, FileSystemMetrics.WRITE_BYTES.get());
            Assert.assertEquals("WRITE_LATENCY should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, FileSystemMetrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                expectedMetricsSize += writeData.length;
                expectedMetricsSuccesses += 1;
                Assert.assertEquals("WRITE_LATENCY should increase the count of successful events in case of successful writes",
                        expectedMetricsSuccesses, FileSystemMetrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
                Assert.assertEquals("WRITE_BYTES should increase by the size of successful writes",
                        expectedMetricsSize, FileSystemMetrics.WRITE_BYTES.get());

                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertSuppliedFutureThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);
            Assert.assertEquals("WRITE_BYTES should not change in case of unsuccessful writes",
                    expectedMetricsSize, FileSystemMetrics.WRITE_BYTES.get());
            Assert.assertEquals("WRITE_LATENCY should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, FileSystemMetrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertSuppliedFutureThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
            Assert.assertEquals("WRITE_BYTES should not change in case of unsuccessful writes",
                    expectedMetricsSize, FileSystemMetrics.WRITE_BYTES.get());
            Assert.assertEquals("WRITE_LATENCY should not increase the count of successful events in case of unsuccessful writes",
                    expectedMetricsSuccesses, FileSystemMetrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new FileSystemStorage(this.adapterConfig), executorService());
    }

    //region RollingStorageTests

    /**
     * Tests the FileSystemStorage adapter with a RollingStorage wrapper.
     */
    public static class RollingStorageTests extends RollingStorageTestBase {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private File baseDir = null;
        private FileSystemStorageConfig adapterConfig;

        @Before
        public void setUp() throws Exception {
            this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
            this.adapterConfig = FileSystemStorageConfig
                    .builder()
                    .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                    .build();
        }

        @After
        public void tearDown() {
            FileHelpers.deleteFileOrDirectory(baseDir);
        }

        @Override
        protected Storage createStorage() {
            return wrap(new FileSystemStorage(this.adapterConfig));
        }
    }

    //endregion
}
