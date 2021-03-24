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
package io.pravega.storage.filesystem;

import io.pravega.common.function.RunnableWithException;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.IdempotentStorageTestBase;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.util.Random;
import lombok.Cleanup;
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
        statsProvider.startWithoutExporting();
        this.adapterConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
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

    /**
     * Tests the {@link FileSystemStorage#replace} without having to perform recovery.
     */
    @Test
    public void testReplaceNormalCase() throws Exception {
        val segmentName = "BaseSegment";
        val tempSegmentName = getTempSegmentName(segmentName);
        val segmentHandle = FileSystemSegmentHandle.writeHandle(segmentName);
        val data1 = new ByteArraySegment(new byte[]{1, 2});
        val data2 = new ByteArraySegment(new byte[]{3, 4, 5});

        // We use "base" for direct access to files.
        @Cleanup
        val base = new FileSystemStorage(this.adapterConfig);
        Assert.assertFalse(base.supportsReplace());
        AssertExtensions.assertThrows("", () -> base.replace(segmentHandle, data1), ex -> ex instanceof UnsupportedOperationException);

        // Create a FileSystemStorageWithReplace, which is what we are testing.
        @Cleanup
        val s = (FileSystemStorage.FileSystemStorageWithReplace) base.withReplaceSupport();
        Assert.assertTrue(s.supportsReplace());
        Assert.assertSame(s, s.withReplaceSupport());
        Assert.assertFalse(base.exists(segmentName));
        Assert.assertFalse(base.exists(tempSegmentName));

        // Part 1. Segment Does not exist and has never been created.
        Assert.assertFalse(s.exists(segmentName));
        AssertExtensions.assertThrows("", () -> s.openRead(segmentName), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.openWrite(segmentName), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.getStreamSegmentInfo(segmentName), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.write(segmentHandle, 0, data1.getReader(), data1.getLength()), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.read(segmentHandle, 0, new byte[1], 0, 1), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.delete(segmentHandle), ex -> ex instanceof StreamSegmentNotExistsException);
        AssertExtensions.assertThrows("", () -> s.replace(segmentHandle, data1), ex -> ex instanceof StreamSegmentNotExistsException);

        // Not expecting any file to have actually been created.
        Assert.assertFalse(base.exists(segmentName));
        Assert.assertFalse(base.exists(tempSegmentName));

        // Part 2. Segment Exists, but not the temp file.
        s.create(segmentName);
        Assert.assertTrue(base.exists(segmentName));
        Assert.assertFalse(base.exists(tempSegmentName));
        Assert.assertTrue(s.exists(segmentName));
        val wh1 = s.openWrite(segmentName);
        s.write(wh1, 0, data1.getReader(), data1.getLength());
        val rh1 = s.openRead(segmentName);
        val rb1 = new byte[data1.getLength()];
        s.read(rh1, 0, rb1, 0, rb1.length);
        Assert.assertArrayEquals(data1.array(), rb1);

        // Now perform a replace and verify that we still have the desired outcomes.
        s.replace(wh1, data2);
        val si1 = s.getStreamSegmentInfo(segmentName);
        Assert.assertEquals(data2.getLength(), si1.getLength());
        Assert.assertFalse(si1.isSealed());
        Assert.assertFalse(base.exists(tempSegmentName));

        s.write(wh1, si1.getLength(), data1.getReader(), data1.getLength());
        s.seal(wh1);
        val si2 = s.getStreamSegmentInfo(segmentName);
        Assert.assertEquals(data2.getLength() + data1.getLength(), si2.getLength());
        Assert.assertTrue(si2.isSealed());

        s.replace(wh1, data1);
        val si3 = s.getStreamSegmentInfo(segmentName);
        Assert.assertTrue(si3.isSealed());
        s.delete(wh1);
    }

    /**
     * Tests the behavior of {@link FileSystemStorage} when a previous invocation of {@link FileSystemStorage#replace}
     * has only partially completed (created a Temp file and deleted the original file).
     */
    @Test
    public void testReplaceRecoveryWithTempFileOnly() throws Exception {
        val segmentName = "BaseSegment";
        val tempSegmentName = getTempSegmentName(segmentName);
        val segmentHandle = FileSystemSegmentHandle.writeHandle(segmentName);
        val data1 = new ByteArraySegment(new byte[]{1, 2});
        val data2 = new ByteArraySegment(new byte[]{3, 4, 5});

        // We use "base" for direct access to files.
        @Cleanup
        val base = new FileSystemStorage(this.adapterConfig);
        Assert.assertFalse(base.supportsReplace());
        AssertExtensions.assertThrows("", () -> base.replace(segmentHandle, data1), ex -> ex instanceof UnsupportedOperationException);

        // Create a FileSystemStorageWithReplace, which is what we are testing.
        @Cleanup
        val s = (FileSystemStorage.FileSystemStorageWithReplace) base.withReplaceSupport();

        // Exists (does not perform auto-recovery).
        withPartialReplace(segmentName, null, data2, base, () -> {
            Assert.assertTrue(s.exists(segmentName));
            Assert.assertFalse(base.exists(segmentName));
            Assert.assertTrue(base.exists(tempSegmentName));
        });

        // GetStreamSegmentInfo (performs auto-recovery).
        withPartialReplace(segmentName, null, data2, base, () -> {
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertEquals(data2.getLength(), si.getLength());
            Assert.assertEquals(data2.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // OpenRead (performs auto-recovery) + Read.
        withPartialReplace(segmentName, null, data2, base, () -> {
            val rh = s.openRead(segmentName);
            val rb = new byte[data2.getLength()];
            s.read(rh, 0, rb, 0, rb.length);
            Assert.assertArrayEquals(data2.array(), rb);
            Assert.assertEquals(data2.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // OpenWrite (performs auto-recovery) + Write + Seal.
        withPartialReplace(segmentName, null, data2, base, () -> {
            val wh = s.openWrite(segmentName);
            s.write(wh, data2.getLength(), data1.getReader(), data1.getLength());
            s.seal(wh);
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertTrue(si.isSealed());
            Assert.assertEquals(data2.getLength() + data1.getLength(), si.getLength());
            Assert.assertEquals(data2.getLength() + data1.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // Concat (performs auto-recovery on source segment).
        withPartialReplace(segmentName, null, data2, base, () -> {
            base.seal(base.openWrite(getTempSegmentName(segmentName)));
            val targetSegment = s.create("Target");
            try {
                s.concat(targetSegment, 0, segmentName);

                val si = s.getStreamSegmentInfo(targetSegment.getSegmentName());
                Assert.assertFalse(si.isSealed());
                Assert.assertEquals(data2.getLength(), si.getLength());
                Assert.assertFalse(base.exists(segmentName));
                Assert.assertFalse(base.exists(tempSegmentName));
            } finally {
                s.delete(targetSegment);
            }
        });

        // Replace (with fake handle) - perform auto-recovery and subsequent replace.
        withPartialReplace(segmentName, null, data2, base, () -> {
            s.replace(segmentHandle, data1);
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertEquals(data1.getLength(), si.getLength());
            Assert.assertEquals(data1.getLength(), base.getStreamSegmentInfo(segmentName).getLength());

            val rb = new byte[data1.getLength()];
            s.read(segmentHandle, 0, rb, 0, rb.length);
            Assert.assertArrayEquals(data1.array(), rb);

            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // Delete (should clean up everything).
        withPartialReplace(segmentName, null, data2, base, () -> {
            s.delete(segmentHandle);
            Assert.assertFalse(s.exists(segmentName));
            Assert.assertFalse(base.exists(segmentName));
            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // Create (performs auto-recovery).
        withPartialReplace(segmentName, null, data2, base, () -> {
            AssertExtensions.assertThrows("", () -> s.create(segmentName), ex -> ex instanceof StreamSegmentExistsException);
            Assert.assertTrue(s.exists(segmentName));
            Assert.assertTrue(base.exists(segmentName));
            Assert.assertFalse(base.exists(tempSegmentName));
        });
    }

    /**
     * Tests the behavior of {@link FileSystemStorage} when a previous invocation of {@link FileSystemStorage#replace}
     * has only partially completed (created a Temp file but did not delete the original file).
     *
     * These methods do not perform auto-recovery.
     */
    @Test
    public void testReplaceRecoveryBothSegmentAndTempFile() throws Exception {
        val segmentName = "BaseSegment";
        val tempSegmentName = getTempSegmentName(segmentName);
        val segmentHandle = FileSystemSegmentHandle.writeHandle(segmentName);
        val data1 = new ByteArraySegment(new byte[]{1, 2});
        val data2 = new ByteArraySegment(new byte[]{3, 4, 5});
        val data3 = new ByteArraySegment(new byte[]{6});

        // We use "base" for direct access to files.
        @Cleanup
        val base = new FileSystemStorage(this.adapterConfig);
        Assert.assertFalse(base.supportsReplace());
        AssertExtensions.assertThrows("", () -> base.replace(segmentHandle, data1), ex -> ex instanceof UnsupportedOperationException);

        // Create a FileSystemStorageWithReplace, which is what we are testing.
        @Cleanup
        val s = (FileSystemStorage.FileSystemStorageWithReplace) base.withReplaceSupport();

        // Exists.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            Assert.assertTrue(s.exists(segmentName));
            Assert.assertTrue(base.exists(segmentName));
            Assert.assertTrue(base.exists(tempSegmentName));
        });

        // GetStreamSegmentInfo.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertEquals(data1.getLength(), si.getLength());
            Assert.assertEquals(data1.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertTrue(base.exists(tempSegmentName));
        });

        // OpenRead  + Read.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            val rh = s.openRead(segmentName);
            val rb = new byte[data1.getLength()];
            s.read(rh, 0, rb, 0, rb.length);
            Assert.assertArrayEquals(data1.array(), rb);
            Assert.assertEquals(data1.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertTrue(base.exists(tempSegmentName));
        });

        // OpenWrite + Write + Seal.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            val wh = s.openWrite(segmentName);
            s.write(wh, data1.getLength(), data1.getReader(), data1.getLength());
            s.seal(wh);
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertTrue(si.isSealed());
            Assert.assertEquals(data1.getLength() + data1.getLength(), si.getLength());
            Assert.assertEquals(data1.getLength() + data1.getLength(), base.getStreamSegmentInfo(segmentName).getLength());
            Assert.assertTrue(base.exists(tempSegmentName));
        });

        // Concat.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            base.seal(base.openWrite(segmentName));
            val targetSegment = s.create("Target");
            try {
                s.concat(targetSegment, 0, segmentName);

                val si = s.getStreamSegmentInfo(targetSegment.getSegmentName());
                Assert.assertFalse(si.isSealed());
                Assert.assertEquals(data1.getLength(), si.getLength());
                Assert.assertFalse(base.exists(segmentName));
                Assert.assertTrue(base.exists(tempSegmentName));
            } finally {
                s.delete(targetSegment);
            }
        });

        // Replace (with fake handle) - perform auto-recovery and subsequent replace.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            s.replace(segmentHandle, data3);
            val si = s.getStreamSegmentInfo(segmentName);
            Assert.assertEquals(data3.getLength(), si.getLength());
            Assert.assertEquals(data3.getLength(), base.getStreamSegmentInfo(segmentName).getLength());

            val rb = new byte[data3.getLength()];
            s.read(segmentHandle, 0, rb, 0, rb.length);
            Assert.assertArrayEquals(data3.array(), rb);

            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // Delete (should clean up everything).
        withPartialReplace(segmentName, data1, data2, base, () -> {
            s.delete(segmentHandle);
            Assert.assertFalse(s.exists(segmentName));
            Assert.assertFalse(base.exists(segmentName));
            Assert.assertFalse(base.exists(tempSegmentName));
        });

        // Create.
        withPartialReplace(segmentName, data1, data2, base, () -> {
            AssertExtensions.assertThrows("", () -> s.create(segmentName), ex -> ex instanceof StreamSegmentExistsException);
            Assert.assertTrue(s.exists(segmentName));
            Assert.assertTrue(base.exists(segmentName));
            Assert.assertTrue(base.exists(tempSegmentName));
        });
    }

    private void withPartialReplace(String segmentName, BufferView segmentContents, BufferView tempSegmentContents,
                                    FileSystemStorage baseStorage, RunnableWithException toRun) throws Exception {
        if (segmentContents != null) {
            val sh = baseStorage.create(segmentName);
            baseStorage.write(sh, 0, segmentContents.getReader(), segmentContents.getLength());
        }

        String tempSegmentName = getTempSegmentName(segmentName);
        val tsh = baseStorage.create(tempSegmentName);
        baseStorage.write(tsh, 0, tempSegmentContents.getReader(), tempSegmentContents.getLength());
        try {
            toRun.run();
        } finally {
            // Cleanup
            if (baseStorage.exists(segmentName)) {
                baseStorage.delete(baseStorage.openWrite(segmentName));
            }

            if (baseStorage.exists(tempSegmentName)) {
                baseStorage.delete(baseStorage.openWrite(tempSegmentName));
            }
        }
    }

    private String getTempSegmentName(String segmentName) {
        return segmentName + FileSystemStorage.FileSystemStorageWithReplace.TEMP_SUFFIX;
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
                    .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
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

        @Test
        public void testTruncate() {
            val segmentName = "TruncatedSegment";
            val rollingPolicy = new SegmentRollingPolicy(100);
            val writeCount = 50;
            val rnd = new Random(0);

            // Write small and large writes, alternatively.
            @Cleanup
            val s = createStorage();
            s.initialize(1);
            val writeHandle = s.create(segmentName, rollingPolicy, TIMEOUT).join();
            val readHandle = s.openRead(segmentName).join(); // Open now, before writing, so we force a refresh.
            val writeStream = new ByteBufferOutputStream();
            populate(writeHandle, writeCount, writeStream, s, rollingPolicy);

            // Test that truncate works in this scenario.
            int truncateOffset = 0;
            ArrayView writtenData = writeStream.getData();
            while (true) {
                s.truncate(writeHandle, truncateOffset, TIMEOUT).join();

                // Verify we can still read properly.
                checkWrittenData(writeHandle, writtenData, truncateOffset, s);
                checkWrittenData(readHandle, writtenData, truncateOffset, s);

                // Increment truncateOffset by some value, but let's make sure we also truncate at the very end of the Segment.
                if (truncateOffset >= writtenData.getLength()) {
                    break;
                }

                truncateOffset = (int) Math.min(writtenData.getLength(), truncateOffset + rollingPolicy.getMaxLength() / 2);
            }

            // Make sure we can still write after truncation.
            populate(writeHandle, writeCount, writeStream, s, rollingPolicy);
            writtenData = writeStream.getData();
            checkWrittenData(writeHandle, writtenData, truncateOffset, s);
            checkWrittenData(readHandle, writtenData, truncateOffset, s);
        }

        private void populate(SegmentHandle handle, int writeCount, ByteBufferOutputStream writeStream,
                              Storage s, SegmentRollingPolicy rollingPolicy) {
            int offset = (int) s.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join().getLength();
            for (int i = 0; i < writeCount; i++) {
                byte[] appendData = new byte[i % 2 == 0 ? (int) (rollingPolicy.getMaxLength() * 0.24) : (int) (rollingPolicy.getMaxLength() * 1.8)];
                rnd.nextBytes(appendData);
                s.write(handle, offset, new ByteArrayInputStream(appendData), appendData.length, TIMEOUT).join();
                offset += appendData.length;
                writeStream.write(appendData);
            }
        }

        private void checkWrittenData(SegmentHandle handle, ArrayView writtenData, int truncateOffset, Storage s) {
            // Verify we can still read properly.
            byte[] readBuffer = new byte[writtenData.getLength() - truncateOffset];
            if (readBuffer.length != 0) {
                // Nothing to check.
                int bytesRead = s.read(handle, truncateOffset, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
                AssertExtensions.assertArrayEquals("Unexpected data read back.",
                        writtenData.array(), writtenData.arrayOffset() + truncateOffset, readBuffer, 0, readBuffer.length);
            }
        }

        //endregion
    }
}
