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
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Client;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.IdempotentStorageTestBase;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ExtendedS3Storage.
 */
@Slf4j
public class ExtendedS3StorageTest extends IdempotentStorageTestBase {
    private ExtendedS3TestContext setup;

    @Before
    public void setUp() throws Exception {
        this.setup = new ExtendedS3TestContext();
        MetricsConfig metricsConfig = MetricsConfig.builder().with(MetricsConfig.ENABLE_STATISTICS, true).build();
        MetricsProvider.initialize(metricsConfig);
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
    }

    @After
    public void tearDown() throws Exception {
        if (this.setup != null) {
            this.setup.close();
        }
    }

    //region If-none-match test
    /**
     * Tests the create() method with if-none-match set. Note that we currently
     * do not run a real storage tier, so we cannot verify the behavior of the
     * option against a real storage. Here instead, we are simply making sure
     * that the new execution path does not break anything.
     */
    @Test
    public void testCreateIfNoneMatch() {
        val adapterConfig = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, setup.configUri)
                .with(ExtendedS3StorageConfig.BUCKET, setup.adapterConfig.getBucket())
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .with(ExtendedS3StorageConfig.USENONEMATCH, true)
                .build();

        String segmentName = "foo_open";
        try (Storage s = createStorage(setup.client, adapterConfig, executorService())) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, null).join();
            assertFutureThrows("create() did not throw for existing StreamSegment.",
                    s.create(segmentName, null),
                    ex -> ex instanceof StreamSegmentExistsException);
        }
    }
    //endregion

    @Test
    public void testConfigForTrailingCharInPrefix() {
        // Missing trailing '/'
        ConfigBuilder<ExtendedS3StorageConfig> builder1 = ExtendedS3StorageConfig.builder();
        builder1.with(Property.named("configUri"), "http://127.0.0.1:9020?identity=x&secretKey=x")
                .with(Property.named("prefix"), "samplePrefix");
        ExtendedS3StorageConfig config1 = builder1.build();
        assertTrue(config1.getPrefix().endsWith("/"));
        assertEquals("samplePrefix/", config1.getPrefix());

        // Not missing '/'
        ConfigBuilder<ExtendedS3StorageConfig> builder2 = ExtendedS3StorageConfig.builder();
        builder2.with(Property.named("configUri"), "http://127.0.0.1:9020?identity=x&secretKey=x")
                .with(Property.named("prefix"), "samplePrefix/");
        ExtendedS3StorageConfig config2 = builder2.build();
        assertTrue(config2.getPrefix().endsWith("/"));
        assertEquals("samplePrefix/", config2.getPrefix());
    }

    @Test
    public void testMetrics() throws Exception {
        assertEquals(0, ExtendedS3Metrics.CREATE_COUNT.get());
        assertEquals(0, ExtendedS3Metrics.CONCAT_COUNT.get());
        assertEquals(0, ExtendedS3Metrics.DELETE_COUNT.get());

        try (val storage = createStorage()) {
            // Create segment A
            storage.create("a", null).join();
            assertEquals(1, ExtendedS3Metrics.CREATE_COUNT.get());
            assertEquals(1, ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 <= ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // Create segment B
            storage.create("b", null).join();
            assertEquals(2, ExtendedS3Metrics.CREATE_COUNT.get());
            assertEquals(2, ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 <= ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            SegmentHandle handleA = storage.openWrite("a").get();
            SegmentHandle handleB = storage.openWrite("b").get();

            // Write some data to A
            String str = "0123456789";
            int totalBytesWritten = 0;
            for (int i = 1; i < 5; i++) {
                try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(str.getBytes()), i)) {
                    storage.write(handleA, totalBytesWritten, bis, i, null).join();
                }
                totalBytesWritten += i;
                assertEquals(totalBytesWritten, ExtendedS3Metrics.WRITE_BYTES.get());
                assertEquals(i, ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
                assertTrue(0 <= ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getAvgLatencyMillis());
            }
            // Write some data to segment B
            storage.write(handleB, 0, new ByteArrayInputStream(str.getBytes()), str.length(), null).join();
            totalBytesWritten += str.length();
            assertEquals(totalBytesWritten, ExtendedS3Metrics.WRITE_BYTES.get());
            assertEquals(5, ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 <= ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // Read some data
            int totalBytesRead = 0;
            byte[] buffer = new byte[10];
            for (int i = 1; i < 5; i++) {
                totalBytesRead += storage.read(handleA, totalBytesRead, buffer, 0, i, null).join();
                assertEquals(totalBytesRead, ExtendedS3Metrics.READ_BYTES.get());
                assertEquals(i, ExtendedS3Metrics.READ_LATENCY.toOpStatsData().getNumSuccessfulEvents());
                assertTrue(0 <= ExtendedS3Metrics.READ_LATENCY.toOpStatsData().getAvgLatencyMillis());
            }

            // Concat
            SegmentProperties info = storage.getStreamSegmentInfo(handleA.getSegmentName(), null).join();
            storage.seal(handleB, null).join();
            storage.concat(handleA, info.getLength(), handleB.getSegmentName(), null).get();
            assertEquals(str.length(), ExtendedS3Metrics.CONCAT_BYTES.get());
            assertEquals(1, ExtendedS3Metrics.CONCAT_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 <= ExtendedS3Metrics.CONCAT_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // delete
            storage.delete(handleA, null).join();
            assertEquals(1, ExtendedS3Metrics.DELETE_COUNT.get());
            assertEquals(1, ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 <= ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getAvgLatencyMillis());
        }
    }

    /**
     * Tests fix for https://github.com/pravega/pravega/issues/4591.
     * @throws Exception Exception if any.
     */
    @Test
    public void testExistsWithPrefix() throws Exception {
        val adapterConfig = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, setup.configUri)
                .with(ExtendedS3StorageConfig.BUCKET, setup.adapterConfig.getBucket())
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .with(ExtendedS3StorageConfig.USENONEMATCH, true)
                .build();

        String segmentName1 = "issue4591";
        String segmentName2 = "normal";
        try (Storage s = createStorage(setup.client, adapterConfig, executorService())) {
            s.initialize(DEFAULT_EPOCH);

            // No segment should exist
            assertFalse(s.exists(segmentName1, null).get());
            assertFalse(s.exists(segmentName1 + "$index", null).get());
            assertFalse(s.exists(segmentName2, null).get());

            // Create and verify
            s.create(segmentName2, null).join();
            assertTrue(s.exists(segmentName2, null).get());

            s.create(segmentName1 + "$index", null).join();
            assertTrue(s.exists(segmentName1 + "$index", null).get());

            // Verify with prefix
            assertFalse(s.exists(segmentName1, null).get());
            assertFalse(s.exists(segmentName1 + "$header", null).get());
        }
    }

    /**
     * Tests the concat() method forcing to use multipart upload.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testConcatWithMultipartUpload() throws Exception {
        val adapterConfig = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, setup.configUri)
                .with(ExtendedS3StorageConfig.BUCKET, setup.adapterConfig.getBucket())
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .with(ExtendedS3StorageConfig.USENONEMATCH, true)
                .with(ExtendedS3StorageConfig.SMALL_OBJECT_THRESHOLD, 1)
                .build();
        final String context = createSegmentName("Concat");
        assertEquals(0, ExtendedS3Metrics.LARGE_CONCAT_COUNT.get());
        try (Storage s = createStorage(setup.client, adapterConfig, executorService())) {
            testConcat(context, s);
            assertTrue(ExtendedS3Metrics.LARGE_CONCAT_COUNT.get() > 0);
            assertEquals(ExtendedS3Metrics.CONCAT_COUNT.get(), ExtendedS3Metrics.LARGE_CONCAT_COUNT.get());
        }
    }

    /**
     * Tests the next batch of segments in ExtendedS3Storage.
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testListSegmentsBatch() throws Exception {
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            Iterator<SegmentProperties> iterator = s.listSegments();
            Assert.assertFalse(iterator.hasNext());
            int expectedCount = 1001; // Create more segments than 1000 which is the maximum number of segments in one batch.
            for (int i = 0; i < expectedCount; i++) {
                String segmentName = "segment-" + i;
                createSegment(segmentName, s);
            }
            iterator = s.listSegments();
            int actualCount = 0;
            while (iterator.hasNext()) {
                iterator.next();
                ++actualCount;
            }
            Assert.assertEquals(actualCount, expectedCount);
            Assert.assertFalse(iterator.hasNext());
        }
    }

    private static Storage createStorage(S3Client client, ExtendedS3StorageConfig adapterConfig, Executor executor) {
        // We can't use the factory here because we're setting our own (mock) client.
        ExtendedS3Storage storage = new ExtendedS3Storage(client, adapterConfig, false);
        return new AsyncStorageWrapper(storage, executor);
    }

    @Override
    protected Storage createStorage() {
        return createStorage(setup.client, setup.adapterConfig, executorService());
    }

    //region RollingStorageTests

    /**
     * Tests the InMemoryStorage adapter with a RollingStorage wrapper.
     */
    public static class RollingStorageTests extends RollingStorageTestBase {
        private ExtendedS3TestContext setup;

        @Before
        public void setUp() throws Exception {
            this.setup = new ExtendedS3TestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.setup != null) {
                this.setup.close();
            }
        }

        @Override
        protected Storage createStorage() {
            ExtendedS3Storage storage = new ExtendedS3Storage(setup.client, setup.adapterConfig, false);
            return wrap(storage);
        }
    }

    //endregion
}
