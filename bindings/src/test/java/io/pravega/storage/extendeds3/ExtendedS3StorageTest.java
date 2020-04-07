/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.bean.ObjectKey;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.util.ConfigUri;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.storage.IdempotentStorageTestBase;
import io.pravega.test.common.TestUtils;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
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
    private TestContext setup;

    @Before
    public void setUp() throws Exception {
        this.setup = new TestContext();
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
            assertTrue(0 < ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // Create segment B
            storage.create("b", null).join();
            assertEquals(2, ExtendedS3Metrics.CREATE_COUNT.get());
            assertEquals(2, ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 < ExtendedS3Metrics.CREATE_LATENCY.toOpStatsData().getAvgLatencyMillis());

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
                assertTrue(0 < ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getAvgLatencyMillis());
            }
            // Write some data to segment B
            storage.write(handleB, 0, new ByteArrayInputStream(str.getBytes()), str.length(), null).join();
            totalBytesWritten += str.length();
            assertEquals(totalBytesWritten, ExtendedS3Metrics.WRITE_BYTES.get());
            assertEquals(5, ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 < ExtendedS3Metrics.WRITE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // Read some data
            int totalBytesRead = 0;
            byte[] buffer = new byte[10];
            for (int i = 1; i < 5; i++) {
                totalBytesRead += storage.read(handleA, totalBytesRead, buffer, 0, i, null).join();
                assertEquals(totalBytesRead, ExtendedS3Metrics.READ_BYTES.get());
                assertEquals(i, ExtendedS3Metrics.READ_LATENCY.toOpStatsData().getNumSuccessfulEvents());
                assertTrue(0 < ExtendedS3Metrics.READ_LATENCY.toOpStatsData().getAvgLatencyMillis());
            }

            // Concat
            SegmentProperties info = storage.getStreamSegmentInfo(handleA.getSegmentName(), null).join();
            storage.seal(handleB, null).join();
            storage.concat(handleA, info.getLength(), handleB.getSegmentName(), null).get();
            assertEquals(str.length(), ExtendedS3Metrics.CONCAT_BYTES.get());
            assertEquals(1, ExtendedS3Metrics.CONCAT_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 < ExtendedS3Metrics.CONCAT_LATENCY.toOpStatsData().getAvgLatencyMillis());

            // delete
            storage.delete(handleA, null).join();
            assertEquals(1, ExtendedS3Metrics.DELETE_COUNT.get());
            assertEquals(1, ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 < ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getAvgLatencyMillis());

            storage.delete(handleB, null).join();
            assertEquals(2, ExtendedS3Metrics.DELETE_COUNT.get());
            assertEquals(2, ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getNumSuccessfulEvents());
            assertTrue(0 < ExtendedS3Metrics.DELETE_LATENCY.toOpStatsData().getAvgLatencyMillis());

        } finally {

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

    private static Storage createStorage(S3Client client, ExtendedS3StorageConfig adapterConfig, Executor executor) {
        // We can't use the factory here because we're setting our own (mock) client.
        ExtendedS3Storage storage = new ExtendedS3Storage(client, adapterConfig);
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
        private TestContext setup;

        @Before
        public void setUp() throws Exception {
            this.setup = new TestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.setup != null) {
                this.setup.close();
            }
        }

        @Override
        protected Storage createStorage() {
            ExtendedS3Storage storage = new ExtendedS3Storage(setup.client, setup.adapterConfig);
            return wrap(storage);
        }
    }

    //endregion

    private static class TestContext {
        private static final String BUCKET_NAME_PREFIX = "pravegatest-";
        private final ExtendedS3StorageConfig adapterConfig;
        private final S3JerseyClient client;
        private final S3ImplBase s3Proxy;
        private final int port = TestUtils.getAvailableListenPort();
        private final String configUri = "http://127.0.0.1:" + port + "?identity=x&secretKey=x";
        private final S3Config s3Config;

        TestContext() throws Exception {
            String bucketName = BUCKET_NAME_PREFIX + UUID.randomUUID().toString();
            this.adapterConfig = ExtendedS3StorageConfig.builder()
                    .with(ExtendedS3StorageConfig.CONFIGURI, configUri)
                    .with(ExtendedS3StorageConfig.BUCKET, bucketName)
                    .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                    .build();
            s3Config = new ConfigUri<>(S3Config.class).parseUri(configUri);
            s3Proxy = new S3ProxyImpl(configUri, s3Config);
            s3Proxy.start();
            client = new S3JerseyClientWrapper(s3Config, s3Proxy);
            client.createBucket(bucketName);
            List<ObjectKey> keys = client.listObjects(bucketName).getObjects().stream()
                    .map(object -> new ObjectKey(object.getKey()))
                    .collect(Collectors.toList());

            if (!keys.isEmpty()) {
                client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
            }
        }

        void close() throws Exception {
            if (client != null) {
                client.destroy();
            }
            s3Proxy.stop();
        }
    }
}
