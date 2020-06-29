/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataRecoveryTestUtils;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Tests for DebugStreamSegmentContainer class.
 */
public class DebugStreamSegmentContainerTests extends ThreadPooledTestSuite {

    private static final int MIN_SEGMENT_LENGTH = 0;
    private static final int MAX_SEGMENT_LENGTH = 10100;
    private static final int CONTAINER_ID = 1234567;
    private static final int EXPECTED_PINNED_SEGMENT_COUNT = 4;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final int TEST_TIMEOUT_MILLIS = 100 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(TEST_TIMEOUT_MILLIS);
    private static final Random RANDOM = new Random(1234);
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();

    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
            .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
            .build();

    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
            .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
            .build();

    private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
            .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 3)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .build();

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT_MILLIS);

    /**
     * Tests the ability to create Segments.
     */
    @Test
    public void testCreateStreamSegment() {
        int maxSegmentCount = 100;
        final int createdSegmentCount = maxSegmentCount * 2;
        final ContainerConfig containerConfig = ContainerConfig
                .builder()
                .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
                .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, maxSegmentCount + EXPECTED_PINNED_SEGMENT_COUNT)
                .build();

        final DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
                .build();

        @Cleanup
        TestContext context = createContext();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(durableLogConfig, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        ArrayList<String> segments = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        long[] segmentLengths = new long[createdSegmentCount];
        boolean[] segmentSealedStatus = new boolean[createdSegmentCount];
        for (int i = 0; i < createdSegmentCount; i++) {
            segmentLengths[i] = MIN_SEGMENT_LENGTH + RANDOM.nextInt(MAX_SEGMENT_LENGTH - MIN_SEGMENT_LENGTH);
            segmentSealedStatus[i] = RANDOM.nextBoolean();
            String name = "Segment_" + i;
            segments.add(name);
            futures.add(localContainer.createStreamSegment(name, segmentLengths[i], segmentSealedStatus[i]));
        }
        Futures.allOf(futures).join();

        // Verify the Segments are still there with their length & sealed status.
        for (int i = 0; i < createdSegmentCount; i++) {
            SegmentProperties props = localContainer.getStreamSegmentInfo(segments.get(i), TIMEOUT).join();
            Assert.assertEquals("Segment length mismatch ", segmentLengths[i], props.getLength());
            Assert.assertEquals("Segment sealed status mismatch", segmentSealedStatus[i], props.isSealed());
        }
        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Tests list segments from a storage after creating them and then recreate the segments using DebugStreamSegmentContainer.
     * @throws Exception in case of an exception encountered during execution.
     */
    @Test
    public void testEndToEnd() throws Exception {
        int containerCount = 4;
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, new SegmentRollingPolicy(1));
        Set<String> sealedSegments = new HashSet<>();
        s.initialize(1);
        int segmentsToCreateCount = 50;
        byte[] data = "data".getBytes();
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount);

        int segmentsCountByContainer[] = new int[containerCount];
        ArrayList<ArrayList<String>> segmentByContainers = new ArrayList<ArrayList<String>>();
        for (int containerId = 0; containerId<containerCount; containerId++) {
            ArrayList<String> segmentList = new ArrayList<>();
            segmentByContainers.add(segmentList);
        }

        // Create some segments and get their container Id and names to verify.
        for (int i = 0; i < segmentsToCreateCount; i++) {
            String segmentName = "segment-" + RANDOM.nextInt();
            segmentsCountByContainer[segToConMapper.getContainerId(segmentName)]++;
            segmentByContainers.get(segToConMapper.getContainerId(segmentName)).add(segmentName);
            val wh1 = s.create(segmentName); // Use segmentName to map to different containers
            // Write data.
            s.write(wh1, 0, new ByteArrayInputStream(data), data.length);
            if (RANDOM.nextInt(2) == 1) {
                s.seal(wh1);
                sealedSegments.add(segmentName);
            }
        }

        int segmentsRecoveredCount = 0;
        // List all segments
        Map<Integer, List<SegmentProperties>> segments = DataRecoveryTestUtils.listAllSegments(new AsyncStorageWrapper(s,
                DataRecoveryTestUtils.createExecutorService(10)), containerCount);
        // Check every segments count by container Id
        for (int i = 0; i < segments.size(); i++) {
            segmentsRecoveredCount += segments.get(i).size();
            Assert.assertTrue("Number of recovered segments is less than number of created segments with this container",
                    segments.get(i).size() >= segmentsCountByContainer[i]);
        }
        Assert.assertTrue("Number of recovered segments is less than number of created segments",
                segmentsRecoveredCount >= segmentsToCreateCount);

        final ContainerConfig containerConfig = ContainerConfig
                .builder()
                .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
                .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, segmentsToCreateCount + EXPECTED_PINNED_SEGMENT_COUNT)
                .build();
        @Cleanup
        TestContext context = createContext();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory,
                DataRecoveryTestUtils.createExecutorService(10));

        // Recover all segments
        for (int containerId = 0; containerId < containerCount; containerId++) {
            @Cleanup
            MetadataCleanupContainer localContainer = new MetadataCleanupContainer(containerId, containerConfig, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                    context.getDefaultExtensions(), DataRecoveryTestUtils.createExecutorService(10));
            localContainer.startAsync().awaitRunning();
            DataRecoveryTestUtils.createAllSegments(localContainer, segments.get(containerId));

            //
            for (String segmentName : segmentByContainers.get(containerId)) {
                SegmentProperties props = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).join();
                Assert.assertEquals("Segment length mismatch ", data.length, props.getLength());
                if (sealedSegments.contains(segmentName)) {
                    Assert.assertTrue("Segment should have been sealed", props.isSealed());
                }
            }
            localContainer.stopAsync().awaitTerminated();
        }
    }

    public static class MetadataCleanupContainer extends DebugStreamSegmentContainer {
        private final ScheduledExecutorService executor;

        public MetadataCleanupContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                        ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                                        WriterFactory writerFactory, StorageFactory storageFactory,
                                        SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
            super(streamSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                    storageFactory, createExtensions, executor);
            this.executor = executor;
        }
    }

    public TestContext createContext() {
        return new TestContext(DEFAULT_CONFIG, null);
    }


    public class TestContext implements AutoCloseable {
        final SegmentContainerFactory containerFactory;
        final SegmentContainer container;
        private final WatchableInMemoryStorageFactory storageFactory;
        private final DurableDataLogFactory dataLogFactory;
        private final OperationLogFactory operationLogFactory;
        private final ReadIndexFactory readIndexFactory;
        private final AttributeIndexFactory attributeIndexFactory;
        private final WriterFactory writerFactory;
        private final CacheStorage cacheStorage;
        private final CacheManager cacheManager;
        private final Storage storage;

        TestContext(ContainerConfig config, SegmentContainerFactory.CreateExtensions createAdditionalExtensions) {
            this.storageFactory = new WatchableInMemoryStorageFactory(executorService());
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService());
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService());
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService());
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, executorService());
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, executorService());
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, executorService());
            this.containerFactory = new StreamSegmentContainerFactory(config, this.operationLogFactory,
                    this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, this.storageFactory,
                    createExtensions(createAdditionalExtensions), executorService());
            this.container = this.containerFactory.createDebugStreamSegmentContainer(CONTAINER_ID);
            this.storage = this.storageFactory.createStorageAdapter();
        }

        SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(c, this.cacheManager, e);
        }

        private SegmentContainerFactory.CreateExtensions createExtensions(SegmentContainerFactory.CreateExtensions additional) {
            return (c, e) -> {
                val extensions = new HashMap<Class<? extends SegmentContainerExtension>, SegmentContainerExtension>();
                extensions.putAll(getDefaultExtensions().apply(c, e));
                if (additional != null) {
                    extensions.putAll(additional.apply(c, e));
                }

                return extensions;
            };
        }

        @Override
        public void close() {
            this.container.close();
            this.dataLogFactory.close();
            this.storage.close();
            this.storageFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }

    private static class WatchableInMemoryStorageFactory extends InMemoryStorageFactory {
        private final ConcurrentHashMap<String, Long> truncationOffsets = new ConcurrentHashMap<>();

        public WatchableInMemoryStorageFactory(ScheduledExecutorService executor) {
            super(executor);
        }

        @Override
        public Storage createStorageAdapter() {
            return new WatchableAsyncStorageWrapper(new RollingStorage(this.baseStorage), this.executor);
        }

        private class WatchableAsyncStorageWrapper extends AsyncStorageWrapper {
            public WatchableAsyncStorageWrapper(SyncStorage syncStorage, Executor executor) {
                super(syncStorage, executor);
            }

            @Override
            public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
                return super.truncate(handle, offset, timeout)
                        .thenRun(() -> truncationOffsets.put(handle.getSegmentName(), offset));
            }
        }
    }
}