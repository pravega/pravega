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
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
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
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.segmentstore.server.containers.ContainerRecoveryUtils.recoverAllSegments;

/**
 * Tests for DebugStreamSegmentContainer class.
 */
@Slf4j
public class DebugStreamSegmentContainerTests extends ThreadPooledTestSuite {
    private static final int MIN_SEGMENT_LENGTH = 0; // Used in randomly generating the length for a segment
    private static final int MAX_SEGMENT_LENGTH = 10100; // Used in randomly generating the length for a segment
    private static final int CONTAINER_ID = 1234567;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final int TEST_TIMEOUT_MILLIS = 60 * 1000;
    private static final Duration TIMEOUT = Duration.ofMillis(TEST_TIMEOUT_MILLIS);
    private static final Random RANDOM = new Random(1234);
    private static final int THREAD_POOL_COUNT = 30;
    private static final String APPEND_FORMAT = "Segment_%s_Append_%d";
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
    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 200)
            .build();

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT_MILLIS);

    protected int getThreadPoolSize() {
        return THREAD_POOL_COUNT;
    }

    /**
     * It tests the ability to register an existing segment(segment existing only in the Long-Term Storage) using debug
     * segment container. Method registerSegment in {@link DebugStreamSegmentContainer} is tested here.
     * The test starts a debug segment container and creates some segments using it and then verifies if the segments
     * were created successfully.
     */
    @Test
    public void testRegisterExistingSegment() {
        int maxSegmentCount = 100;
        final int createdSegmentCount = maxSegmentCount * 2;

        // Sets up dataLogFactory, readIndexFactory, attributeIndexFactory etc for the DebugSegmentContainer.
        @Cleanup
        TestContext context = createContext(executorService());
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        // Starts a DebugSegmentContainer.
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, CONTAINER_CONFIG, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();
        log.info("Started debug segment container.");

        // Record details(name, length & sealed status) of each segment to be created.
        ArrayList<String> segments = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        long[] segmentLengths = new long[createdSegmentCount];
        boolean[] segmentSealedStatus = new boolean[createdSegmentCount];
        for (int i = 0; i < createdSegmentCount; i++) {
            segmentLengths[i] = MIN_SEGMENT_LENGTH + RANDOM.nextInt(MAX_SEGMENT_LENGTH - MIN_SEGMENT_LENGTH);
            segmentSealedStatus[i] = RANDOM.nextBoolean();
            String name = "Segment_" + i;
            segments.add(name);
            futures.add(localContainer.registerSegment(name, segmentLengths[i], segmentSealedStatus[i]));
        }
        // Creates all the segments.
        Futures.allOf(futures).join();
        log.info("Created the segments using debug segment container.");

        // Verify the Segments are still there with their length & sealed status.
        for (int i = 0; i < createdSegmentCount; i++) {
            SegmentProperties props = localContainer.getStreamSegmentInfo(segments.get(i), TIMEOUT).join();
            Assert.assertEquals("Segment length mismatch ", segmentLengths[i], props.getLength());
            Assert.assertEquals("Segment sealed status mismatch", segmentSealedStatus[i], props.isSealed());
        }
        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Use a storage instance to create segments. Lists the segments from the storage and and then recreates them using
     * debug segment containers. Before re-creating(or registering), the segments are mapped to their respective debug
     * segment container. Once registered, segment's properties are matched to verify if the test was successful or not.
     */
    @Test
    public void testEndToEnd() throws Exception {
        // Segments are mapped to four different containers.
        int containerCount = 4;
        int segmentsToCreateCount = 50;

        // Create a storage.
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, new SegmentRollingPolicy(1));
        s.initialize(1);
        log.info("Created a storage instance");

        // Record details(name, container Id & sealed status) of each segment to be created.
        Set<String> sealedSegments = new HashSet<>();
        byte[] data = "data".getBytes();
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount);
        Map<Integer, ArrayList<String>> segmentByContainers = new HashMap<>();

        // Create segments and get their container Ids, sealed status and names to verify.
        for (int i = 0; i < segmentsToCreateCount; i++) {
            String segmentName = "segment-" + RANDOM.nextInt();

            // Use segmentName to map to different containers.
            int containerId = segToConMapper.getContainerId(segmentName);
            ArrayList<String> segmentsList = segmentByContainers.get(containerId);
            if (segmentsList == null) {
                segmentsList = new ArrayList<>();
                segmentByContainers.put(containerId, segmentsList);
            }
            segmentByContainers.get(containerId).add(segmentName);

            // Create segments, write data and randomly seal some of them.
            val wh1 = s.create(segmentName);
            // Write data.
            s.write(wh1, 0, new ByteArrayInputStream(data), data.length);
            if (RANDOM.nextBoolean()) {
                s.seal(wh1);
                sealedSegments.add(segmentName);
            }
        }
        log.info("Created some segments using the storage.");

        @Cleanup
        TestContext context = createContext(executorService());
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory,
                executorService());

        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        log.info("Start a debug segment container corresponding to each container id.");
        for (int containerId = 0; containerId < containerCount; containerId++) {
            MetadataCleanupContainer localContainer = new MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                    context.getDefaultExtensions(), executorService());

            Services.startAsync(localContainer, executorService()).join();
            debugStreamSegmentContainerMap.put(containerId, localContainer);
        }

        log.info("Recover all segments using the storage and debug segment containers.");
        recoverAllSegments(new AsyncStorageWrapper(s, executorService()), debugStreamSegmentContainerMap, executorService());

        // Re-create all segments which were listed.
        for (int containerId = 0; containerId < containerCount; containerId++) {
            for (String segment : segmentByContainers.get(containerId)) {
                SegmentProperties props = debugStreamSegmentContainerMap.get(containerId).getStreamSegmentInfo(segment, TIMEOUT).join();
                Assert.assertEquals("Segment length mismatch.", data.length, props.getLength());
                Assert.assertEquals("Sealed status of the segment don't match.", sealedSegments.contains(segment), props.isSealed());
            }
            debugStreamSegmentContainerMap.get(containerId).close();
        }
    }

    /**
     * The test creates a segment and then writes some data to it. The method under the test copies the contents of the
     * segment to a segment with a different name. At the end, it is verified that the new segment has the accurate
     * contents from the first one.
     */
    @Test
    public void testCopySegment() {
        int dataSize = 10 * 1024 * 1024;
        // Create a storage.
        StorageFactory storageFactory = new InMemoryStorageFactory(executorService());
        @Cleanup
        Storage s = storageFactory.createStorageAdapter();
        s.initialize(1);
        log.info("Created a storage instance");

        String sourceSegmentName = "segment-" + RANDOM.nextInt();
        String targetSegmentName = "segment-" + RANDOM.nextInt();

        // Create source segment
        s.create(sourceSegmentName, TIMEOUT).join();
        val handle = s.openWrite(sourceSegmentName).join();

        // do some writing
        byte[] writeData = populate(dataSize);
        val dataStream = new ByteArrayInputStream(writeData);
        s.write(handle, 0, dataStream, writeData.length, TIMEOUT).join();

        // copy segment
        ContainerRecoveryUtils.copySegment(s, sourceSegmentName, targetSegmentName, executorService()).join();

        // source segment should exist
        Assert.assertTrue("Unexpected result for existing segment (no files).", s.exists(sourceSegmentName, null).join());
        // target segment should exist
        Assert.assertTrue("Unexpected result for existing segment (no files).", s.exists(targetSegmentName, null).join());

        // Do reading on target segment to verify if the copy was successful or not
        val readHandle = s.openRead(targetSegmentName).join();
        int length = writeData.length;
        byte[] readBuffer = new byte[length];
        int bytesRead = s.read(readHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals(String.format("Unexpected number of bytes read."), length, bytesRead);
        AssertExtensions.assertArrayEquals(String.format("Unexpected read result."), writeData, 0, readBuffer, 0, bytesRead);
    }

    private void populate(byte[] data) {
        RANDOM.nextBytes(data);
    }

    private byte[] populate(int size) {
        byte[] bytes = new byte[size];
        populate(bytes);
        return bytes;
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

    public static TestContext createContext(ScheduledExecutorService scheduledExecutorService) {
        return new TestContext(scheduledExecutorService);
    }


    public static class TestContext implements AutoCloseable {
        public final StorageFactory storageFactory;
        public final DurableDataLogFactory dataLogFactory;
        public final ReadIndexFactory readIndexFactory;
        public final AttributeIndexFactory attributeIndexFactory;
        public final WriterFactory writerFactory;
        public final CacheStorage cacheStorage;
        public final CacheManager cacheManager;

        TestContext(ScheduledExecutorService scheduledExecutorService) {
            this.storageFactory = new InMemoryStorageFactory(scheduledExecutorService);
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, scheduledExecutorService);
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE / 5);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, scheduledExecutorService);
        }

        public SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(c, this.cacheManager, e);
        }

        @Override
        public void close() {
            this.readIndexFactory.close();
            this.dataLogFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }
}
