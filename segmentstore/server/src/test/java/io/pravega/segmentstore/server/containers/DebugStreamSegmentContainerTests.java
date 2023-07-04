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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
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
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
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
import io.pravega.shared.NameUtils;
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
import java.util.concurrent.TimeUnit;

import static io.pravega.segmentstore.contracts.Attributes.CORE_ATTRIBUTE_ID_PREFIX;
import static io.pravega.segmentstore.server.containers.ContainerRecoveryUtils.recoverAllSegments;
import static io.pravega.segmentstore.server.containers.ContainerRecoveryUtils.updateCoreAttributes;

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
    private static final SegmentType BASIC_TYPE = SegmentType.STREAM_SEGMENT;
    private static final SegmentType[] SEGMENT_TYPES = new SegmentType[]{
            BASIC_TYPE,
            SegmentType.builder(BASIC_TYPE).build(),
            SegmentType.builder(BASIC_TYPE).internal().build(),
            SegmentType.builder(BASIC_TYPE).critical().build(),
            SegmentType.builder(BASIC_TYPE).system().build(),
            SegmentType.builder(BASIC_TYPE).system().critical().build(),
    };
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .with(ContainerConfig.DATA_INTEGRITY_CHECKS_ENABLED, true)
            .build();

    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
            .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
            .build();

    // DL config that can be used to simulate no DurableLog truncations.
    private static final DurableLogConfig NO_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10000)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 50000)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1024 * 1024 * 1024L)
            .build();

    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
            .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
            .build();

    private static final WriterConfig INFREQUENT_FLUSH_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1024 * 1024 * 1024)
            .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 3000)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 250000L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 500L)
            .build();

    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 200)
            .with(ContainerConfig.DATA_INTEGRITY_CHECKS_ENABLED, true)
            .build();

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT_MILLIS);

    @Override
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
    public void testDataRecoveryStorageLevel() throws Exception {
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
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount, true);
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
        recoverAllSegments(new AsyncStorageWrapper(s, executorService()), debugStreamSegmentContainerMap, executorService(), TIMEOUT);

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
     * The test create a dummy metadata segment and its attribute segment using a storage instance. The method under the
     * test creates copies of the segments. After that, it is verified if the new segments exist.
     */
    @Test
    public void testBackUpMetadataAndAttributeSegments() {
        // Create a storage.
        StorageFactory storageFactory = new InMemoryStorageFactory(executorService());
        @Cleanup
        Storage s = storageFactory.createStorageAdapter();
        s.initialize(1);
        log.info("Created a storage instance");

        String metadataSegment = NameUtils.getMetadataSegmentName(CONTAINER_ID);
        String attributeSegment = NameUtils.getAttributeSegmentName(metadataSegment);
        String backUpMetadataSegment = "segment-" + RANDOM.nextInt();
        String backUpAttributeSegment = "segment-" + RANDOM.nextInt();

        s.create(metadataSegment, TIMEOUT).join();
        s.create(attributeSegment, TIMEOUT).join();

        ContainerRecoveryUtils.backUpMetadataAndAttributeSegments(s, CONTAINER_ID, backUpMetadataSegment, backUpAttributeSegment,
                executorService(), TIMEOUT).join();

        // back up metadata segment should exist
        Assert.assertTrue("Unexpected result for existing segment (no files).", s.exists(backUpMetadataSegment, TIMEOUT).join());
        // back up attribute segment should exist
        Assert.assertTrue("Unexpected result for existing segment (no files).", s.exists(backUpAttributeSegment, TIMEOUT).join());
    }

    /**
     * The test creates a container. Using it, some segments are created, data is written to them and their attributes are
     * updated. After Tier1 is flushed to the storage, the container is closed. From the storage, we recover the
     * segments and update their attributes. The data and attributes for each of the segment are verified to validate the
     * recovery process.
     */
    @Test
    public void testDataRecoveryContainerLevel() throws Exception {
        int attributesUpdatesPerSegment = 50;
        int segmentsCount = 10;
        final AttributeId attributeReplace = AttributeId.uuid(CORE_ATTRIBUTE_ID_PREFIX, RANDOM.nextInt(10));
        final long expectedAttributeValue = attributesUpdatesPerSegment;
        int containerId = 0;
        int containerCount = 1;
        int maxDataSize = 1024 * 1024; // 1MB

        StorageFactory storageFactory = new InMemoryStorageFactory(executorService());

        @Cleanup
        TestContext context = createContext(executorService());
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer container = new MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                context.getDefaultExtensions(), executorService());
        container.startAsync().awaitRunning();

        // 1. Create segments.
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (int i = 0; i < segmentsCount; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            opFutures.add(container.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT));
        }

        // 1.1 Wait for all segments to be created prior to using them.
        Futures.allOf(opFutures).join();
        opFutures.clear();

        // 2. Write data and update some of the attributes.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArraySegment> segmentContents = new HashMap<>();
        for (String segmentName : segmentNames) {
            val dataSize = RANDOM.nextInt(maxDataSize);

            byte[] writeData = populate(dataSize);
            val appendData = new ByteArraySegment(writeData);

            val append = container.append(segmentName, appendData, null, TIMEOUT);
            opFutures.add(Futures.toVoid(append));
            lengths.put(segmentName, (long) dataSize);
            segmentContents.put(segmentName, appendData);

            for (int i = 0; i < attributesUpdatesPerSegment; i++) {
                AttributeUpdateCollection attributeUpdates = AttributeUpdateCollection.from(
                        new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, i + 1));
                opFutures.add(container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Instead of waiting for the Writer to move data to Storage, we invoke the flushToStorage to verify that all
        // operations have been applied to Storage.
        val forceFlush = container.flushToStorage(TIMEOUT);
        forceFlush.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        container.close();

        // Get the storage instance
        @Cleanup
        Storage storage = storageFactory.createStorageAdapter();

        // 4. Move container metadata and its attribute segment to back up segments.
        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage, containerCount,
                executorService(), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        OperationLogFactory localDurableLogFactory2 = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG,
                new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService()), executorService());
        // Starts a DebugSegmentContainer with new Durable Log
        @Cleanup
        TestContext context1 = createContext(executorService());

        @Cleanup
        MetadataCleanupContainer container2 = new MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory2,
                context1.readIndexFactory, context1.attributeIndexFactory, context1.writerFactory, storageFactory,
                context1.getDefaultExtensions(), executorService());
        container2.startAsync().awaitRunning();
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainersMap = new HashMap<>();
        debugStreamSegmentContainersMap.put(containerId, container2);

        // 4. Recover all segments.
        recoverAllSegments(storage, debugStreamSegmentContainersMap, executorService(), TIMEOUT);
        // 5. Update core attributes using back up segments.
        updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainersMap, executorService(), TIMEOUT);

        // 6. Verify Segment Data.
        for (val sc : segmentContents.entrySet()) {
            // Contents.
            byte[] expectedData = sc.getValue().array();
            byte[] actualData = new byte[expectedData.length];
            container2.read(sc.getKey(), 0, actualData.length, TIMEOUT).join().readRemaining(actualData, TIMEOUT);
            Assert.assertArrayEquals("Unexpected contents for " + sc.getKey(), expectedData, actualData);

            // Length.
            val si = container2.getStreamSegmentInfo(sc.getKey(), TIMEOUT).join();
            Assert.assertEquals("Unexpected length for " + sc.getKey(), expectedData.length, si.getLength());

            // Attributes.
            val attributes = container2.getAttributes(sc.getKey(), Collections.singleton(attributeReplace),
                    false, TIMEOUT).join();
            Assert.assertEquals("Unexpected attribute for " + sc.getKey(), expectedAttributeValue, (long) attributes.get(attributeReplace));
        }
    }

    private SegmentType getSegmentType(String segmentName) {
        // "Randomize" through all the segment types, but using a deterministic function so we can check results later.
        return SEGMENT_TYPES[Math.abs(segmentName.hashCode() % SEGMENT_TYPES.length)];
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
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
        ContainerRecoveryUtils.copySegment(s, sourceSegmentName, targetSegmentName, executorService(), TIMEOUT).join();

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
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(INFREQUENT_FLUSH_WRITER_CONFIG, scheduledExecutorService);
        }

        public SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(TableExtensionConfig.builder().build(), c, this.cacheManager, e);
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
