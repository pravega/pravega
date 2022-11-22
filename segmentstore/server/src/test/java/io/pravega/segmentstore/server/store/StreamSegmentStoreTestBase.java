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
package io.pravega.segmentstore.server.store;

import com.google.common.collect.Streams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainerTests;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for any test that verifies the functionality of a StreamSegmentStore class.
 */
@Slf4j
public abstract class StreamSegmentStoreTestBase extends ThreadPooledTestSuite {
    //region Test Configuration

    // Even though this should work with just 1-2 threads, doing so would cause this test to run for a long time. Choosing
    // a decent size so that the tests do finish up within a few seconds.
    private static final int THREADPOOL_SIZE_SEGMENT_STORE = 20;
    private static final int THREADPOOL_SIZE_SEGMENT_STORE_STORAGE = 10;
    private static final int THREADPOOL_SIZE_TEST = 3;
    private static final String EMPTY_SEGMENT_NAME = "Empty_Segment";
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 1;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int ATTRIBUTE_UPDATES_PER_SEGMENT = 100;
    private static final int MAX_INSTANCE_COUNT = 4;
    private static final int CONTAINER_COUNT = 4;
    private static final long DEFAULT_EPOCH = 1;
    private static final List<AttributeId> ATTRIBUTES = Streams.concat(Stream.of(Attributes.EVENT_COUNT), IntStream.range(0, 10).mapToObj(i -> AttributeId.randomUUID())).collect(Collectors.toList());
    private static final int ATTRIBUTE_UPDATE_DELTA = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();

    // Configurations for DebugSegmentContainer
    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 100)
            .build();
    private static final DurableLogConfig DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
            .build();

    private static final SegmentType BASIC_SEGMENT_TYPE = SegmentType.STREAM_SEGMENT;

    protected final ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig
                    .builder()
                    .with(ServiceConfig.CONTAINER_COUNT, CONTAINER_COUNT)
                    .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE)
                    .with(ServiceConfig.STORAGE_THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE_STORAGE)
                    .with(ServiceConfig.CACHE_POLICY_MAX_SIZE, 64 * 1024 * 1024L)
                    .with(ServiceConfig.CACHE_POLICY_MAX_TIME, 30))
            .include(ContainerConfig
                    .builder()
                    .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS))
            .include(DurableLogConfig
                    .builder()
                    .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                    .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                    .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L))
            .include(ReadIndexConfig
                    .builder()
                    .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 512) // Need this for truncation testing.
                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024))
            .include(WriterConfig
                    .builder()
                    .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                    .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, ATTRIBUTES.size() / 2)
                    .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                    .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                    .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L));

    @Override
    protected int getThreadPoolSize() {
        return THREADPOOL_SIZE_TEST;
    }

    /**
     * When overridden in a derived class, this will return a multiplier applied to APPENDS_PER_SEGMENT and
     * ATTRIBUTE_COUNT_PER_SEGMENT that will be used for the fencing test. For non-memory tests, executing too many
     * operations (in sequence, like the test does) will cause the test to run for too long, hence a need to be able to
     * reduce this if needed.
     */
    protected double getFencingTestOperationMultiplier() {
        return 1.0;
    }

    /**
     * When overridden in a derived class, this will indicate whether we want to execute a new set of Segment Appends
     * after we have merged transactions into them. Default is true, but some tests may take longer to execute so this
     * can be disabled for those.
     *
     * @return True if {@link #testEndToEndWithChunkedStorage()} should append data after merging transactions, false otherwise.
     */
    protected boolean appendAfterMerging() {
        return true;
    }

    /**
     * SegmentStore is used to create some segments, write data to them and let them flush to the storage.
     * This test only uses this storage to restore the container metadata segments in a new durable data log. Segment
     * properties are matched for verification after the restoration.
     * @throws Exception If an exception occurred.
     */
    public void testSegmentRestoration() throws Exception {
        ArrayList<String> segmentNames;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        HashMap<String, Long> lengths = new HashMap<>();
        ArrayList<ByteBuf> appendBuffers = new ArrayList<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        try (val builder = createBuilder(0, false)) {
            val segmentStore = builder.createStreamSegmentService();

            segmentNames = createSegments(segmentStore);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));
            transactionsBySegment = createTransactions(segmentNames, segmentStore);
            log.info("Created Transactions: {}.", transactionsBySegment.values().stream().flatMap(Collection::stream).collect(Collectors.joining(", ")));

            // Add some appends and seal segments
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            appendData(segmentsAndTransactions, segmentContents, lengths, appendBuffers, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished appending data.");

            // Wait for flushing the segments to tier2
            waitForSegmentsInStorage(segmentNames, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished waiting for segments in Storage.");

            // Get the persistent storage from readOnlySegmentStore.
            @Cleanup
            Storage storage = builder.createStorageFactory().createStorageAdapter();
            storage.initialize(DEFAULT_EPOCH);

            // Create the environment for DebugSegmentContainer using the given storageFactory.
            @Cleanup
            DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());
            OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, context.dataLogFactory,
                    executorService());

            // Start a debug segment container corresponding to each container Id and put it in the Hashmap with the Id.
            Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
            for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
                // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
                ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, containerId, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                DebugStreamSegmentContainerTests.MetadataCleanupContainer localContainer = new
                        DebugStreamSegmentContainerTests.MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                        context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                        context.getDefaultExtensions(), executorService());

                Services.startAsync(localContainer, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                debugStreamSegmentContainerMap.put(containerId, localContainer);
            }

            // Restore all segments from the long term storage using debug segment container.
            ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService(), TIMEOUT);

            // Verify that segment details match post restoration.
            SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(CONTAINER_COUNT, true);
            for (String segment : segmentNames) {
                int containerId = segToConMapper.getContainerId(segment);
                SegmentProperties props = debugStreamSegmentContainerMap.get(containerId).getStreamSegmentInfo(segment, TIMEOUT)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertEquals("Segment length mismatch.", (long) lengths.get(segment), props.getLength());
            }

            for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
                debugStreamSegmentContainerMap.get(containerId).close();
            }
        }
    }

    /**
     * Tests an end-to-end scenario for the SegmentStore, utilizing a read-write SegmentStore for making modifications
     * (writes, seals, creates, etc.) and another instance to verify the changes being persisted into Storage.
     * This test uses ChunkedSegmentStorage.
     * * Appends
     * * Reads
     * * Segment and transaction creation
     * * Transaction mergers
     * * Recovery
     *
     * @throws Exception If an exception occurred.
     */
    @Test(timeout = 120000)
    public void testEndToEndWithChunkedStorage() throws Exception {
        endToEndProcess(false, true);
    }

    /**
     * End to end test to verify segment store process.
     *
     * @param verifySegmentContent whether it's needed to read segment content for verification.
     * @param useChunkedStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
     * @throws Exception If an exception occurred.
     */
    void endToEndProcess(boolean verifySegmentContent, boolean useChunkedStorage) throws Exception {
        ArrayList<String> segmentNames;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        HashMap<String, Long> lengths = new HashMap<>();
        ArrayList<ByteBuf> appendBuffers = new ArrayList<>();
        HashMap<String, Long> startOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        long expectedAttributeValue = 0;
        int instanceId = 0;

        // Phase 1: Create segments and add some appends.
        log.info("Starting Phase 1.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage)) {
            val segmentStore = builder.createStreamSegmentService();

            // Create the StreamSegments.
            segmentNames = createSegments(segmentStore);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));
            transactionsBySegment = createTransactions(segmentNames, segmentStore);
            log.info("Created Transactions: {}.", transactionsBySegment.values().stream().flatMap(Collection::stream).collect(Collectors.joining(", ")));

            // Add some appends.
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            appendData(segmentsAndTransactions, segmentContents, lengths, appendBuffers, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            expectedAttributeValue += ATTRIBUTE_UPDATE_DELTA;
            log.info("Finished appending data.");

            checkSegmentStatus(lengths, startOffsets, false, false, expectedAttributeValue, segmentStore);
            log.info("Finished Phase 1");
        }

        // Verify all buffers have been released.
        checkAppendLeaks(appendBuffers);
        appendBuffers.clear();

        // Phase 2: Force a recovery and merge all transactions.
        log.info("Starting Phase 2.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage)) {
            val segmentStore = builder.createStreamSegmentService();
            checkReads(segmentContents, segmentStore);
            log.info("Finished checking reads.");

            // Merge all transactions.
            mergeTransactions(transactionsBySegment, lengths, segmentContents, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished merging transactions.");

            if (appendAfterMerging()) {
                // Check the status now. A nice side effect of this is that it loads all extended attributes from Storage so
                // that we can modify them in the next step (during appending).
                checkSegmentStatus(lengths, startOffsets, false, false, expectedAttributeValue, segmentStore);

                // Append more data.
                appendData(segmentNames, segmentContents, lengths, appendBuffers, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                expectedAttributeValue += ATTRIBUTE_UPDATE_DELTA;
                log.info("Finished appending after merging transactions.");
            } else {
                log.info("Skipped appending after merging transactions due to setting being disabled in this test.");
            }

            checkSegmentStatus(lengths, startOffsets, false, false, expectedAttributeValue, segmentStore);
            log.info("Finished Phase 2.");
        }

        // Verify all buffers have been released.
        checkAppendLeaks(appendBuffers);
        appendBuffers.clear();

        // Phase 3: Force a recovery, immediately check reads, then truncate and read at the same time.
        log.info("Starting Phase 3.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage);) {
            val segmentStore = builder.createStreamSegmentService();
            checkReads(segmentContents, segmentStore);
            log.info("Finished checking reads.");
        }

        if (verifySegmentContent) {
            try (val builder = createBuilder(++instanceId, useChunkedStorage);) {
                val segmentStore = builder.createStreamSegmentService();
                // Wait for all the data to move to Storage.
                waitForSegmentsInStorage(segmentNames, segmentStore)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                log.info("Finished waiting for segments in Storage.");

                checkStorage(segmentContents, segmentStore);
                log.info("Finished Storage check.");

                checkReadsWhileTruncating(segmentContents, startOffsets, segmentStore);
                log.info("Finished checking reads while truncating.");

                checkStorage(segmentContents, segmentStore);
                log.info("Finished Phase 3.");
            }
        }

        // Phase 4: Force a recovery, seal segments and then delete them.
        log.info("Starting Phase 4.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage)) {
            val segmentStore = builder.createStreamSegmentService();
            // Seals.
            sealSegments(segmentNames, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished sealing.");

            checkSegmentStatus(lengths, startOffsets, true, false, expectedAttributeValue, segmentStore);
            if (verifySegmentContent) {
                waitForSegmentsInStorage(segmentNames, segmentStore)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                log.info("Finished waiting for segments in Storage.");
            }
        }

        try (val builder = createBuilder(++instanceId, useChunkedStorage)) {
            val segmentStore = builder.createStreamSegmentService();
            // Deletes.
            deleteSegments(segmentNames, segmentStore).join();
            log.info("Finished deleting segments.");

            checkSegmentStatus(lengths, startOffsets, true, true, expectedAttributeValue, segmentStore);
            log.info("Finished Phase 4.");
        }

        log.info("Finished.");
    }

    /**
     * Tests an end-to-end scenario for the SegmentStore, using a read-write SegmentStore to add some segment data.
     * Using another instance to verify that the segments have been successfully persisted to Storage.
     * This test does not use ChunkedSegmentStorage.
     *
     * @throws Exception If an exception occurred.
     */
    @Test(timeout = 120000)
    public void testFlushToStorage() throws Exception {
        endToEndFlushToStorage(false);
    }

    /**
     * Tests an end-to-end scenario for the SegmentStore, using a read-write SegmentStore to add some segment data.
     * Using another instance to verify that the segments have been successfully persisted to Storage.
     * This test uses ChunkedSegmentStorage.
     *
     * @throws Exception If an exception occurred.
     */
    @Test(timeout = 120000)
    public void testFlushToStorageWithChunkedStorage() throws Exception {
        endToEndFlushToStorage(true);
    }

    /**
     * End to end test to verify storage flush API.
     *
     * @param useChunkedStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
     * @throws Exception If an exception occurred.
     */
    void endToEndFlushToStorage(boolean useChunkedStorage) throws Exception {
        ArrayList<String> segmentNames;
        HashMap<String, Long> lengths = new HashMap<>();
        ArrayList<ByteBuf> appendBuffers = new ArrayList<>();
        HashMap<String, Long> startOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        long expectedAttributeValue = 0;
        int instanceId = 0;

        // Phase 1: Create segments and add some appends.
        log.info("Starting Phase 1.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage)) {
            val segmentStore = builder.createStreamSegmentService();

            // Create the StreamSegments.
            segmentNames = createSegments(segmentStore);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));

            // Add some appends.
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            appendData(segmentsAndTransactions, segmentContents, lengths, appendBuffers, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            expectedAttributeValue += ATTRIBUTE_UPDATE_DELTA;
            log.info("Finished appending data.");

            checkSegmentStatus(lengths, startOffsets, false, false, expectedAttributeValue, segmentStore);
            log.info("Finished Phase 1");
        }

        // Verify all buffers have been released.
        checkAppendLeaks(appendBuffers);
        appendBuffers.clear();

        log.info("Starting Phase 2.");
        try (val builder = createBuilder(++instanceId, useChunkedStorage);) {
            val segmentStore = builder.createStreamSegmentService();
            for (int id = 1; id < CONTAINER_COUNT; id++) {
                segmentStore.flushToStorage(id, TIMEOUT);
            }
            // Wait for all the data to move to Storage.
            waitForSegmentsInStorage(segmentNames, segmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished waiting for segments in Storage.");

            checkStorage(segmentContents, segmentStore);
            log.info("Finished Storage check.");
            log.info("Finished Phase 2.");
        }

        log.info("Finished.");
    }


    /**
     * Tests an end-to-end scenario for the SegmentStore where operations are continuously executed while the SegmentStore
     * itself is being fenced out by new instances. The difference between this and testEndToEnd() is that this does not
     * do a graceful shutdown of the Segment Store, instead it creates a new instance while the previous one is still running.
     *
     * @throws Exception If an exception occurred.
     */
    @Test(timeout = 120000)
    public void testEndToEndWithFencingWithChunkedStorage() throws Exception {
        endToEndProcessWithFencing(true, true);
    }

    /**
     * End to end test to verify segment store process with fencing.
     *
     * @param verifySegmentContent whether it's needed to read segment content for verification.
     * @param useChunkedSegmentStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
     * @throws Exception If an exception occurred.
     */
    public void endToEndProcessWithFencing(boolean verifySegmentContent, boolean useChunkedSegmentStorage) throws Exception {
        log.info("Starting.");
        ArrayList<SegmentProperties> segmentProperties;
        try (val context = new FencingTestContext()) {
            // Create first instance (this is a one-off so we can bootstrap the test).
            context.createNewInstance(useChunkedSegmentStorage);

            // Create the StreamSegments and their transactions.
            val segmentNames = createSegments(context.getActiveStore());
            val segmentsAndTransactions = new ArrayList<String>(segmentNames);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));

            // Generate all the requests.
            HashMap<String, Long> lengths = new HashMap<>();
            HashMap<String, Long> startOffsets = new HashMap<>();
            HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
            val appends = createAppendDataRequests(segmentsAndTransactions, segmentContents, lengths, null,
                    applyFencingMultiplier(ATTRIBUTE_UPDATES_PER_SEGMENT), applyFencingMultiplier(APPENDS_PER_SEGMENT));
            val requests = appends.iterator();

            // Calculate how frequently to create a new instance of the Segment Store.
            int newInstanceFrequency = appends.size() / applyFencingMultiplier(MAX_INSTANCE_COUNT);
            log.info("Creating a new Segment Store instance every {} operations.", newInstanceFrequency);

            // Execute all the requests.
            val operationCompletions = executeWithFencing(requests, newInstanceFrequency, context, useChunkedSegmentStorage);

            // Wait for our operations to complete.
            operationCompletions.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Wait for the instance creations to be done (this will help surface any exceptions coming from this).
            context.awaitAllInitializations().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (verifySegmentContent) {
                val readonlySegmentStore = context.getActiveStore();
                // Check reads.
                checkReads(segmentContents, readonlySegmentStore);
                log.info("Finished checking reads.");
                waitForSegmentsInStorage(segmentNames, readonlySegmentStore)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                log.info("Finished waiting for segments in Storage.");
            }

            // Delete everything.
            deleteSegments(segmentNames, context.getActiveStore()).join();
            log.info("Finished deleting segments.");
            checkSegmentStatus(lengths, startOffsets, true, true, ATTRIBUTE_UPDATE_DELTA, context.getActiveStore());
        }

        log.info("Finished.");
    }

    //region Helpers

    private ServiceBuilder createBuilder(int instanceId, boolean useChunkedSegmentStorage) throws Exception {
        val builder = createBuilder(this.configBuilder, instanceId, useChunkedSegmentStorage);
        try {
            builder.initialize();
        } catch (Throwable ex) {
            builder.close();
            throw ex;
        }
        return builder;
    }

    /**
     * When overridden in a derived class, creates a ServiceBuilder using the given configuration.
     *
     * @param builderConfig The configuration to use.
     * @param instanceId    The Id of the ServiceBuilder to create. For least interference, these should be unique.
     * @param useChunkedSegmentStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
     * @return The ServiceBuilder.
     */
    protected abstract ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId, boolean useChunkedSegmentStorage);

    private ArrayList<StoreRequest> createAppendDataRequests(
            Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, List<ByteBuf> appendBuffers) {
        return createAppendDataRequests(segmentNames, segmentContents, lengths, appendBuffers, ATTRIBUTE_UPDATES_PER_SEGMENT, APPENDS_PER_SEGMENT);
    }

    private ArrayList<StoreRequest> createAppendDataRequests(
            Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths,
            List<ByteBuf> appendBuffers, int attributeUpdatesPerSegment, int appendsPerSegment) {
        val result = new ArrayList<StoreRequest>();
        val halfAttributeCount = attributeUpdatesPerSegment / 2;
        for (String segmentName : segmentNames) {
            if (isEmptySegment(segmentName)) {
                continue;
            }

            // Add half the attribute updates now.
            for (int i = 0; i < halfAttributeCount; i++) {
                result.add(store -> store.updateAttributes(segmentName, createAttributeUpdates(), TIMEOUT));
            }

            // Add some appends.
            for (int i = 0; i < appendsPerSegment; i++) {
                byte[] appendData = getAppendData(segmentName, i);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                // Use Netty ByteBuf here - this mimics the behavior of AppendProcessor.
                ByteBuf buf = Unpooled.wrappedBuffer(appendData);
                result.add(store -> Futures.toVoid(store.append(segmentName, new ByteBufWrapper(buf), createAttributeUpdates(), TIMEOUT)));
                if (appendBuffers != null) {
                    appendBuffers.add(buf);
                }
            }

            // Add the rest of the attribute updates.
            for (int i = 0; i < halfAttributeCount; i++) {
                result.add(store -> store.updateAttributes(segmentName, createAttributeUpdates(), TIMEOUT));
            }
        }

        return result;
    }

    private CompletableFuture<Void> appendData(Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents,
                                               HashMap<String, Long> lengths, List<ByteBuf> appendBuffers, StreamSegmentStore store) {
        return execute(createAppendDataRequests(segmentNames, segmentContents, lengths, appendBuffers), store);
    }

    private AttributeUpdateCollection createAttributeUpdates() {
        return ATTRIBUTES.stream()
                .map(id -> new AttributeUpdate(id, AttributeUpdateType.Accumulate, 1))
                .collect(Collectors.toCollection(AttributeUpdateCollection::new));
    }

    private ArrayList<StoreRequest> createMergeTransactionsRequests(
            HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
            HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {

        val result = new ArrayList<StoreRequest>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                result.add(store -> Futures.toVoid(store.mergeStreamSegment(parentName, transactionName, TIMEOUT)));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        return result;
    }

    private CompletableFuture<Void> mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
                                                      HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        return execute(createMergeTransactionsRequests(transactionsBySegment, lengths, segmentContents), store);
    }

    private ArrayList<StoreRequest> createSealSegmentsRequests(Collection<String> segmentNames) {
        val result = new ArrayList<StoreRequest>();
        for (String segmentName : segmentNames) {
            result.add(store -> Futures.toVoid(store.sealStreamSegment(segmentName, TIMEOUT)));
        }
        return result;
    }

    private CompletableFuture<Void> sealSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        return execute(createSealSegmentsRequests(segmentNames), store);
    }

    private CompletableFuture<Void> execute(ArrayList<StoreRequest> requests, StreamSegmentStore store) {
        return Futures.allOf(requests.stream().map(r -> r.apply(store)).collect(Collectors.toList()));
    }

    /**
     * Executes all the requests asynchronously, one by one, on the given FencingTextContext.
     */
    private CompletableFuture<Void> executeWithFencing(Iterator<StoreRequest> requests, int newInstanceFrequency, FencingTestContext context, boolean useChunkedSegmentStorage) {
        AtomicInteger index = new AtomicInteger();
        return Futures.loop(
                requests::hasNext,
                () -> {
                    // Create a new Segment Store instance if we need to.
                    if (index.incrementAndGet() % newInstanceFrequency == 0) {
                        context.createNewInstanceAsync(useChunkedSegmentStorage);
                    }

                    return executeWithFencing(requests.next(), index.get(), context, useChunkedSegmentStorage);
                },
                executorService());
    }

    /**
     * Executes the given request on the given FencingTextContext.. We retry all expected exceptions, and when we do, we
     * make sure to execute them on the current (active) Segment Store instance (since the previous one may be unusable).
     * @param useChunkedSegmentStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
     */
    private CompletableFuture<Void> executeWithFencing(StoreRequest request, int index, FencingTestContext context, boolean useChunkedSegmentStorage) {
        log.debug("Initiating Operation #{} on iteration {}.", index, context.getIteration());
        AtomicReference<StreamSegmentStore> requestStore = new AtomicReference<>(context.getActiveStore());
        return Retry.withExpBackoff(50, 2, 10, TIMEOUT.toMillis() / 10)
                .retryWhen(ex -> {
                    requestStore.getAndSet(context.getActiveStore());
                    ex = Exceptions.unwrap(ex);
                    log.info("Operation #{} (Iteration = {}) failed due to {}.", index, context.getIteration(), ex.toString());
                    return isExpectedFencingException(ex);
                })
                .runAsync(() -> request.apply(requestStore.get()), executorService());
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            result.add(store.deleteStreamSegment(segmentName, TIMEOUT));
        }

        return Futures.allOf(result);
    }

    private ArrayList<String> createSegments(StreamSegmentStore store) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createStreamSegment(segmentName, BASIC_SEGMENT_TYPE, null, TIMEOUT));
        }

        futures.add(store.createStreamSegment(EMPTY_SEGMENT_NAME, BASIC_SEGMENT_TYPE, null, TIMEOUT));
        Futures.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, StreamSegmentStore store) {
        // Create the Transactions and collect their names.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        HashMap<String, ArrayList<String>> transactions = new HashMap<>();
        for (String segmentName : segmentNames) {
            if (isEmptySegment(segmentName)) {
                continue;
            }

            val txnList = new ArrayList<String>(TRANSACTIONS_PER_SEGMENT);
            transactions.put(segmentName, txnList);
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                String txnName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                txnList.add(txnName);
                futures.add(store.createStreamSegment(txnName, BASIC_SEGMENT_TYPE, null, TIMEOUT));
            }
        }

        Futures.allOf(futures).join();
        return transactions;
    }

    private boolean isExpectedFencingException(Throwable ex) {
        return ex instanceof DataLogWriterNotPrimaryException
                || ex instanceof IllegalContainerStateException
                || ex instanceof ContainerNotFoundException
                || ex instanceof ObjectClosedException
                || ex instanceof CancellationException;
    }

    private boolean isEmptySegment(String segmentName) {
        return segmentName.equals(EMPTY_SEGMENT_NAME);
    }

    private byte[] getAppendData(String segmentName, int appendId) {
        return String.format("%s_%d", segmentName, appendId).getBytes();
    }

    @SneakyThrows(IOException.class)
    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
        contents.flush();
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private void checkSegmentStatus(HashMap<String, Long> segmentLengths, HashMap<String, Long> startOffsets,
                                    boolean expectSealed, boolean expectDeleted, long expectedAttributeValue, StreamSegmentStore store) {
        for (Map.Entry<String, Long> e : segmentLengths.entrySet()) {
            String segmentName = e.getKey();
            if (expectDeleted) {
                AssertExtensions.assertSuppliedFutureThrows(
                        "Segment '" + segmentName + "' was not deleted.",
                        () -> store.getStreamSegmentInfo(segmentName, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            } else {
                SegmentProperties sp = store.getStreamSegmentInfo(segmentName, TIMEOUT).join();
                long expectedStartOffset = startOffsets.getOrDefault(segmentName, 0L);
                long expectedLength = e.getValue();
                Assert.assertEquals("Unexpected Start Offset for segment " + segmentName, expectedStartOffset, sp.getStartOffset());
                Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
                Assert.assertEquals("Unexpected value for isSealed for segment " + segmentName, expectSealed, sp.isSealed());
                Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());

                // Check attributes.
                val allAttributes = store.getAttributes(segmentName, ATTRIBUTES, true, TIMEOUT).join();
                for (AttributeId attributeId : ATTRIBUTES) {
                    Assert.assertEquals("Unexpected attribute value from getAttributes().",
                            expectedAttributeValue, (long) allAttributes.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));

                    if (Attributes.isCoreAttribute(attributeId)) {
                        // Core attributes must always be available from getInfo
                        Assert.assertEquals("Unexpected core attribute value from getInfo().",
                                expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                    } else {
                        val extAttrValue = sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE);
                        Assert.assertTrue("Unexpected extended attribute value from getInfo()",
                                extAttrValue == Attributes.NULL_ATTRIBUTE_VALUE || extAttrValue == expectedAttributeValue);
                    }
                }
            }
        }
    }

    private void checkReads(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();
            AtomicReference<StreamSegmentInformation> info = new AtomicReference<>((StreamSegmentInformation) store.getStreamSegmentInfo(segmentName, TIMEOUT).join());
            Assert.assertEquals("Unexpected Read Index length for segment " + segmentName, expectedData.length, info.get().getLength());

            AtomicLong expectedCurrentOffset = new AtomicLong(0);

            // We retry a number of times on StreamSegmentNotExists. It is possible that waitForSegmentsInStorage may have
            // returned successfully because it detected the Segment was complete there, but the internal callback to the
            // ReadIndex (completeMerge) may not yet have been executed. The ReadIndex has a mechanism to cope with this,
            // but it only retries once, after a fixed time interval, which is more than generous on any system.
            // However, on very slow systems, it is possible that that callback may take a significant amount of time to even
            // begin executing, hence the trying to read data that was merged from a Transaction may result in a spurious
            // StreamSegmentNotExistsException.
            // This is gracefully handled by retries in AppendProcessor and/or Client, but in this case, we simply have to
            // do the retries ourselves, hoping that the callback eventually executes.
            Retry.withExpBackoff(100, 2, 10, TIMEOUT.toMillis() / 5)
                    .retryWhen(ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException || info.get().getLength() != info.get().getStorageLength())
                    .run(() -> {
                        final StreamSegmentInformation latestInfo = (StreamSegmentInformation) store.getStreamSegmentInfo(segmentName, TIMEOUT).join();
                        try {
                            checkSegmentReads(segmentName, expectedCurrentOffset, info.get().getLength(), store, expectedData);
                        } catch (Exception ex2) {
                            log.debug("Exception during checkReads", ex2);
                        }
                        info.set(latestInfo);
                        return null;
                    });
        }
    }

    private void checkSegmentReads(String segmentName, AtomicLong expectedCurrentOffset, long segmentLength, StreamSegmentStore store, byte[] expectedData) throws Exception {
        @Cleanup
        ReadResult readResult = store.read(segmentName, expectedCurrentOffset.get(), (int) (segmentLength - expectedCurrentOffset.get()), TIMEOUT).join();
        Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());
        // A more thorough read check is done in StreamSegmentContainerTests; here we just check if the data was merged correctly.
        while (readResult.hasNext()) {
            ReadResultEntry readEntry = readResult.next();
            AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName,
                    0, readEntry.getRequestedReadLength());
            Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName,
                    expectedCurrentOffset.get(), readEntry.getStreamSegmentOffset());
            if (!readEntry.getContent().isDone()) {
                readEntry.requestContent(TIMEOUT);
            }
            readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName,
                    ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

            BufferView readEntryContents = readEntry.getContent().join();
            byte[] actualData = readEntryContents.getCopy();
            AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset,
                    expectedData, (int) expectedCurrentOffset.get(), actualData, 0, readEntryContents.getLength());
            expectedCurrentOffset.addAndGet(readEntryContents.getLength());
        }
        Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
    }

    private void checkReadsWhileTruncating(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> startOffsets,
                                           StreamSegmentStore store) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();
            long segmentLength = store.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();
            long expectedCurrentOffset = 0;
            boolean truncate = false;

            while (expectedCurrentOffset < segmentLength) {
                @Cleanup
                ReadResult readResult = store.read(segmentName, expectedCurrentOffset, (int) (segmentLength - expectedCurrentOffset), TIMEOUT).join();
                Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

                // We only test the truncation-related pieces here; other read-related checks are done in checkReads.
                while (readResult.hasNext()) {
                    ReadResultEntry readEntry = readResult.next();
                    Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName,
                            expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                    if (!readEntry.getContent().isDone()) {
                        readEntry.requestContent(TIMEOUT);
                    }

                    if (readEntry.getType() == ReadResultEntryType.Truncated) {
                        long startOffset = startOffsets.getOrDefault(segmentName, 0L);
                        // Verify that the Segment actually is truncated beyond this offset.
                        AssertExtensions.assertLessThan("Found Truncated ReadResultEntry but current offset not truncated.",
                                startOffset, readEntry.getStreamSegmentOffset());

                        // Verify the ReadResultEntry cannot be used and throws an appropriate exception.
                        AssertExtensions.assertSuppliedFutureThrows(
                                "ReadEntry.getContent() did not throw for a Truncated entry.",
                                readEntry::getContent,
                                ex -> ex instanceof StreamSegmentTruncatedException);

                        // Verify ReadResult is done.
                        Assert.assertFalse("Unexpected result from ReadResult.hasNext when encountering truncated entry.",
                                readResult.hasNext());

                        // Verify attempting to read at the current offset will return the appropriate entry (and not throw).
                        @Cleanup
                        ReadResult truncatedResult = store.read(segmentName, readEntry.getStreamSegmentOffset(), 1, TIMEOUT).join();
                        val first = truncatedResult.next();
                        Assert.assertEquals("Read request for a truncated offset did not start with a Truncated ReadResultEntryType.",
                                ReadResultEntryType.Truncated, first.getType());

                        // Skip over until the first non-truncated offset.
                        expectedCurrentOffset = Math.max(expectedCurrentOffset, startOffset);
                        continue;
                    }

                    // Non-truncated entry; do the usual verifications.
                    readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName,
                            ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                    BufferView readEntryContents = readEntry.getContent().join();
                    byte[] actualData = readEntryContents.getCopy();
                    AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset,
                            expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                    expectedCurrentOffset += readEntryContents.getLength();

                    // Every other read, determine if we should truncate or not.
                    if (truncate) {
                        long truncateOffset;
                        if (segmentName.hashCode() % 2 == 0) {
                            // Truncate just beyond the current read offset.
                            truncateOffset = Math.min(segmentLength, expectedCurrentOffset + 1);
                        } else {
                            // Truncate half of what we read so far.
                            truncateOffset = Math.min(segmentLength, expectedCurrentOffset / 2 + 1);
                        }

                        startOffsets.put(segmentName, truncateOffset);
                        store.truncateStreamSegment(segmentName, truncateOffset, TIMEOUT).join();
                    }

                    truncate = !truncate;
                }

                Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
            }
        }
    }

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents,
                                     StreamSegmentStore readOnlySegmentStore) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();

            // 1. Deletion status
            StreamSegmentInformation sp = null;
            try {
                sp = (StreamSegmentInformation) readOnlySegmentStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                AssertExtensions.assertSuppliedFutureThrows(
                        "Segment is marked as deleted in SegmentStore but was not deleted in Storage " + segmentName,
                        () -> readOnlySegmentStore.getStreamSegmentInfo(segmentName, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            StreamSegmentInformation storageProps = (StreamSegmentInformation) readOnlySegmentStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Store and Storage for segment " + segmentName,
                    sp.isSealed(), storageProps.isSealedInStorage());

            // 3. Contents.
            StreamSegmentInformation metadataProps = (StreamSegmentInformation) readOnlySegmentStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedData.length,
                    storageProps.getStorageLength());
            byte[] actualData = new byte[expectedData.length];
            int actualLength = 0;
            int expectedLength = actualData.length;

            try {
                @Cleanup
                ReadResult readResult = readOnlySegmentStore.read(segmentName, 0, actualData.length, TIMEOUT).join();
                actualLength = readResult.readRemaining(actualData, TIMEOUT);
            } catch (Exception ex) {
                ex = (Exception) Exceptions.unwrap(ex);
                if (!(ex instanceof StreamSegmentTruncatedException) || metadataProps.getStartOffset() == 0) {
                    // We encountered an unexpected Exception, or a Truncated Segment which was not expected to be truncated.
                    throw ex;
                }

                // Read from the truncated point, except if the whole segment got truncated.
                expectedLength = (int) (storageProps.getLength() - metadataProps.getStartOffset());
                if (metadataProps.getStartOffset() < storageProps.getLength()) {
                    @Cleanup
                    ReadResult readResult = readOnlySegmentStore.read(segmentName, metadataProps.getStartOffset(),
                            expectedLength, TIMEOUT).join();
                    actualLength = readResult.readRemaining(actualData, TIMEOUT);
                }
            }

            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentName,
                    expectedLength, actualLength);
            AssertExtensions.assertArrayEquals("Unexpected data written to storage for segment " + segmentName,
                    expectedData, expectedData.length - expectedLength, actualData, 0, expectedLength);
        }
    }

    private void checkAppendLeaks(ArrayList<ByteBuf> buffers) {
        // Release our reference to these buffers.
        buffers.forEach(ByteBuf::release);

        // Then verify nobody else still holds such a reference.
        Assert.assertTrue("Memory Leak: At least one append buffer did not have its data released.",
                buffers.stream().allMatch(r -> r.refCnt() == 0));
    }

    private ArrayList<SegmentProperties> getStreamSegmentInfoList(Collection<String> segmentNames, StreamSegmentStore baseStore) {
        ArrayList<SegmentProperties> retValue = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = baseStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            retValue.add(sp);
        }

        return retValue;
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames,
                                                             StreamSegmentStore readOnlyStore) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = readOnlyStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, readOnlyStore));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, StreamSegmentStore readOnlyStore) {
        if (sp.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // We want to make sure that both the main segment and its attribute segment have been sync-ed to Storage. In case
        // of the attribute segment, the only thing we can easily do is verify that it has been sealed when the main segment
        // it is associated with has also been sealed.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(sp.getName());
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return Futures.loop(
                tryAgain::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(sp.getName(), timer, readOnlyStore);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, readOnlyStore);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                StreamSegmentInformation storageProps = (StreamSegmentInformation) segInfo.join();
                                StreamSegmentInformation attrProps = (StreamSegmentInformation) attrInfo.join();
                                if (sp.isDeleted()) {
                                    tryAgain.set(!storageProps.isDeletedInStorage());
                                } else if (sp.isSealed()) {
                                    tryAgain.set(!storageProps.isSealedInStorage());
                                } else {
                                    tryAgain.set(sp.getLength() != storageProps.getStorageLength());
                                }

                                if (tryAgain.get() && !timer.hasRemaining()) {
                                    return Futures.<Void>failedFuture(new TimeoutException(
                                            String.format("Segment %s did not complete in Storage in the allotted time. %s ", sp.getName(), segInfo)));
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(100), executorService());
                                }
                            });
                },
                executorService());
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, StreamSegmentStore readOnlyStore) {
        return Futures
                .exceptionallyExpecting(readOnlyStore.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }

    private int applyFencingMultiplier(int originalValue) {
        return (int) Math.round(originalValue * getFencingTestOperationMultiplier());
    }

    //endregion

    //region FencingTestContext

    /**
     * Context for the Fencing test.
     */
    private class FencingTestContext implements AutoCloseable {
        private final Retry.RetryAndThrowConditionally newInstanceRetry =
                Retry.withExpBackoff(20, 2, 20, TIMEOUT.toMillis() / 10)
                        .retryWhen(ex -> Exceptions.unwrap(ex) instanceof DataLogWriterNotPrimaryException);
        private final AtomicReference<StreamSegmentStore> activeStore = new AtomicReference<>();
        private final AtomicInteger iteration = new AtomicInteger();
        private final ArrayList<ServiceBuilder> builders = new ArrayList<>();
        private final AtomicReference<CompletableFuture<Void>> newInstanceCompletions = new AtomicReference<>(CompletableFuture.completedFuture(null));

        @Override
        public void close() {
            log.info("Stopping all instances.");
            this.builders.forEach(ServiceBuilder::close);
        }

        /**
         * Gets a pointer to the active StreamSegmentStore.
         */
        StreamSegmentStore getActiveStore() {
            return this.activeStore.get();
        }

        /**
         * Gets a value representing the current test iteration.
         */
        int getIteration() {
            return this.iteration.get();
        }

        /**
         * Gets a CompletableFuture that, when completed, will indicate that all calls to createNewInstanceAsync() so far
         * will have completed (successfully or not).
         */
        CompletableFuture<Void> awaitAllInitializations() {
            return this.newInstanceCompletions.get();
        }

        /**
         * Same as createNewInstance(), but runs asynchronously, and only after the previous initialization completed.
         * @param useChunkedSegmentStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
         */
        void createNewInstanceAsync(boolean useChunkedSegmentStorage) {
            this.newInstanceCompletions.set(
                    this.newInstanceCompletions.get().thenRunAsync(() -> createNewInstance(useChunkedSegmentStorage), executorService()));
        }

        /**
         * Creates a new Segment Store Instance, with retries.
         * Normally we have the Controller coordinating which instances are the rightful survivors, however in this case
         * we need to simulate some of this behavior ourselves, by being insistent. It is possible that previous instances
         * meddle with the BKLog ZK metadata during the new instance's initialization, causing the new instance to wrongfully
         * assume it's not the rightful survivor. A quick retry solves this problem, as there is no other kind of information
         * available to disambiguate this.
         * @param useChunkedStorage whether to use ChunkedSegmentStorage or instead use AsyncStorageWrapper.
         */
        void createNewInstance(boolean useChunkedStorage) {
            this.newInstanceRetry.run(() -> {
                int instanceId = getIteration() + 1;
                log.info("Starting Instance {}.", instanceId);
                ServiceBuilder b = createBuilder(instanceId, useChunkedStorage);
                this.builders.add(b);
                this.activeStore.set(b.createStreamSegmentService());
                this.iteration.incrementAndGet();
                log.info("Instance {} Started.", instanceId);
                return null;
            });
        }
    }

    //endregion

    @FunctionalInterface
    private interface StoreRequest {
        CompletableFuture<Void> apply(StreamSegmentStore store);
    }
}
