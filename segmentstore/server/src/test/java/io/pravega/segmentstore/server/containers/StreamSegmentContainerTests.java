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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.DynamicAttributeUpdate;
import io.pravega.segmentstore.contracts.DynamicAttributeValue;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.ContainerEventProcessor;
import io.pravega.segmentstore.server.ContainerOfflineException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.ServiceListeners;
import io.pravega.segmentstore.server.TestDurableDataLogFactory;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.Writer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.logs.AttributeIdLengthMismatchException;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.reading.TestReadResultHandler;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.tables.EntrySerializerTests;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.segmentstore.server.tables.WriterTableProcessor;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.SnapshotInfo;
import io.pravega.segmentstore.storage.chunklayer.SystemJournal;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.common.concurrent.ExecutorServiceHelpers.newScheduledThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for StreamSegmentContainer class.
 * These are not really unit tests. They are more like integration/end-to-end tests, since they test a real StreamSegmentContainer
 * using a real DurableLog, real ReadIndex and real StorageWriter - but all against in-memory mocks of Storage and
 * DurableDataLog.
 */
public class StreamSegmentContainerTests extends ThreadPooledTestSuite {
    /**
     * Auto-generated attributes which are not set externally but maintained internally. To ease our testing, we will
     * exclude these from all our checks.
     */
    private static final Collection<AttributeId> AUTO_ATTRIBUTES = Collections.unmodifiableSet(
            Sets.newHashSet(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Attributes.ATTRIBUTE_SEGMENT_TYPE));
    private static final int SEGMENT_COUNT = 100;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int ATTRIBUTE_UPDATES_PER_SEGMENT = 50;
    private static final int CONTAINER_ID = 1234567;
    private static final int EXPECTED_PINNED_SEGMENT_COUNT = 1;
    private static final long EXPECTED_METADATA_SEGMENT_ID = 1L;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final int TEST_TIMEOUT_MILLIS = 100 * 1000;
    private static final int EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT = 250; // Good for majority of tests.
    private static final int EVICTION_SEGMENT_EXPIRATION_MILLIS_LONG = 4 * EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT; // For heavy tests.
    private static final Duration TIMEOUT = Duration.ofMillis(TEST_TIMEOUT_MILLIS);
    private static final SegmentType BASIC_TYPE = SegmentType.STREAM_SEGMENT;
    private static final int EVENT_PROCESSOR_EVENTS_AT_ONCE = 10;
    private static final int EVENT_PROCESSOR_MAX_OUTSTANDING_BYTES = 4 * 1024 * 1024;
    private static final int EVENT_PROCESSOR_TRUNCATE_SIZE_BYTES = 1024;
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
            .with(ContainerConfig.STORAGE_SNAPSHOT_TIMEOUT_SECONDS, 60)
            .with(ContainerConfig.DATA_INTEGRITY_CHECKS_ENABLED, true)
            .build();

    // Create checkpoints every 100 operations or after 10MB have been written, but under no circumstance less frequently than 10 ops.
    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
            .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
            .build();

    // DL config that can be used to force truncations after every operation - this will speed up metadata eviction eligibility.
    private static final DurableLogConfig FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 5)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
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

    private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
            .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 3)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .build();

    private static final WriterConfig INFREQUENT_FLUSH_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1024 * 1024 * 1024)
            .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 3000)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 250000L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 500L)
            .build();

    private static final Duration TIMEOUT_FUTURE = Duration.ofSeconds(1);
    private static final Duration TIMEOUT_EVENT_PROCESSOR_ITERATION = Duration.ofMillis(100);

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT_MILLIS);

    @Override
    protected int getThreadPoolSize() {
        return 2;
    }

    /**
     * Tests the createSegment, append, updateAttributes, read, getSegmentInfo, getActiveSegments.
     */
    @Test
    public void testSegmentRegularOperations() throws Exception {
        final AttributeId attributeAccumulate = AttributeId.randomUUID();
        final AttributeId attributeReplace = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfGreater = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfEquals = AttributeId.randomUUID();
        final AttributeId attributeNoUpdate = AttributeId.randomUUID();
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;

        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        checkActiveSegments(context.container, 0);
        activateAllSegments(segmentNames, context);
        checkActiveSegments(context.container, segmentNames.size());

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        ArrayList<RefCountByteArraySegment> appends = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                val attributeUpdates = new AttributeUpdateCollection();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals,
                        i == 0 ? AttributeUpdateType.Replace : AttributeUpdateType.ReplaceIfEquals, i + 1, i));
                RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                long expectedLength = lengths.getOrDefault(segmentName, 0L) + appendData.getLength();
                val append = (i % 2 == 0) ? context.container.append(segmentName, appendData, attributeUpdates, TIMEOUT) :
                        context.container.append(segmentName, lengths.get(segmentName), appendData, attributeUpdates, TIMEOUT);
                opFutures.add(append.thenApply(length -> {
                    assertEquals(expectedLength, length.longValue());
                    return null;
                }));
                lengths.put(segmentName, expectedLength);
                recordAppend(segmentName, appendData, segmentContents, appends);
            }
        }

        // 2.1 Update some of the attributes.
        for (String segmentName : segmentNames) {
            // Record a one-off update.
            opFutures.add(context.container.updateAttributes(
                    segmentName,
                    AttributeUpdateCollection.from(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, expectedAttributeValue)),
                    TIMEOUT));

            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                val attributeUpdates = new AttributeUpdateCollection();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, APPENDS_PER_SEGMENT + i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, APPENDS_PER_SEGMENT + i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals,
                        APPENDS_PER_SEGMENT + i + 1, APPENDS_PER_SEGMENT + i));
                opFutures.add(context.container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            long expectedLength = lengths.get(segmentName);

            Assert.assertEquals("Unexpected StartOffset for non-truncated segment " + segmentName, 0, sp.getStartOffset());
            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
            Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());

            // Verify all attribute values.
            Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeNoUpdate, Attributes.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeAccumulate, Attributes.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeReplace + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeReplace, Attributes.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeReplaceIfGreater + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeReplaceIfGreater, Attributes.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeReplaceIfEquals + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeReplaceIfEquals, Attributes.NULL_ATTRIBUTE_VALUE));

            val expectedType = getSegmentType(segmentName);
            val actualType = SegmentType.fromAttributes(sp.getAttributes());
            Assert.assertEquals("Unexpected Segment Type.", expectedType, actualType);
        }

        checkActiveSegments(context.container, segmentNames.size());

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

        // 4.1. After we ensured that all data has been ingested and processed, verify that all data buffers have been released.
        checkAppendLeaks(appends);

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Same as testRegularOperations, but truncates the segments as progress is made.
     */
    @Test
    public void testRegularOperationsWithTruncate() throws Exception {
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);

        // 2. Add some appends & truncate as we go.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        ArrayList<RefCountByteArraySegment> appends = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, Long> truncationOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                opFutures.add(Futures.toVoid(context.container.append(segmentName, appendData, null, TIMEOUT)));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, appends);

                long truncateOffset = truncationOffsets.getOrDefault(segmentName, 0L) + appendData.getLength() / 2 + 1;
                truncationOffsets.put(segmentName, truncateOffset);
                opFutures.add(context.container.truncateStreamSegment(segmentName, truncateOffset, TIMEOUT));
            }
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            long expectedStartOffset = truncationOffsets.getOrDefault(segmentName, 0L);
            long expectedLength = lengths.get(segmentName);

            Assert.assertEquals("Unexpected StartOffset for segment " + segmentName, expectedStartOffset, sp.getStartOffset());
            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
            Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());
            Assert.assertEquals("Unexpected Segment Type.", getSegmentType(segmentName), SegmentType.fromAttributes(sp.getAttributes()));
        }

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, truncationOffsets, context);

        // 4.1. After we ensured that all data has been ingested and processed, verify that all data buffers have been released.
        checkAppendLeaks(appends);

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the {@link SegmentContainer#flushToStorage} method.
     */
    @Test
    public void testForceFlush() throws Exception {
        final AttributeId attributeReplace = AttributeId.randomUUID();
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
        final int entriesPerSegment = 10;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, NO_TRUNCATIONS_DURABLE_LOG_CONFIG, INFREQUENT_FLUSH_WRITER_CONFIG, null);
        val durableLog = new AtomicReference<OperationLog>();
        val durableLogFactory = new WatchableOperationLogFactory(context.operationLogFactory, durableLog::set);
        @Cleanup
        val container = new StreamSegmentContainer(CONTAINER_ID, DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container.startAsync().awaitRunning();
        Assert.assertNotNull(durableLog.get());
        val tableStore = container.getExtension(ContainerTableExtension.class);

        // 1. Create the StreamSegments and Table Segments.
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<String> tableSegmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            opFutures.add(container.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT));
        }

        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i) + "_Table";
            tableSegmentNames.add(segmentName);
            val type = SegmentType.builder(getSegmentType(segmentName)).tableSegment().build();
            opFutures.add(tableStore.createSegment(segmentName, type, TIMEOUT));
        }

        // 1.1 Wait for all segments to be created prior to using them.
        Futures.allOf(opFutures).join();
        opFutures.clear();

        // 2. Add some appends and update some of the attributes.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
                val attributeUpdates = AttributeUpdateCollection.from(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, i + 1));
                val appendData = getAppendData(segmentName, i);
                long expectedLength = lengths.getOrDefault(segmentName, 0L) + appendData.getLength();
                val append = (i % 2 == 0) ? container.append(segmentName, appendData, attributeUpdates, TIMEOUT) :
                        container.append(segmentName, lengths.get(segmentName), appendData, attributeUpdates, TIMEOUT);
                opFutures.add(Futures.toVoid(append));
                lengths.put(segmentName, expectedLength);
                recordAppend(segmentName, appendData, segmentContents, null);
            }

            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                val attributeUpdates = AttributeUpdateCollection.from(
                        new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, APPENDS_PER_SEGMENT + i + 1));
                opFutures.add(container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }
        }

        // 2.2 Add some entries to the table segments.
        final BiFunction<String, Integer, TableEntry> createTableEntry = (segmentName, entryId) ->
                TableEntry.unversioned(new ByteArraySegment(String.format("Key_%s_%s", segmentName, entryId).getBytes()),
                        new ByteArraySegment(String.format("Value_%s_%s", segmentName, entryId).getBytes()));
        for (String segmentName : tableSegmentNames) {
            for (int i = 0; i < entriesPerSegment; i++) {
                opFutures.add(Futures.toVoid(tableStore.put(segmentName, Collections.singletonList(createTableEntry.apply(segmentName, i)), TIMEOUT)));
            }
        }
        Futures.allOf(opFutures).join();

        // 3. Instead of waiting for the Writer to move data to Storage, we invoke the flushToStorage to verify that all
        // operations have been applied to Storage.
        val forceFlush = container.flushToStorage(TIMEOUT);
        forceFlush.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, container, context.storage);

        // 4. Truncate all the data in the DurableLog and immediately shut down the container.
        val truncateSeqNo = container.metadata.getClosestValidTruncationPoint(container.metadata.getOperationSequenceNumber());
        durableLog.get().truncate(truncateSeqNo, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        container.close();

        // 5. Create a new container instance (from the nearly empty DurableLog) and with an empty cache.
        @Cleanup
        val container2 = new StreamSegmentContainer(CONTAINER_ID, DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container2.startAsync().awaitRunning();

        // 5.1 Verify Segment Data.
        for (val sc : segmentContents.entrySet()) {
            // Contents.
            byte[] expectedData = sc.getValue().toByteArray();
            byte[] actualData = new byte[expectedData.length];
            container2.read(sc.getKey(), 0, actualData.length, TIMEOUT).join().readRemaining(actualData, TIMEOUT);
            Assert.assertArrayEquals("Unexpected contents for " + sc.getKey(), expectedData, actualData);

            // Length.
            val si = container2.getStreamSegmentInfo(sc.getKey(), TIMEOUT).join();
            Assert.assertEquals("Unexpected length for " + sc.getKey(), expectedData.length, si.getLength());

            // Attributes.
            val attributes = container2.getAttributes(sc.getKey(), Collections.singleton(attributeReplace), false, TIMEOUT).join();
            Assert.assertEquals("Unexpected attribute for " + sc.getKey(), expectedAttributeValue, (long) attributes.get(attributeReplace));
        }

        // 5.2 Verify table segment data.
        val tableStore2 = container2.getExtension(ContainerTableExtension.class);
        for (String segmentName : tableSegmentNames) {
            for (int i = 0; i < entriesPerSegment; i++) {
                val expected = createTableEntry.apply(segmentName, i);
                val actual = tableStore2.get(segmentName, Collections.singletonList(expected.getKey().getKey()), TIMEOUT)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                        .get(0);
                Assert.assertTrue("Unexpected Table Entry for " + segmentName + " at position " + i,
                        expected.getKey().getKey().equals(actual.getKey().getKey()) && expected.getValue().equals(actual.getValue()));
            }
        }

        // Ending Note: if all the above tests passed, we have implicitly validated that the Container Metadata Segment has also
        // been properly flushed.
    }

    /**
     * Tests the ability to set attributes (via append() or updateAttributes()), then fetch them back using getAttributes(),
     * emphasizing on Extended Attributes that are dumped into Storage and cleared from memory.
     */
    @Test
    public void testAttributes() throws Exception {
        final AttributeId coreAttribute = Attributes.EVENT_COUNT;
        final int variableAttributeIdLength = 4;
        final List<AttributeId> extendedAttributesUUID = Arrays.asList(AttributeId.randomUUID(), AttributeId.randomUUID());
        final List<AttributeId> extendedAttributesVariable = Arrays.asList(AttributeId.random(variableAttributeIdLength), AttributeId.random(variableAttributeIdLength));
        final List<AttributeId> allAttributesWithUUID = Stream.concat(extendedAttributesUUID.stream(), Stream.of(coreAttribute)).collect(Collectors.toList());
        final List<AttributeId> allAttributesWithVariable = Stream.concat(extendedAttributesVariable.stream(), Stream.of(coreAttribute)).collect(Collectors.toList());
        final AttributeId segmentLengthAttributeUUID = AttributeId.randomUUID();
        final AttributeId segmentLengthAttributeVariable = AttributeId.random(variableAttributeIdLength);
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));
        containerConfig.setMaxCachedExtendedAttributeCount(SEGMENT_COUNT * allAttributesWithUUID.size());

        @Cleanup
        TestContext context = createContext();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        val segmentNames = IntStream.range(0, SEGMENT_COUNT).boxed()
                .collect(Collectors.toMap(StreamSegmentContainerTests::getSegmentName, i -> i % 2 == 0 ? variableAttributeIdLength : 0));
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (val sn : segmentNames.entrySet()) {
            opFutures.add(localContainer.createStreamSegment(
                sn.getKey(), SegmentType.STREAM_SEGMENT,
                AttributeUpdateCollection.from(new AttributeUpdate(Attributes.ATTRIBUTE_ID_LENGTH, AttributeUpdateType.None, sn.getValue())),
                TIMEOUT));
        }
        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Predicate<Map.Entry<String, Integer>> isUUIDOnly = e -> e.getValue() == 0;

        // 2. Add some appends.
        for (val sn : segmentNames.entrySet()) {
            boolean isUUID = isUUIDOnly.test(sn);
            for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
                AttributeUpdateCollection attributeUpdates = (isUUID ? allAttributesWithUUID : allAttributesWithVariable)
                        .stream()
                        .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                        .collect(Collectors.toCollection(AttributeUpdateCollection::new));
                opFutures.add(Futures.toVoid(localContainer.append(sn.getKey(), getAppendData(sn.getKey(), i), attributeUpdates, TIMEOUT)));
            }
        }

        // 2.1 Update some of the attributes.
        for (val sn : segmentNames.entrySet()) {
            boolean isUUID = isUUIDOnly.test(sn);
            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                AttributeUpdateCollection attributeUpdates = (isUUID ? allAttributesWithUUID : allAttributesWithVariable)
                        .stream()
                        .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                        .collect(Collectors.toCollection(AttributeUpdateCollection::new));
                opFutures.add(localContainer.updateAttributes(sn.getKey(), attributeUpdates, TIMEOUT));
            }

            // Verify that we are not allowed to update attributes of the wrong type.
            val badUpdate = new AttributeUpdate(isUUID ? AttributeId.random(variableAttributeIdLength) : AttributeId.randomUUID(),
                    AttributeUpdateType.Accumulate, 1);
            AssertExtensions.assertSuppliedFutureThrows(
                    "updateAttributes allowed updating attributes with wrong type and/or length.",
                    () -> localContainer.updateAttributes(sn.getKey(), AttributeUpdateCollection.from(badUpdate), TIMEOUT),
                    ex -> ex instanceof AttributeIdLengthMismatchException);
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2.2 Dynamic attributes.
        for (val sn : segmentNames.entrySet()) {
            boolean isUUID = isUUIDOnly.test(sn);

            val dynamicId = isUUID ? segmentLengthAttributeUUID : segmentLengthAttributeVariable;
            val dynamicAttributes = AttributeUpdateCollection.from(new DynamicAttributeUpdate(dynamicId, AttributeUpdateType.Replace, DynamicAttributeValue.segmentLength(10)));
            val appendData = getAppendData(sn.getKey(), 1000);
            val lastOffset = localContainer.append(sn.getKey(), appendData, dynamicAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            val expectedValue = lastOffset - appendData.getLength() + 10;

            Assert.assertEquals(expectedValue, (long) localContainer.getAttributes(sn.getKey(), Collections.singleton(dynamicId), false, TIMEOUT)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get(dynamicId));
        }

        // 3. getSegmentInfo
        for (val sn : segmentNames.entrySet()) {
            val segmentName = sn.getKey();
            val allAttributes = isUUIDOnly.test(sn) ? allAttributesWithUUID : allAttributesWithVariable;
            val allAttributeValues = localContainer.getAttributes(segmentName, allAttributes, false, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of attributes retrieved via getAttributes().", allAttributes.size(), allAttributeValues.size());

            // Verify all attribute values.
            SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            for (val attributeId : allAttributes) {
                Assert.assertEquals("Unexpected value for attribute " + attributeId + " via getInfo() for segment " + segmentName,
                        expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                Assert.assertEquals("Unexpected value for attribute " + attributeId + " via getAttributes() for segment " + segmentName,
                        expectedAttributeValue, (long) allAttributeValues.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
            }

            // Verify we can't request wrong lengths/types.
            val badId = isUUIDOnly.test(sn) ? AttributeId.random(variableAttributeIdLength) : AttributeId.randomUUID();
            AssertExtensions.assertSuppliedFutureThrows(
                    "getAttributes allowed getting attributes with wrong type and/or length.",
                    () -> localContainer.getAttributes(segmentName, Collections.singleton(badId), true, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);
        }

        // Force these segments out of memory, so that we may verify that extended attributes are still recoverable.
        localContainer.triggerMetadataCleanup(segmentNames.keySet()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        for (val sn : segmentNames.entrySet()) {
            val segmentName = sn.getKey();
            val allAttributes = isUUIDOnly.test(sn) ? allAttributesWithUUID : allAttributesWithVariable;
            val allAttributeValues = localContainer.getAttributes(segmentName, allAttributes, false, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of attributes retrieved via getAttributes() after recovery for segment " + segmentName,
                    allAttributes.size(), allAttributeValues.size());

            // Verify all attribute values. Core attributes should still be loaded in memory, while extended attributes can
            // only be fetched via their special API.
            SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            for (val attributeId : allAttributes) {
                Assert.assertEquals("Unexpected value for attribute " + attributeId + " via getAttributes() after recovery for segment " + segmentName,
                        expectedAttributeValue, (long) allAttributeValues.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                if (Attributes.isCoreAttribute(attributeId)) {
                    Assert.assertEquals("Expecting core attribute to be loaded in memory.",
                            expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                } else {
                    Assert.assertEquals("Not expecting extended attribute to be loaded in memory.",
                            Attributes.NULL_ATTRIBUTE_VALUE, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                }
            }

            // Now instruct the Container to cache missing values (do it a few times so we make sure it's idempotent).
            // Also introduce some random new attribute to fetch. We want to make sure we can properly handle caching
            // missing attribute values.
            val missingAttributeId = isUUIDOnly.test(sn) ? AttributeId.randomUUID() : AttributeId.random(variableAttributeIdLength);
            val attributesToCache = new ArrayList<>(allAttributes);
            attributesToCache.add(missingAttributeId);
            val attributesToCacheValues = new HashMap<>(allAttributeValues);
            attributesToCacheValues.put(missingAttributeId, Attributes.NULL_ATTRIBUTE_VALUE);
            Map<AttributeId, Long> allAttributeValuesWithCache;
            for (int i = 0; i < 2; i++) {
                allAttributeValuesWithCache = localContainer.getAttributes(segmentName, attributesToCache, true, TIMEOUT).join();
                AssertExtensions.assertMapEquals("Inconsistent results from getAttributes(cache=true, attempt=" + i + ").",
                        attributesToCacheValues, allAttributeValuesWithCache);
                sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).join();
                for (val attributeId : allAttributes) {
                    Assert.assertEquals("Expecting all attributes to be loaded in memory.",
                            expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                }

                Assert.assertEquals("Unexpected value for missing Attribute Id",
                        Attributes.NULL_ATTRIBUTE_VALUE, (long) sp.getAttributes().get(missingAttributeId));
            }
        }

        // 4. Make an update, then immediately seal the segment, then verify the update updated the root pointer.
        AttributeId attr = Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER;
        val oldRootPointers = new HashMap<String, Long>();
        for (val sn : segmentNames.entrySet()) {
            val segmentName = sn.getKey();
            val newAttributeId = isUUIDOnly.test(sn) ? AttributeId.randomUUID() : AttributeId.random(variableAttributeIdLength);

            // Get the old root pointer, then make a random attribute update, then immediately seal the segment.
            localContainer.getAttributes(segmentName, Collections.singleton(attr), false, TIMEOUT)
                    .thenCompose(values -> {
                        oldRootPointers.put(segmentName, values.get(attr));
                        return CompletableFuture.allOf(
                                localContainer.updateAttributes(segmentName, AttributeUpdateCollection.from(new AttributeUpdate(newAttributeId, AttributeUpdateType.Replace, 1L)), TIMEOUT),
                                localContainer.sealStreamSegment(segmentName, TIMEOUT));
                    }).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // We don't know the right value for Root Pointer, but we want to make sure it was increased (after the seal),
        // which indicates the StorageWriter was able to successfully record it after its final Attribute Index update.
        for (String segmentName : segmentNames.keySet()) {
            Long oldValue = oldRootPointers.get(segmentName);
            TestUtils.await(() -> {
                        val newVal = localContainer.getAttributes(segmentName, Collections.singleton(attr), false, TIMEOUT).join().get(attr);
                        return oldValue < newVal;
                    },
                    10,
                    TIMEOUT.toMillis());
        }
        waitForSegmentsInStorage(segmentNames.keySet(), localContainer, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to run attribute iterators over all or a subset of attributes in a segment.
     */
    @Test
    public void testAttributeIterators() throws Exception {
        final List<AttributeId> sortedAttributes = IntStream.range(0, 100).mapToObj(i -> AttributeId.uuid(i, i)).sorted().collect(Collectors.toList());
        final Map<AttributeId, Long> expectedValues = sortedAttributes.stream().collect(Collectors.toMap(id -> id, id -> id.getBitGroup(0)));
        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // 1. Create the Segment.
        String segmentName = getSegmentName(0);
        localContainer.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val segment1 = localContainer.forSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2. Set some initial attribute values and verify in-memory iterator.
        AttributeUpdateCollection attributeUpdates = sortedAttributes
                .stream()
                .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Replace, expectedValues.get(attributeId)))
                .collect(Collectors.toCollection(AttributeUpdateCollection::new));
        segment1.updateAttributes(attributeUpdates, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment1, sortedAttributes, expectedValues);

        // 3. Force these segments out of memory and verify out-of-memory iterator.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val segment2 = localContainer.forSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment2, sortedAttributes, expectedValues);

        // 4. Update some values, and verify mixed iterator.
        attributeUpdates.clear();
        for (int i = 0; i < sortedAttributes.size(); i += 3) {
            AttributeId attributeId = sortedAttributes.get(i);
            expectedValues.put(attributeId, expectedValues.get(attributeId) + 1);
            attributeUpdates.add(new AttributeUpdate(attributeId, AttributeUpdateType.Replace, expectedValues.get(attributeId)));
        }
        segment2.updateAttributes(attributeUpdates, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment2, sortedAttributes, expectedValues);

        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * The below test validates a case where the container does not bail out
     * post container recovery where the StorageWriter has to deal with an Operation
     * on a segment that was originally evicted out(not present in metadata) yet
     * the Operation still exists in the BK Log and read during recovery and hence
     * processed by StorageWriter.
     * @throws Exception in case of exception.
     */
    @Test(timeout = 10000)
    public void testWritingEvictedSegmentOperations() throws Exception {
        final String segmentName = "segment";
        final ByteArraySegment appendData = new ByteArraySegment("hello".getBytes());

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext(containerConfig);
        val localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        AtomicReference<OperationLog> durableLog = new AtomicReference<>();

        val watchableOperationLogFactory = new WatchableOperationLogFactory(localDurableLogFactory, durableLog::set);
        try (val container1 = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, watchableOperationLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService())) {
            container1.startAsync().awaitRunning();

            // Create segment and make one append to it.
            container1.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.append(segmentName, appendData, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            val metadataId = container1.metadata.getStreamSegmentId(segmentName, true);
            // Wait until the segment is forgotten.
            container1.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Add a read so the segment is re-instated.
            container1.read(segmentName, 0, 5, TIMEOUT).join();

            container1.deleteStreamSegment(segmentName, TIMEOUT).join();

            // Simulate segment eviction in metadata
            StreamSegmentMetadata met = new TestStreamSegmentMetadata(segmentName, metadataId, container1.getId());
            val segmentMetadata = container1.metadata.getStreamSegmentMetadata(metadataId);
            met.copyFrom(segmentMetadata);
            met.setLastUsed(0);
            Collection<SegmentMetadata> evictSegment = List.of(met);
            container1.metadata.cleanup(evictSegment, container1.metadata.getOperationSequenceNumber());
            // Generate a checkpoint without the segment metadata.
            durableLog.get().checkpoint(TIMEOUT).join();
            container1.stopAsync().awaitTerminated();
        }

        // Restart container and verify it started successfully.
        @Cleanup
        val container2 = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container2.startAsync().awaitRunning();
        container2.flushToStorage(TIMEOUT).join();
        container2.stopAsync().awaitTerminated();
    }

    /**
     * Test conditional updates for Extended Attributes when they are not loaded in memory (i.e., they will need to be
     * auto-fetched from the AttributeIndex so that the operation may succeed).
     */
    @Test
    public void testExtendedAttributesConditionalUpdates() throws Exception {
        final AttributeId ea1 = AttributeId.uuid(0, 1);
        final AttributeId ea2 = AttributeId.uuid(0, 2);
        final List<AttributeId> allAttributes = Stream.of(ea1, ea2).collect(Collectors.toList());

        // We set a longer Segment Expiration time, since this test executes more operations than the others.
        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_LONG));
        AtomicInteger expectedAttributeValue = new AtomicInteger(0);

        @Cleanup
        TestContext context = createContext();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // 1. Create the StreamSegments and set the initial attribute values.
        ArrayList<String> segmentNames = createSegments(localContainer);
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();

        // 2. Update some of the attributes.
        expectedAttributeValue.set(1);
        for (String segmentName : segmentNames) {
            AttributeUpdateCollection attributeUpdates = allAttributes
                    .stream()
                    .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                    .collect(Collectors.toCollection(AttributeUpdateCollection::new));
            opFutures.add(localContainer.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Force these segments out of memory, so that we may verify conditional appends/updates on extended attributes.
        localContainer.triggerMetadataCleanup(segmentNames).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Execute various conditional operations using these attributes as comparison.
        long compare = expectedAttributeValue.getAndIncrement();
        long set = expectedAttributeValue.get();
        boolean badUpdate = false;
        for (String segmentName : segmentNames) {
            if (badUpdate) {
                // For every other segment, try to do a bad update, then a good one. This helps us verify both direct
                // conditional operations, failed conditional operations and conditional operations with cached attributes.
                AssertExtensions.assertSuppliedFutureThrows(
                        "Conditional append succeeded with incorrect compare value.",
                        () -> localContainer.append(segmentName, getAppendData(segmentName, 0),
                                AttributeUpdateCollection.from(new AttributeUpdate(ea1, AttributeUpdateType.ReplaceIfEquals, set, compare - 1)), TIMEOUT),
                        ex -> (ex instanceof BadAttributeUpdateException) && !((BadAttributeUpdateException) ex).isPreviousValueMissing());
                AssertExtensions.assertSuppliedFutureThrows(
                        "Conditional update-attributes succeeded with incorrect compare value.",
                        () -> localContainer.updateAttributes(segmentName,
                                AttributeUpdateCollection.from(new AttributeUpdate(ea2, AttributeUpdateType.ReplaceIfEquals, set, compare - 1)), TIMEOUT),
                        ex -> (ex instanceof BadAttributeUpdateException) && !((BadAttributeUpdateException) ex).isPreviousValueMissing());

            }

            opFutures.add(Futures.toVoid(localContainer.append(segmentName, getAppendData(segmentName, 0),
                    AttributeUpdateCollection.from(new AttributeUpdate(ea1, AttributeUpdateType.ReplaceIfEquals, set, compare)), TIMEOUT)));
            opFutures.add(localContainer.updateAttributes(segmentName,
                    AttributeUpdateCollection.from(new AttributeUpdate(ea2, AttributeUpdateType.ReplaceIfEquals, set, compare)), TIMEOUT));
            badUpdate = !badUpdate;
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Evict the segment from memory, then verify results.
        localContainer.triggerMetadataCleanup(segmentNames).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (String segmentName : segmentNames) {
            // Verify all attribute values.
            val attributeValues = localContainer.getAttributes(segmentName, allAttributes, true, TIMEOUT).join();
            val sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            for (val attributeId : allAttributes) {
                Assert.assertEquals("Unexpected value for non-cached attribute " + attributeId + " for segment " + segmentName,
                        expectedAttributeValue.get(), (long) attributeValues.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                Assert.assertEquals("Unexpected value for metadata attribute " + attributeId + " for segment " + segmentName,
                        expectedAttributeValue.get(), (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
            }
        }

        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability for the StreamSegmentContainer to handle concurrent actions on a Segment that it does not know
     * anything about, and handling the resulting concurrency.
     * Note: this is tested with a single segment. It could be tested with multiple segments, but different segments
     * are mostly independent of each other, so we would not be gaining much by doing so.
     */
    @Test
    public void testConcurrentSegmentActivation() throws Exception {
        final AttributeId attributeAccumulate = AttributeId.randomUUID();
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
        final int appendLength = 10;

        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        String segmentName = createSegments(context).get(0);

        // 2. Add some appends.
        List<CompletableFuture<Void>> opFutures = Collections.synchronizedList(new ArrayList<>());
        AtomicLong expectedLength = new AtomicLong();

        @Cleanup("shutdown")
        ExecutorService testExecutor = newScheduledThreadPool(Math.min(20, APPENDS_PER_SEGMENT), "testConcurrentSegmentActivation");
        val submitFutures = new ArrayList<Future<?>>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            final byte fillValue = (byte) i;
            submitFutures.add(testExecutor.submit(() -> {
                val attributeUpdates = AttributeUpdateCollection.from(
                        new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                byte[] appendData = new byte[appendLength];
                Arrays.fill(appendData, (byte) (fillValue + 1));
                opFutures.add(Futures.toVoid(context.container.append(segmentName, new ByteArraySegment(appendData), attributeUpdates, TIMEOUT)));
                expectedLength.addAndGet(appendData.length);
            }));
        }

        // 2.1 Update the attribute.
        for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
            submitFutures.add(testExecutor.submit(() -> {
                AttributeUpdateCollection attributeUpdates = AttributeUpdateCollection.from(
                        new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                opFutures.add(context.container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }));
        }

        // Wait for the submittal of tasks to complete.
        submitFutures.forEach(this::await);

        // Now wait for all the appends to finish.
        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo: verify final state of the attribute.
        SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();

        Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength.get(), sp.getLength());
        Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
        Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());
        Assert.assertEquals("Unexpected Segment Type.", getSegmentType(segmentName), SegmentType.fromAttributes(sp.getAttributes()));

        // Verify all attribute values.
        Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeAccumulate, Attributes.NULL_ATTRIBUTE_VALUE));

        checkActiveSegments(context.container, 1);

        // 4. Written data.
        waitForOperationsInReadIndex(context.container);
        byte[] actualData = new byte[(int) expectedLength.get()];
        int offset = 0;
        @Cleanup
        ReadResult readResult = context.container.read(segmentName, 0, actualData.length, TIMEOUT).join();
        while (readResult.hasNext()) {
            ReadResultEntry readEntry = readResult.next();
            BufferView readEntryContents = readEntry.getContent().join();
            AssertExtensions.assertLessThanOrEqual("Too much to read.", actualData.length, offset + actualData.length);
            readEntryContents.copyTo(ByteBuffer.wrap(actualData, offset, actualData.length));
            offset += actualData.length;
        }

        Assert.assertEquals("Unexpected number of bytes read.", actualData.length, offset);
        Assert.assertTrue("Unexpected number of bytes read (multiple of appendLength).", actualData.length % appendLength == 0);

        boolean[] observedValues = new boolean[APPENDS_PER_SEGMENT + 1];
        for (int i = 0; i < actualData.length; i += appendLength) {
            byte value = actualData[i];
            Assert.assertFalse("Append with value " + value + " was written multiple times.", observedValues[value]);
            observedValues[value] = true;
            for (int j = 1; j < appendLength; j++) {
                Assert.assertEquals("Append was not written atomically at offset " + (i + j), value, actualData[i + j]);
            }
        }

        // Verify all the appends made it (we purposefully did not write 0, since that's the default fill value in an array).
        Assert.assertFalse("Not expecting 0 as a value.", observedValues[0]);
        for (int i = 1; i < observedValues.length; i++) {
            Assert.assertTrue("Append with value " + i + " was not written.", observedValues[i]);
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to make appends with offset.
     */
    @Test
    public void testAppendWithOffset() throws Exception {
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        activateAllSegments(segmentNames, context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        ArrayList<RefCountByteArraySegment> appends = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        int appendId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                long offset = lengths.getOrDefault(segmentName, 0L);
                appendFutures.add(context.container.append(segmentName, offset, appendData, null, TIMEOUT));

                lengths.put(segmentName, offset + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, appends);
                appendId++;
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2.1 Verify that if we pass wrong offsets, the append is failed.
        for (String segmentName : segmentNames) {
            RefCountByteArraySegment appendData = getAppendData(segmentName, appendId);
            long offset = lengths.get(segmentName) + (appendId % 2 == 0 ? 1 : -1);

            AssertExtensions.assertSuppliedFutureThrows(
                    "append did not fail with the appropriate exception when passed a bad offset.",
                    () -> context.container.append(segmentName, offset, appendData, null, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Verify that failed appends have their buffers released.
            checkAppendLeaks(Collections.singletonList(appendData));
            appendId++;
        }

        // 3. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

        // 3.1. After we ensured that all data has been ingested and processed, verify that all data buffers have been released.
        checkAppendLeaks(appends);

        // 4. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the seal operation on StreamSegments. Also tests the behavior of Reads (non-tailing) when encountering
     * the end of a sealed StreamSegment.
     */
    @Test
    public void testSegmentSeal() throws Exception {
        final int appendsPerSegment = 1;
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();

        for (String segmentName : segmentNames) {
            ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
            segmentContents.put(segmentName, segmentStream);
            for (int i = 0; i < appendsPerSegment; i++) {
                ByteArraySegment appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                appendData.copyTo(segmentStream);
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Seal first half of segments.
        ArrayList<CompletableFuture<Long>> sealFutures = new ArrayList<>();
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            sealFutures.add(context.container.sealStreamSegment(segmentNames.get(i), TIMEOUT));
        }

        Futures.allOf(sealFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Check that the segments were properly sealed.
        for (int i = 0; i < segmentNames.size(); i++) {
            String segmentName = segmentNames.get(i);
            boolean expectedSealed = i < segmentNames.size() / 2;
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            if (expectedSealed) {
                Assert.assertTrue("Segment is not sealed when it should be " + segmentName, sp.isSealed());
                Assert.assertEquals("Unexpected result from seal() future for segment " + segmentName, sp.getLength(), (long) sealFutures.get(i).join());
                AssertExtensions.assertThrows(
                        "Container allowed appending to a sealed segment " + segmentName,
                        context.container.append(segmentName, new ByteArraySegment("foo".getBytes()), null, TIMEOUT)::join,
                        ex -> ex instanceof StreamSegmentSealedException);
            } else {
                Assert.assertFalse("Segment is sealed when it shouldn't be " + segmentName, sp.isSealed());

                // Verify we can still append to these segments.
                byte[] appendData = "foo".getBytes();
                context.container.append(segmentName, new ByteArraySegment(appendData), null, TIMEOUT).join();
                segmentContents.get(segmentName).write(appendData);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
            }
        }

        // 4. Reads (regular reads, not tail reads, and only for the sealed segments).
        waitForOperationsInReadIndex(context.container);
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            String segmentName = segmentNames.get(i);
            long segmentLength = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();

            // Read starting 1 byte from the end - make sure it wont hang at the end by turning into a future read.
            final int totalReadLength = 1;
            long expectedCurrentOffset = segmentLength - totalReadLength;
            @Cleanup
            ReadResult readResult = context.container.read(segmentName, expectedCurrentOffset, Integer.MAX_VALUE, TIMEOUT).join();

            int readLength = 0;
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                if (readEntry.getStreamSegmentOffset() >= segmentLength) {
                    Assert.assertEquals(
                        "Unexpected value for isEndOfStreamSegment when reaching the end of sealed segment "
                                + segmentName,
                        ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    AssertExtensions.assertSuppliedFutureThrows(
                            "ReadResultEntry.getContent() returned a result when reached the end of sealed segment " + segmentName,
                            readEntry::getContent,
                            ex -> ex instanceof IllegalStateException);
                } else {
                    Assert.assertNotEquals(
                        "Unexpected value for isEndOfStreamSegment before reaching end of sealed segment " + segmentName,
                        ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    Assert.assertTrue(
                        "getContent() did not return a completed future for segment" + segmentName,
                        readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                    BufferView readEntryContents = readEntry.getContent().join();
                    expectedCurrentOffset += readEntryContents.getLength();
                    readLength += readEntryContents.getLength();
                }
            }

            Assert.assertEquals("Unexpected number of bytes read.", totalReadLength, readLength);
            Assert.assertTrue("ReadResult was not closed when reaching the end of sealed segment" + segmentName, readResult.isClosed());
        }

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the behavior of various operations when the StreamSegment does not exist.
     */
    @Test
    public void testInexistentSegment() {
        final String segmentName = "foo";
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        AssertExtensions.assertThrows(
                "getStreamSegmentInfo did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.getStreamSegmentInfo(segmentName, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "append did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.append(segmentName, new ByteArraySegment("foo".getBytes()), null, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "read did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.read(segmentName, 0, 1, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "delete did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.deleteStreamSegment(segmentName, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests the ability to delete StreamSegments.
     */
    @Test
    public void testSegmentDelete() throws Exception {
        final int appendsPerSegment = 1;
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        val emptySegmentName = segmentNames.get(0);
        val nonEmptySegmentNames = segmentNames.subList(1, segmentNames.size() - 1);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();

        for (int i = 0; i < appendsPerSegment; i++) {
            // Append to all but the "empty" segment.
            for (String segmentName : nonEmptySegmentNames) {
                appendFutures.add(context.container.append(segmentName, getAppendData(segmentName, i), null, TIMEOUT));
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Delete the empty segment (twice).
        context.container.deleteStreamSegment(emptySegmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertSuppliedFutureThrows(
                "Empty segment was not deleted.",
                () -> context.container.deleteStreamSegment(emptySegmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
        checkNotExistsInStorage(emptySegmentName, context);

        // 3.1. Delete the first half of the segments.
        ArrayList<CompletableFuture<Void>> deleteFutures = new ArrayList<>();
        for (int i = 0; i < nonEmptySegmentNames.size() / 2; i++) {
            String segmentName = nonEmptySegmentNames.get(i);
            deleteFutures.add(context.container.deleteStreamSegment(segmentName, TIMEOUT));
        }

        Futures.allOf(deleteFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3.2. Verify that only the first half of the segments were deleted, and not the others.
        for (int i = 0; i < nonEmptySegmentNames.size(); i++) {
            ArrayList<String> toCheck = new ArrayList<>();
            toCheck.add(nonEmptySegmentNames.get(i));

            boolean expectedDeleted = i < nonEmptySegmentNames.size() / 2;
            if (expectedDeleted) {
                // Verify the segments and their Transactions are not there anymore.
                for (String sn : toCheck) {
                    AssertExtensions.assertThrows(
                            "getStreamSegmentInfo did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.getStreamSegmentInfo(sn, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "append did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.append(sn, new ByteArraySegment("foo".getBytes()), null, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "read did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.read(sn, 0, 1, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    checkNotExistsInStorage(sn, context);
                    //Assert.assertFalse("Segment not deleted in storage.", context.storage.exists(sn, TIMEOUT).join());
                }
            } else {
                // Verify the Segments are still there.
                for (String sn : toCheck) {
                    SegmentProperties props = context.container.getStreamSegmentInfo(sn, TIMEOUT).join();
                    Assert.assertFalse("Not-deleted segment was marked as deleted in metadata.", props.isDeleted());

                    // Verify we can still append and read from this segment.
                    context.container.append(sn, new ByteArraySegment("foo".getBytes()), null, TIMEOUT).join();

                    @Cleanup
                    ReadResult rr = context.container.read(sn, 0, 1, TIMEOUT).join();
                    context.container.getStreamSegmentInfo(sn, TIMEOUT).join();
                }
            }
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the createTransaction, append-to-Transaction, mergeTransaction methods.
     */
    @Test
    public void testTransactionOperations() throws Exception {
        // Create Transaction and Append to Transaction were partially tested in the Delete test, so we will focus on merge Transaction here.
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        // 2. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 3. Merge all the Transaction.
        Futures.allOf(mergeTransactions(transactionsBySegment, lengths, segmentContents, context, false))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, null);

                // Verify that we can no longer append to Transaction.
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    AssertExtensions.assertThrows(
                            "An append was allowed to a merged Transaction " + transactionName,
                            context.container.append(transactionName, new ByteArraySegment("foo".getBytes()), null, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentMergedException || ex instanceof StreamSegmentNotExistsException);
                }
            }
        }
        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 5. Verify their contents.
        checkReadIndex(segmentContents, lengths, context);

        // 6. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the createTransaction, append-to-Transaction, mergeTransaction methods with attribute updates.
     */
    @Test
    public void testConditionalTransactionOperations() throws Exception {
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        // 2. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 3. Correctly update attribute on parent Segments. Each source Segment will be initialized with a value and
        // after the merge, that value should have been updated.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                opFutures.add(context.container.updateAttributes(
                        parentName,
                        AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.fromUUID(UUID.nameUUIDFromBytes(transactionName.getBytes())),
                                AttributeUpdateType.None, transactionName.hashCode())),
                        TIMEOUT));
            }
        }
        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Merge all the Transactions. Now this should work.
        Futures.allOf(mergeTransactions(transactionsBySegment, lengths, segmentContents, context, true))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 5. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        HashMap<String, CompletableFuture<Map<AttributeId, Long>>> getAttributeFutures = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, null);

                // Verify that we can no longer append to Transaction.
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    AssertExtensions.assertThrows(
                            "An append was allowed to a merged Transaction " + transactionName,
                            context.container.append(transactionName, new ByteArraySegment("foo".getBytes()), null, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentMergedException || ex instanceof StreamSegmentNotExistsException);
                    getAttributeFutures.put(transactionName, context.container.getAttributes(segmentName,
                            Collections.singletonList(AttributeId.fromUUID(UUID.nameUUIDFromBytes(transactionName.getBytes()))),
                            true, TIMEOUT));
                }
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Futures.allOf(getAttributeFutures.values()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 6. Verify their contents.
        checkReadIndex(segmentContents, lengths, context);

        // 7. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        // 8. Verify that the parent Segment contains the expected attributes updated.
        for (Map.Entry<String, CompletableFuture<Map<AttributeId, Long>>> transactionAndAttribute : getAttributeFutures.entrySet()) {
            Map<AttributeId, Long> segmentAttributeUpdated = transactionAndAttribute.getValue().join();
            AttributeId transactionAttributeId = AttributeId.fromUUID(UUID.nameUUIDFromBytes(transactionAndAttribute.getKey().getBytes()));
            // Conditional merges in mergeTransactions() update the attribute value to adding 1.
            Assert.assertEquals(transactionAndAttribute.getKey().hashCode() + 1, segmentAttributeUpdated.get(transactionAttributeId).longValue());
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the createTransaction, append-to-Transaction, mergeTransaction methods with invalid attribute updates.
     */
    @Test
    public void testConditionalTransactionOperationsWithWrongAttributes() throws Exception {
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        // 2. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 3. Wrongly update attribute on parent Segments. First, we update the attributes with a wrong value to
        // validate that Segments do not get merged when attribute updates fail.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                opFutures.add(context.container.updateAttributes(
                        parentName,
                        AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.fromUUID(UUID.nameUUIDFromBytes(transactionName.getBytes())),
                                AttributeUpdateType.None, 0)),
                        TIMEOUT));
            }
        }
        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Merge all the Transaction and expect this to fail.
        for (CompletableFuture<Void> mergeTransaction : mergeTransactions(transactionsBySegment, lengths, segmentContents, context, true)) {
            AssertExtensions.assertMayThrow("If the transaction merge fails, it should be due to BadAttributeUpdateException",
                    () -> mergeTransaction,
                    ex -> ex instanceof BadAttributeUpdateException);
        }
        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test in detail the basic situations that a conditional segment merge can face.
     */
    @Test
    public void testBasicConditionalMergeScenarios() throws Exception {
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();
        final String parentSegment = "parentSegment";

        // This will be the attribute update to execute against the parent segment.
        Function<String, AttributeUpdateCollection> attributeUpdateForTxn = txnName -> AttributeUpdateCollection.from(
                new AttributeUpdate(AttributeId.fromUUID(UUID.nameUUIDFromBytes(txnName.getBytes())),
                        AttributeUpdateType.ReplaceIfEquals, txnName.hashCode() + 1, txnName.hashCode()));

        Function<String, Long> getAttributeValue = txnName -> {
            AttributeId attributeId = AttributeId.fromUUID(UUID.nameUUIDFromBytes(txnName.getBytes()));
            return context.container.getAttributes(parentSegment, Collections.singletonList(attributeId), true, TIMEOUT)
                    .join().get(attributeId);
        };

        // Create a parent Segment.
        context.container.createStreamSegment(parentSegment, getSegmentType(parentSegment), null, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentType segmentType = getSegmentType(parentSegment);

        // Case 1: Create and empty transaction that fails to merge conditionally due to bad attributes.
        String txnName = NameUtils.getTransactionNameFromId(parentSegment, UUID.randomUUID());
        AttributeId txnAttributeId = AttributeId.fromUUID(UUID.nameUUIDFromBytes(txnName.getBytes()));
        context.container.createStreamSegment(txnName, segmentType, null, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AttributeUpdateCollection attributeUpdates = attributeUpdateForTxn.apply(txnName);
        AssertExtensions.assertFutureThrows("Transaction was expected to fail on attribute update",
                context.container.mergeStreamSegment(parentSegment, txnName, attributeUpdates, TIMEOUT),
                ex -> ex instanceof BadAttributeUpdateException);
        Assert.assertEquals(Attributes.NULL_ATTRIBUTE_VALUE, (long) getAttributeValue.apply(txnName));

        // Case 2: Now, we prepare the attributes in the parent segment so the merge of the empty transaction succeeds.
        context.container.updateAttributes(
                parentSegment,
                AttributeUpdateCollection.from(new AttributeUpdate(txnAttributeId, AttributeUpdateType.Replace, txnName.hashCode())),
                TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // As the source segment is empty, the amount of merged data should be 0.
        Assert.assertEquals(0L, context.container.mergeStreamSegment(parentSegment, txnName, attributeUpdates, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getMergedDataLength());
        // But the attribute related to that transaction merge on the parent segment should have been updated.
        Assert.assertEquals(txnName.hashCode() + 1L, (long) context.container.getAttributes(parentSegment,
                Collections.singletonList(txnAttributeId), true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get(txnAttributeId));

        // Case 3: Create a non-empty transaction that should fail due to a conditional attribute update failure.
        txnName = NameUtils.getTransactionNameFromId(parentSegment, UUID.randomUUID());
        txnAttributeId = AttributeId.fromUUID(UUID.nameUUIDFromBytes(txnName.getBytes()));
        attributeUpdates = attributeUpdateForTxn.apply(txnName);
        context.container.createStreamSegment(txnName, segmentType, null, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // Add some appends to the transaction.
        RefCountByteArraySegment appendData = getAppendData(txnName, 1);
        context.container.append(txnName, appendData, null, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // Attempt the conditional merge.
        AssertExtensions.assertFutureThrows("Transaction was expected to fail on attribute update",
                context.container.mergeStreamSegment(parentSegment, txnName, attributeUpdates, TIMEOUT),
                ex -> ex instanceof BadAttributeUpdateException);
        Assert.assertEquals(Attributes.NULL_ATTRIBUTE_VALUE, (long) getAttributeValue.apply(txnName));

        // Case 4: Now, we prepare the attributes in the parent segment so the merge of the non-empty transaction succeeds.
        context.container.updateAttributes(
                parentSegment,
                AttributeUpdateCollection.from(new AttributeUpdate(txnAttributeId, AttributeUpdateType.Replace, txnName.hashCode())),
                TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // As the source segment is non-empty, the amount of merged data should be greater than 0.
        Assert.assertTrue(context.container.mergeStreamSegment(parentSegment, txnName, attributeUpdates, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getMergedDataLength() > 0);
        // The attribute related to that transaction merge on the parent segment should have been updated as well.
        Assert.assertEquals(txnName.hashCode() + 1L, (long) context.container.getAttributes(parentSegment,
                Collections.singletonList(txnAttributeId), true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).get(txnAttributeId));

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to perform future (tail) reads. Scenarios tested include:
     * * Regular appends
     * * Segment sealing
     * * Transaction merging.
     */
    @Test
    public void testFutureReads() throws Exception {
        final int nonSealReadLimit = 100;
        @Cleanup
        TestContext context = createContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        HashMap<String, ReadResult> readsBySegment = new HashMap<>();
        ArrayList<AsyncReadResultProcessor> readProcessors = new ArrayList<>();
        HashSet<String> segmentsToSeal = new HashSet<>();
        HashMap<String, ByteArrayOutputStream> readContents = new HashMap<>();
        HashMap<String, TestReadResultHandler> entryHandlers = new HashMap<>();

        // 2. Setup tail reads.
        // First 1/2 of segments will try to read Int32.Max bytes, while the other half will try to read 100 bytes.
        // We will then seal the first 1/2 of the segments, which should cause the tail reads to stop (the remaining
        // should stop upon reaching the limit).
        for (int i = 0; i < segmentNames.size(); i++) {
            String segmentName = segmentNames.get(i);
            ByteArrayOutputStream readContentsStream = new ByteArrayOutputStream();
            readContents.put(segmentName, readContentsStream);

            ReadResult readResult;
            if (i < segmentNames.size() / 2) {
                // We're going to seal this one at one point.
                segmentsToSeal.add(segmentName);
                readResult = context.container.read(segmentName, 0, Integer.MAX_VALUE, TIMEOUT).join();
            } else {
                // Just a regular one, nothing special.
                readResult = context.container.read(segmentName, 0, nonSealReadLimit, TIMEOUT).join();
            }

            // The Read callback is only accumulating data in this test; we will then compare it against the real data.
            TestReadResultHandler entryHandler = new TestReadResultHandler(readContentsStream, TIMEOUT);
            entryHandlers.put(segmentName, entryHandler);
            readsBySegment.put(segmentName, readResult);
            readProcessors.add(AsyncReadResultProcessor.process(readResult, entryHandler, executorService()));
        }

        // 3. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 4. Merge all the Transactions.
        Futures.allOf(mergeTransactions(transactionsBySegment, lengths, segmentContents, context, false))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 5. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Void>> operationFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                operationFutures.add(Futures.toVoid(context.container.append(segmentName, appendData, null, TIMEOUT)));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, null);
            }
        }

        segmentsToSeal.forEach(segmentName -> operationFutures
                .add(Futures.toVoid(context.container.sealStreamSegment(segmentName, TIMEOUT))));
        Futures.allOf(operationFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now wait for all the reads to complete, and verify their results against the expected output.
        Futures.allOf(entryHandlers.values().stream().map(h -> h.getCompleted()).collect(Collectors.toList())).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        readProcessors.forEach(AsyncReadResultProcessor::close);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<String, TestReadResultHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().getError().get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail("Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue().getError().get());
                }
            }
        }

        // Check that all the ReadResults are closed
        for (Map.Entry<String, ReadResult> e : readsBySegment.entrySet()) {
            Assert.assertTrue("Read result is not closed for segment " + e.getKey(), e.getValue().isClosed());
        }

        // Compare, byte-by-byte, the outcome of the tail reads.
        Assert.assertEquals("Unexpected number of segments were read.", segmentContents.size(), readContents.size());
        for (String segmentName : segmentNames) {
            boolean isSealed = segmentsToSeal.contains(segmentName);

            byte[] expectedData = segmentContents.get(segmentName).toByteArray();
            byte[] actualData = readContents.get(segmentName).toByteArray();
            int expectedLength = isSealed ? (int) (long) lengths.get(segmentName) : nonSealReadLimit;
            Assert.assertEquals("Unexpected read length for segment " + segmentName, expectedLength, actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + segmentName, expectedData, 0, actualData, 0, actualData.length);
        }

        // 6. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);
    }

    /**
     * Tests the ability to clean up SegmentMetadata for those segments which have not been used recently.
     * This test does the following:
     * 1. Sets up a custom SegmentContainer with a hook into the metadataCleanup task
     * 2. Creates a segment and appends something to it, each time updating attributes (and verifies they were updated correctly).
     * 3. Waits for the segment to be forgotten (evicted).
     * 4. Requests info on the segment, validates it, then makes another append, seals it, at each step verifying it was done
     * correctly (checking Metadata, Attributes and Storage).
     * 5. Deletes the segment, waits for metadata to be cleared (via forcing another log truncation), re-creates the
     * same segment and validates that the old attributes did not "bleed in".
     */
    @Test
    public void testMetadataCleanup() throws Exception {
        final String segmentName = "segment";
        final AttributeId[] attributes = new AttributeId[]{Attributes.CREATION_TIME, Attributes.EVENT_COUNT, AttributeId.randomUUID()};
        final ByteArraySegment appendData = new ByteArraySegment("hello".getBytes());
        Map<AttributeId, Long> expectedAttributes = new HashMap<>();

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext(containerConfig);
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // Create segment with initial attributes and verify they were set correctly.
        val initialAttributes = createAttributeUpdates(attributes);
        applyAttributes(initialAttributes, expectedAttributes);
        expectedAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        localContainer.createStreamSegment(segmentName, getSegmentType(segmentName), initialAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after segment creation.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        // Add one append with some attribute changes and verify they were set correctly.
        val appendAttributes = createAttributeUpdates(attributes);
        applyAttributes(appendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData, appendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after append.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        // Wait until the segment is forgotten.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now get attributes again and verify them.
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        expectedAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        // Append again, and make sure we can append at the right offset.
        val secondAppendAttributes = createAttributeUpdates(attributes);
        applyAttributes(secondAppendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData.getLength(), appendData, secondAppendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length from segment after eviction & resurrection.", 2 * appendData.getLength(), sp.getLength());
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        // Seal.
        localContainer.sealStreamSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after seal.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        // Verify the segment actually made to Storage in one piece.
        waitForSegmentInStorage(sp, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val storageInfo = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length in storage for segment.", sp.getLength(), storageInfo.getLength());

        // Delete segment and wait until it is forgotten again (we need to create another dummy segment so that we can
        // force a Metadata Truncation in order to facilitate that; this is the purpose of segment2).
        localContainer.deleteStreamSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the segment to be forgotten again.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now Create the Segment again and verify the old attributes were not "remembered".
        val newAttributes = createAttributeUpdates(attributes);
        applyAttributes(newAttributes, expectedAttributes);
        localContainer.createStreamSegment(segmentName, getSegmentType(segmentName), newAttributes, TIMEOUT)
                      .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        expectedAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after deletion and re-creation.", expectedAttributes, sp, AUTO_ATTRIBUTES);
    }

    /**
     * Tests the ability for the SegmentContainer to recover in the following scenario:
     * 1. A segment is created and recorded in the metadata (with some optional operations on it)
     * 2. The segment is evicted from the metadata.
     * 3. The segment is reactivated (with a new metadata mapping). No truncations happened since #2 above.
     * 4. Container shuts down and needs to recover. We need to ensure that recovery succeeds even with the new mapping
     * of the segment.
     */
    @Test
    public void testMetadataCleanupRecovery() throws Exception {
        final String segmentName = "segment";
        final ByteArraySegment appendData = new ByteArraySegment("hello".getBytes());

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext(containerConfig);
        val localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        SegmentProperties originalInfo;
        try (val container1 = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService())) {
            container1.startAsync().awaitRunning();

            // Create segment and make one append to it.
            container1.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.append(segmentName, appendData, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Wait until the segment is forgotten.
            container1.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Add an append - this will force the segment to be reactivated, thus be registered with a different id.
            container1.append(segmentName, appendData, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            originalInfo = container1.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.stopAsync().awaitTerminated();
        }

        // Restart container and verify it started successfully.
        @Cleanup
        val container2 = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container2.startAsync().awaitRunning();
        val recoveredInfo = container2.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length from recovered segment.", originalInfo.getLength(), recoveredInfo.getLength());

        container2.stopAsync().awaitTerminated();
    }

    /**
     * Tests the case when the ContainerMetadata has filled up to capacity (with segments and we cannot map anymore segments).
     */
    @Test
    public void testForcedMetadataCleanup() throws Exception {
        final int maxSegmentCount = 3;
        final int createdSegmentCount = maxSegmentCount * 2;
        final ContainerConfig containerConfig = ContainerConfig
                .builder()
                .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
                .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, maxSegmentCount + EXPECTED_PINNED_SEGMENT_COUNT)
                .with(ContainerConfig.STORAGE_SNAPSHOT_TIMEOUT_SECONDS, (int) DEFAULT_CONFIG.getStorageSnapshotTimeout().getSeconds())
                .build();

        // We need a special DL config so that we can force truncations after every operation - this will speed up metadata
        // eviction eligibility.
        final DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
                .build();

        @Cleanup
        TestContext context = createContext(containerConfig);
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(durableLogConfig, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // Create the segments.
        val segments = new ArrayList<String>();
        for (int i = 0; i < createdSegmentCount; i++) {
            String name = getSegmentName(i);
            segments.add(name);
            localContainer.createStreamSegment(name, getSegmentType(name), null, TIMEOUT).join();
        }

        // Activate three segments (this should fill up available capacity).
        activateSegment(segments.get(2), localContainer).join();
        activateSegment(segments.get(4), localContainer).join();
        activateSegment(segments.get(5), localContainer).join();

        // At this point, the active segments should be: 2, 4 and 5.
        // Verify we cannot activate any other segment.
        AssertExtensions.assertSuppliedFutureThrows(
                "getSegmentId() allowed mapping more segments than the metadata can support.",
                () -> activateSegment(segments.get(1), localContainer),
                ex -> ex instanceof TooManyActiveSegmentsException);

        // Test the ability to forcefully evict items from the metadata when there is pressure and we need to register something new.
        // Case 1: following a Segment deletion.
        localContainer.deleteStreamSegment(segments.get(2), TIMEOUT).join();
        val segment1Activation = tryActivate(localContainer, segments.get(1), segments.get(4));
        val segment1Info = segment1Activation.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNotNull("Unable to properly activate dormant segment (1).", segment1Info);

        // Case 2: following a Merge.
        localContainer.sealStreamSegment(segments.get(5), TIMEOUT).join();
        localContainer.mergeStreamSegment(segments.get(4), segments.get(5), TIMEOUT).join();
        val segment0Activation = tryActivate(localContainer, segments.get(0), segments.get(3));
        val segment0Info = segment0Activation.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNotNull("Unable to properly activate dormant segment (0).", segment0Info);

        tryActivate(localContainer, segments.get(1), segments.get(3)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // At this point the active segments should be: 0, 1 and 3.
        Assert.assertNotNull("Pre-activated segment did not stay in metadata (3).",
                localContainer.getStreamSegmentInfo(segments.get(3), TIMEOUT).join());

        Assert.assertNotNull("Pre-activated segment did not stay in metadata (1).",
                localContainer.getStreamSegmentInfo(segments.get(1), TIMEOUT).join());

        Assert.assertNotNull("Pre-activated segment did not stay in metadata (0).",
                localContainer.getStreamSegmentInfo(segments.get(0), TIMEOUT).join());

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to clean up Extended Attributes from Segment Metadatas that have not been used recently.
     * This test does the following:
     * 1. Sets up a custom SegmentContainer with a hook into the metadataCleanup task
     * 2. Creates a segment and appends something to it, each time updating attributes (and verifies they were updated correctly).
     * 3. Waits for the segment's attributes to be forgotten.
     * 4. Verifies that the forgotten attributes can be fetched from the Attribute Index and re-cached in memory.
     */
    @Test
    public void testAttributeCleanup() throws Exception {
        final String segmentName = "segment";
        final AttributeId[] attributes = new AttributeId[]{Attributes.EVENT_COUNT, AttributeId.uuid(0, 1), AttributeId.uuid(0, 2), AttributeId.uuid(0, 3)};
        Map<AttributeId, Long> allAttributes = new HashMap<>();

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(250));
        containerConfig.setMaxCachedExtendedAttributeCount(1);

        @Cleanup
        TestContext context = createContext(containerConfig);
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(FREQUENT_TRUNCATIONS_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        localContainer.startAsync().awaitRunning();

        // Create segment with initial attributes and verify they were set correctly.
        localContainer.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Add one append with some attribute changes and verify they were set correctly.
        val appendAttributes = createAttributeUpdates(attributes);
        applyAttributes(appendAttributes, allAttributes);
        for (val au : appendAttributes) {
            localContainer.updateAttributes(segmentName, AttributeUpdateCollection.from(au), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after initial updateAttributes() call.", allAttributes, sp, AUTO_ATTRIBUTES);

        // Wait until the attributes are forgotten
        localContainer.triggerAttributeCleanup(segmentName, containerConfig.getMaxCachedExtendedAttributeCount()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now get attributes again and verify them.
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // During attribute eviction, we expect all core attributes to be preserved, and only 1 extended attribute (as
        // defined in the config) to be preserved. This extended attribute should be the last one we updated.
        val expectedAttributes = new HashMap<>(Attributes.getCoreNonNullAttributes(allAttributes));
        val lastExtAttribute = appendAttributes.stream()
                .filter(au -> !Attributes.isCoreAttribute(au.getAttributeId()))
                .reduce((a, b) -> b).get(); // .reduce() helps us get the last element in the stream.
        expectedAttributes.put(lastExtAttribute.getAttributeId(), lastExtAttribute.getValue());
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction.", expectedAttributes, sp, AUTO_ATTRIBUTES);

        val fetchedAttributes = localContainer.getAttributes(segmentName, allAttributes.keySet(), true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertMapEquals("Unexpected attributes after eviction & reload.", allAttributes, fetchedAttributes);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & reload+getInfo.", allAttributes, sp, AUTO_ATTRIBUTES);
    }

    /**
     * Tests the behavior of the SegmentContainer when another instance of the same container is activated and fences out
     * the first one.
     */
    @Test
    public void testWriteFenceOut() throws Exception {
        final String segmentName = "SegmentName";
        final Duration shutdownTimeout = Duration.ofSeconds(5);
        @Cleanup
        TestContext context = createContext();
        val container1 = context.container;
        container1.startAsync().awaitRunning();
        container1.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Workaround for pre-SLTS (RollingStorage) known problem when a zombie (fenced-out) instance can still write
        // a header file while shutting down and concurrently with the new instance. This additional step can be retired
        // once RollingStorage is retired.
        waitForSegmentsInStorage(Collections.singleton(NameUtils.getMetadataSegmentName(CONTAINER_ID)), context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        @Cleanup
        val container2 = context.containerFactory.createStreamSegmentContainer(CONTAINER_ID);
        container2.startAsync().awaitRunning();

        AssertExtensions.assertSuppliedFutureThrows(
                "Original container did not reject an append operation after being fenced out.",
                () -> container1.append(segmentName, new ByteArraySegment(new byte[1]), null, TIMEOUT),
                ex -> ex instanceof DataLogWriterNotPrimaryException      // Write fenced.
                        || ex instanceof ObjectClosedException            // Write accepted, but OperationProcessor shuts down while processing it.
                        || ex instanceof IllegalContainerStateException); // Write rejected due to Container not running.

        // Verify we can still write to the second container.
        container2.append(segmentName, 0, new ByteArraySegment(new byte[1]), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify container1 is shutting down (give it some time to complete) and that it ends up in a Failed state.
        ServiceListeners.awaitShutdown(container1, shutdownTimeout, false);
        Assert.assertEquals("Container1 is not in a failed state after fence-out detected.", Service.State.FAILED, container1.state());
    }

    /**
     * Tests the behavior when there is a startup failure (i.e., already started services need to shut down.
     */
    @Test
    public void testStartFailure() throws Exception {
        final Duration shutdownTimeout = Duration.ofSeconds(5);

        @Cleanup
        val context = createContext();
        val failedWriterFactory = new FailedWriterFactory();
        AtomicReference<OperationLog> log = new AtomicReference<>();
        val watchableDurableLogFactory = new WatchableOperationLogFactory(context.operationLogFactory, log::set);
        val containerFactory = new StreamSegmentContainerFactory(DEFAULT_CONFIG, watchableDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, failedWriterFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        val container = containerFactory.createStreamSegmentContainer(CONTAINER_ID);
        container.startAsync();

        // Wait for the container to be shut down and verify it is failed.
        ServiceListeners.awaitShutdown(container, shutdownTimeout, false);
        Assert.assertEquals("Container is not in a failed state after a failed startup.", Service.State.FAILED, container.state());

        Throwable actualException = Exceptions.unwrap(container.failureCause());
        boolean exceptionMatch = actualException instanceof IntentionalException;
        if (!exceptionMatch) {
            Assert.fail(String.format("Container did not fail with the correct exception. Expected '%s', Actual '%s'.",
                    IntentionalException.class.getSimpleName(), actualException));
        }

        // Verify the OperationLog is also shut down, and make sure it is not in a Failed state.
        ServiceListeners.awaitShutdown(log.get(), shutdownTimeout, true);
    }

    /**
     * Tests the ability of the StreamSegmentContainer to start in Offline mode (due to an offline DurableLog) and eventually
     * become online when the DurableLog becomes too.
     */
    @Test
    public void testStartOffline() throws Exception {
        @Cleanup
        val context = createContext();

        AtomicReference<DurableDataLog> dataLog = new AtomicReference<>();
        @Cleanup
        val dataLogFactory = new TestDurableDataLogFactory(context.dataLogFactory, dataLog::set);
        AtomicReference<OperationLog> durableLog = new AtomicReference<>();
        val durableLogFactory = new WatchableOperationLogFactory(new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService()), durableLog::set);
        val containerFactory = new StreamSegmentContainerFactory(DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());

        // Write some data
        ArrayList<String> segmentNames = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        try (val container = containerFactory.createStreamSegmentContainer(CONTAINER_ID)) {
            container.startAsync().awaitRunning();
            val creationFutures = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < SEGMENT_COUNT; i++) {
                String segmentName = getSegmentName(i);
                segmentNames.add(segmentName);
                creationFutures.add(container.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT));
            }

            // Wait for the segments to be created first.
            Futures.allOf(creationFutures).join();

            val opFutures = new ArrayList<CompletableFuture<Long>>();
            for (int i = 0; i < APPENDS_PER_SEGMENT / 2; i++) {
                for (String segmentName : segmentNames) {
                    RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                    opFutures.add(container.append(segmentName, appendData, null, TIMEOUT));
                    lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                    recordAppend(segmentName, appendData, segmentContents, null);
                }
            }

            Futures.allOf(opFutures).join();

            // Disable the DurableDataLog.
            dataLog.get().disable();
        }

        // Start in "Offline" mode, verify operations cannot execute and then shut down - make sure we can shut down an offline container.
        try (val container = containerFactory.createStreamSegmentContainer(CONTAINER_ID)) {
            container.startAsync().awaitRunning();
            Assert.assertTrue("Expecting Segment Container to be offline.", container.isOffline());

            AssertExtensions.assertSuppliedFutureThrows(
                    "append() worked in offline mode.",
                    () -> container.append("foo", new ByteArraySegment(new byte[1]), null, TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "getStreamSegmentInfo() worked in offline mode.",
                    () -> container.getStreamSegmentInfo("foo", TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);
            AssertExtensions.assertSuppliedFutureThrows(
                    "read() worked in offline mode.",
                    () -> container.read("foo", 0, 1, TIMEOUT),
                    ex -> ex instanceof ContainerOfflineException);

            container.stopAsync().awaitTerminated();
        }

        // Start in "Offline" mode and verify we can resume a normal startup.
        try (val container = containerFactory.createStreamSegmentContainer(CONTAINER_ID)) {
            container.startAsync().awaitRunning();
            Assert.assertTrue("Expecting Segment Container to be offline.", container.isOffline());
            dataLog.get().enable();

            // Wait for the DurableLog to become online.
            durableLog.get().awaitOnline().get(DEFAULT_DURABLE_LOG_CONFIG.getStartRetryDelay().toMillis() * 100, TimeUnit.MILLISECONDS);

            // Verify we can execute regular operations now.
            ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
            for (int i = 0; i < APPENDS_PER_SEGMENT / 2; i++) {
                for (String segmentName : segmentNames) {
                    RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                    opFutures.add(Futures.toVoid(container.append(segmentName, appendData, null, TIMEOUT)));
                    lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                    recordAppend(segmentName, appendData, segmentContents, null);
                }
            }

            Futures.allOf(opFutures).join();

            // Verify all operations arrived in Storage.
            ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
            for (String segmentName : segmentNames) {
                SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
                segmentsCompletion.add(waitForSegmentInStorage(sp, context));
            }

            Futures.allOf(segmentsCompletion).join();

            container.stopAsync().awaitTerminated();
        }
    }

    /**
     * Tests the ability to register extensions.
     */
    @Test
    public void testExtensions() throws Exception {
        String segmentName = getSegmentName(123);
        ByteArraySegment data = getAppendData(segmentName, 0);

        // Configure extension.
        val operationProcessed = new CompletableFuture<SegmentOperation>();
        AtomicInteger count = new AtomicInteger();
        val writerProcessor = new TestWriterProcessor(op -> {
            if (op.getStreamSegmentId() != EXPECTED_METADATA_SEGMENT_ID) {
                // We need to exclude any appends that come from the MetadataStore as those do not concern us.
                count.incrementAndGet();
                if (!operationProcessed.isDone()) {
                    operationProcessed.complete(op);
                }
            }
        });

        val extension = new AtomicReference<TestSegmentContainerExtension>();
        SegmentContainerFactory.CreateExtensions additionalExtensions = (container, executor) -> {
            Assert.assertTrue("Already created", extension.compareAndSet(null,
                    new TestSegmentContainerExtension(Collections.singleton(writerProcessor))));
            return Collections.singletonMap(TestSegmentContainerExtension.class, extension.get());
        };

        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG, additionalExtensions);
        context.container.startAsync().awaitRunning();

        // Verify getExtension().
        val p = context.container.getExtension(TestSegmentContainerExtension.class);
        Assert.assertEquals("Unexpected result from getExtension().", extension.get(), p);

        // Verify Writer Segment Processors are properly wired in.
        context.container.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).join();
        context.container.append(segmentName, data, null, TIMEOUT).join();
        val rawOp = operationProcessed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Unexpected operation type.", rawOp instanceof CachedStreamSegmentAppendOperation);

        // Our operation has been transformed into a CachedStreamSegmentAppendOperation, which means it just points to
        // a location in the cache. We do not have access to that cache, so we can only verify its metadata.
        val appendOp = (CachedStreamSegmentAppendOperation) rawOp;
        Assert.assertEquals("Unexpected offset.", 0, appendOp.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected data length.", data.getLength(), appendOp.getLength());
        Assert.assertNull("Unexpected attribute updates.", appendOp.getAttributeUpdates());

        // Verify extension is closed when the SegmentContainer is closed.
        context.container.close();
        Assert.assertTrue("Extension not closed.", extension.get().closed.get());
    }

    /**
     * Tests the forSegment() method. We test this here vs in StreamSegmentContainerTests because we want to exercise
     * additional code in StreamSegmentService. This will invoke the StreamSegmentContainer code as well.
     */
    @Test
    public void testForSegment() {
        AttributeId attributeId1 = AttributeId.randomUUID();
        AttributeId attributeId2 = AttributeId.randomUUID();
        AttributeId attributeId3 = AttributeId.randomUUID();
        @Cleanup
        val context = createContext();
        context.container.startAsync().awaitRunning();

        // Create the StreamSegments.
        val segmentNames = createSegments(context);

        // Add some appends.
        for (String segmentName : segmentNames) {
            byte[] appendData = ("Append_" + segmentName).getBytes();

            val dsa = context.container.forSegment(segmentName, TIMEOUT).join();
            dsa.append(new ByteArraySegment(appendData), AttributeUpdateCollection.from(new AttributeUpdate(attributeId1, AttributeUpdateType.None, 1L)), TIMEOUT).join();
            dsa.updateAttributes(AttributeUpdateCollection.from(new AttributeUpdate(attributeId2, AttributeUpdateType.None, 2L)), TIMEOUT).join();
            dsa.append(new ByteArraySegment(appendData), AttributeUpdateCollection.from(new AttributeUpdate(attributeId3, AttributeUpdateType.None, 3L)), dsa.getInfo().getLength(), TIMEOUT).join();
            dsa.seal(TIMEOUT).join();
            dsa.truncate(1, TIMEOUT).join();

            // Check metadata.
            val info = dsa.getInfo();
            Assert.assertEquals("Unexpected name.", segmentName, info.getName());
            Assert.assertEquals("Unexpected length.", 2 * appendData.length, info.getLength());
            Assert.assertEquals("Unexpected startOffset.", 1, info.getStartOffset());
            Assert.assertEquals("Unexpected attribute count.", 3, info.getAttributes().keySet().stream().filter(id -> !AUTO_ATTRIBUTES.contains(id)).count());
            Assert.assertEquals("Unexpected attribute 1.", 1L, (long) info.getAttributes().get(attributeId1));
            Assert.assertEquals("Unexpected attribute 2.", 2L, (long) info.getAttributes().get(attributeId2));
            Assert.assertEquals("Unexpected attribute 2.", 3L, (long) info.getAttributes().get(attributeId3));
            Assert.assertTrue("Unexpected isSealed.", info.isSealed());
            Assert.assertEquals(-1L, (long) dsa.getExtendedAttributeCount(TIMEOUT).join()); // Not expecting any in this case as they are disabled for this segment.

            // Check written data.
            byte[] readBuffer = new byte[appendData.length - 1];
            @Cleanup
            val readResult = dsa.read(1, readBuffer.length, TIMEOUT);
            val firstEntry = readResult.next();
            firstEntry.requestContent(TIMEOUT);
            val entryContents = firstEntry.getContent().join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, entryContents.getLength());
            entryContents.copyTo(ByteBuffer.wrap(readBuffer));
            AssertExtensions.assertArrayEquals("Unexpected data read back.", appendData, 1, readBuffer, 0, readBuffer.length);
        }
    }

    /**
     * Tests {@link StreamSegmentContainer#forSegment(String, OperationPriority, Duration)}.
     */
    @Test
    public void testForSegmentPriority() throws Exception {
        val segmentName = "Test";
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG, NO_TRUNCATIONS_DURABLE_LOG_CONFIG, INFREQUENT_FLUSH_WRITER_CONFIG, null);
        val durableLog = new AtomicReference<OperationLog>();
        val durableLogFactory = new WatchableOperationLogFactory(context.operationLogFactory, durableLog::set);
        @Cleanup
        val container = new StreamSegmentContainer(CONTAINER_ID, DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, new NoOpWriterFactory(), context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container.startAsync().awaitRunning();

        container.createStreamSegment(segmentName, SegmentType.STREAM_SEGMENT, null, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Create a few operations using the forSegment with desired priority.
        val s1 = container.forSegment(segmentName, OperationPriority.Critical, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val futures = new ArrayList<CompletableFuture<Void>>();
        futures.add(Futures.toVoid(s1.append(new ByteArraySegment(new byte[1]), null, TIMEOUT)));
        futures.add(s1.updateAttributes(AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Replace, 1)), TIMEOUT));
        futures.add(s1.truncate(1, TIMEOUT));
        futures.add(Futures.toVoid(s1.seal(TIMEOUT)));
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Await all operations to be added to the durable log, then fetch them all. We stop when we encounter the Seal we just added.
        val ops = readDurableLog(durableLog.get(), op -> op instanceof StreamSegmentSealOperation);

        // For those operations that we do care about, verify they have the right priority.
        int count = 0;
        for (val op : ops) {
            if (op instanceof SegmentOperation && ((SegmentOperation) op).getStreamSegmentId() == s1.getSegmentId()) {
                count++;
                Assert.assertEquals("Unexpected priority for " + op, OperationPriority.Critical, op.getDesiredPriority());
            }
        }

        AssertExtensions.assertGreaterThan("Expected at least one operation to be verified.", 0, count);
    }

    private List<Operation> readDurableLog(OperationLog log, Predicate<Operation> stop) throws Exception {
        val result = new ArrayList<Operation>();
        while (result.size() == 0 || !stop.test(result.get(result.size() - 1))) {
            val r = log.read(1000, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            result.addAll(r);
        }
        return result;
    }

    /**
     * Tests the ability to save and read {@link SnapshotInfo}
     */
    @Test
    public void testSnapshotInfo() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        val snapshotInfoStore = container.getStorageSnapshotInfoStore();
        Assert.assertNotNull(snapshotInfoStore);
        Assert.assertNull(snapshotInfoStore.readSnapshotInfo().get());
        snapshotInfoStore.writeSnapshotInfo(SnapshotInfo.builder()
                .snapshotId(1)
                .epoch(2)
                .build()).get();
        for (int i = 0; i < 3; i++) {
            val v = snapshotInfoStore.readSnapshotInfo().get();
            Assert.assertNotNull(v);
            Assert.assertEquals(1, v.getSnapshotId());
            Assert.assertEquals(2, v.getEpoch());
        }

        for (int i = 0; i < 3; i++) {
            snapshotInfoStore.writeSnapshotInfo(SnapshotInfo.builder()
                    .snapshotId(i)
                    .epoch(2)
                    .build()).get();
            val v = snapshotInfoStore.readSnapshotInfo().get();
            Assert.assertNotNull(v);
            Assert.assertEquals(i, v.getSnapshotId());
            Assert.assertEquals(2, v.getEpoch());
        }

        snapshotInfoStore.writeSnapshotInfo(SnapshotInfo.builder()
                .snapshotId(1)
                .epoch(0)
                .build()).get();
        Assert.assertNull(snapshotInfoStore.readSnapshotInfo().get());

        for (int i = 1; i < 4; i++) {
            snapshotInfoStore.writeSnapshotInfo(SnapshotInfo.builder()
                    .snapshotId(1)
                    .epoch(i)
                    .build()).get();
            val v = snapshotInfoStore.readSnapshotInfo().get();
            Assert.assertNotNull(v);
            Assert.assertEquals(1, v.getSnapshotId());
            Assert.assertEquals(i, v.getEpoch());
        }

        container.stopAsync().awaitTerminated();
    }

    /**
     * Tests a non-trivial scenario in which ContainerKeyIndex may be tail-caching a stale version of a key if the
     * following conditions occur:
     * 1. StorageWriter processes values v0...vn for k1 and {@link WriterTableProcessor} indexes them.
     * 2. As a result of {@link WriterTableProcessor} activity, the last value vn for k1 is moved to the tail of the Segment.
     * 3. While TableCompactor works, a new PUT operation is appended to the Segment with new value vn+1 for k1.
     * 4. At this point, the StorageWriter stops its progress and the container restarts without processing neither the
     *    new value vn+1 nor the compacted value vn for k1.
     * 5. A subsequent restart will trigger the tail-caching from the last indexed offset, which points to vn+1.
     * 6. The bug, which consists of the tail-caching process not taking care of table entry versions, would overwrite
     *    vn+1 with vn, just because it has a higher offset as it was written later in the Segment.
     */
    @Test
    public void testTableSegmentReadAfterCompactionAndRecovery() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, NO_TRUNCATIONS_DURABLE_LOG_CONFIG, DEFAULT_WRITER_CONFIG, null);
        val durableLog = new AtomicReference<OperationLog>();
        val durableLogFactory = new WatchableOperationLogFactory(context.operationLogFactory, durableLog::set);
        // Data size and count to be written in this test.
        int serializedEntryLength = 28;
        int writtenEntries = 7;

        @Cleanup
        StreamSegmentContainer container = new StreamSegmentContainer(CONTAINER_ID, DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container.startAsync().awaitRunning();
        Assert.assertNotNull(durableLog.get());
        val tableStore = container.getExtension(ContainerTableExtension.class);

        // 1. Create the Table Segment and get a DirectSegmentAccess to it to monitor its size.
        String tableSegmentName = getSegmentName(0) + "_Table";
        val type = SegmentType.builder(getSegmentType(tableSegmentName)).tableSegment().build();
        tableStore.createSegment(tableSegmentName, type, TIMEOUT).join();
        DirectSegmentAccess directTableSegment = container.forSegment(tableSegmentName, TIMEOUT).join();

        // 2. Add some entries to the table segments. Note tha we write multiple values to each key, so the TableCompactor
        // can find entries to move to the tail.
        final BiFunction<String, Integer, TableEntry> createTableEntry = (key, value) ->
                TableEntry.unversioned(new ByteArraySegment(key.getBytes()),
                        new ByteArraySegment(String.format("Value_%s", value).getBytes()));

        // 3. This callback will run when the StorageWriter writes data to Storage. At this point, StorageWriter would
        // have completed its first iteration, so it is the time to add a new value for key1 while TableCompactor is working.
        val compactedEntry = List.of(TableEntry.versioned(new ByteArraySegment("key1".getBytes(StandardCharsets.UTF_8)),
                new ByteArraySegment("3".getBytes(StandardCharsets.UTF_8)), serializedEntryLength * 2L));
        // Simulate that Table Compactor moves [k1, 3] to the tail of the Segment as a result of compacting the first 4 entries.
        val compactedEntryUpdate = EntrySerializerTests.generateUpdateWithExplicitVersion(compactedEntry);
        CompletableFuture<Void> callbackExecuted = new CompletableFuture<>();
        context.storageFactory.getPostWriteCallback().set((segmentHandle, offset) -> {
            if (segmentHandle.getSegmentName().contains("Segment_0_Table$attributes.index") && !callbackExecuted.isDone()) {
                // New PUT with the newest value.
                Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key1", 4)), TIMEOUT)).join();
                // Simulates a compacted entry append performed by Table Compactor.
                directTableSegment.append(compactedEntryUpdate, null, TIMEOUT).join();
                callbackExecuted.complete(null);
            }
        });

        // Do the actual puts.
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key1", 1)), TIMEOUT)).join();
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key1", 2)), TIMEOUT)).join();
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key1", 3)), TIMEOUT)).join();
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key2", 1)), TIMEOUT)).join();
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key2", 2)), TIMEOUT)).join();
        Futures.toVoid(tableStore.put(tableSegmentName, Collections.singletonList(createTableEntry.apply("key2", 3)), TIMEOUT)).join();

        // 4. Above, the test does 7 puts, each one 28 bytes in size (6 entries directly, 1 via callback). Now, we need
        // to wait for the TableCompactor writing the entry (key1, 3) to the tail of the Segment.
        callbackExecuted.join();
        AssertExtensions.assertEventuallyEquals(true, () -> directTableSegment.getInfo().getLength() > (long) serializedEntryLength * writtenEntries, 5000);

        // 5. The TableCompactor has moved the entry, so we immediately stop the container to prevent StorageWriter from
        // making more progress.
        container.close();

        // 6. Create a new container instance that will recover from existing data.
        @Cleanup
        val container2 = new StreamSegmentContainer(CONTAINER_ID, DEFAULT_CONFIG, durableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, context.storageFactory,
                context.getDefaultExtensions(), executorService());
        container2.startAsync().awaitRunning();

        // 7. Verify that (key1, 4) is the actual value after performing the tail-caching process, which now takes care
        // of entry versions.
        val expected = createTableEntry.apply("key1", 4);
        val tableStore2 = container2.getExtension(ContainerTableExtension.class);
        val actual = tableStore2.get(tableSegmentName, Collections.singletonList(expected.getKey().getKey()), TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .get(0);
        Assert.assertEquals(actual.getKey().getKey(), expected.getKey().getKey());
        Assert.assertEquals(actual.getValue(), expected.getValue());
    }

    /**
     * Test that the {@link ContainerEventProcessor} service is started as part of the {@link StreamSegmentContainer}
     * and that it can process events.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorBasicOperation() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testBasicContainerEventProcessor(containerEventProcessor);
    }

    /**
     * Check that the max number of elements processed per EventProcessor iteration is respected.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorMaxItemsPerBatchRespected() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testContainerMaxItemsPerBatchRespected(containerEventProcessor);
    }

    /**
     * Verify that when a faulty handler is passed to an EventProcessor, the Segment is not truncated and the retries
     * continue indefinitely.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorFaultyHandler() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testFaultyHandler(containerEventProcessor);
    }

    /**
     * Test the behavior of the {@link StreamSegmentContainer} when multiple {@link ContainerEventProcessor.EventProcessor}
     * objects are registered, including one with a faulty handler.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorMultipleConsumers() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testMultipleProcessors(containerEventProcessor);
    }

    /**
     * Test the situation in which an EventProcessor gets BufferView.Reader.OutOfBoundsException during deserialization
     * of events.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorWithSerializationError() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testEventProcessorWithSerializationError(containerEventProcessor);
    }

    /**
     * Test the EventProcessor in durable queue mode (no handler). Then, close it and recreate another one on the same
     * internal Segment (same name) that actually consumes the events stored previously.
     *
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testEventProcessorDurableQueueAndSwitchToConsumer() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();

        int allEventsToProcess = 100;
        @Cleanup
        ContainerEventProcessorImpl containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessor.EventProcessor processor = containerEventProcessor.forDurableQueue("testDurableQueue")
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);

        // At this point, we can only add events, but not consuming them as the EventProcessor works in durable queue mode.
        for (int i = 0; i < allEventsToProcess; i++) {
            BufferView event = new ByteArraySegment(ByteBuffer.allocate(Integer.BYTES).putInt(i).array());
            processor.add(event, TIMEOUT_FUTURE).join();
        }
        Assert.assertEquals("Event processor object not matching", processor, containerEventProcessor.forDurableQueue("testDurableQueue")
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS));
        // Close the processor and unregister it.
        processor.close();
        // Make sure that EventProcessor eventually terminates.
        ((ContainerEventProcessorImpl.EventProcessorImpl) processor).awaitTerminated();

        // Now, re-create the Event Processor with a handler to consume the events.
        ContainerEventProcessor.EventProcessorConfig eventProcessorConfig = new ContainerEventProcessor.EventProcessorConfig(EVENT_PROCESSOR_EVENTS_AT_ONCE,
                EVENT_PROCESSOR_MAX_OUTSTANDING_BYTES, EVENT_PROCESSOR_TRUNCATE_SIZE_BYTES);
        List<Integer> processorResults = new ArrayList<>();
        Function<List<BufferView>, CompletableFuture<Void>> handler = l -> {
            l.forEach(b -> {
                try {
                    processorResults.add(ByteBuffer.wrap(b.getReader().readNBytes(Integer.BYTES)).getInt());
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            return CompletableFuture.completedFuture(null);
        };
        processor = containerEventProcessor.forConsumer("testDurableQueue", handler, eventProcessorConfig)
                .get(TIMEOUT_FUTURE.toSeconds(), TimeUnit.SECONDS);
        // Wait for all items to be processed.
        AssertExtensions.assertEventuallyEquals(true, () -> processorResults.size() == allEventsToProcess, 10000);
        Assert.assertArrayEquals(processorResults.toArray(), IntStream.iterate(0, v -> v + 1).limit(allEventsToProcess).boxed().toArray());
        // Just check failure callback.
        ((ContainerEventProcessorImpl.EventProcessorImpl) processor).failureCallback(new IntentionalException());
        // Close the processor and unregister it.
        processor.close();
        // Make sure that EventProcessor eventually terminates.
        ((ContainerEventProcessorImpl.EventProcessorImpl) processor).awaitTerminated();
    }

    /**
     * Check that an EventProcessor does not accept any new event once the maximum outstanding bytes has been reached.
     *
     * @throws Exception
     */
    @Test(timeout = 30000)
    public void testEventProcessorEventRejectionOnMaxOutstanding() throws Exception {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessor containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        ContainerEventProcessorTests.testEventRejectionOnMaxOutstanding(containerEventProcessor);
    }

    /**
     * Tests that call to getOrCreateInternalSegment is idempotent and always provides pinned segments.
     */
    @Test(timeout = 30000)
    public void testPinnedSegmentReload() {
        @Cleanup
        TestContext context = createContext();
        val container = (StreamSegmentContainer) context.container;
        container.startAsync().awaitRunning();
        @Cleanup
        ContainerEventProcessorImpl containerEventProcessor = new ContainerEventProcessorImpl(container, container.metadataStore,
                TIMEOUT_EVENT_PROCESSOR_ITERATION, TIMEOUT_EVENT_PROCESSOR_ITERATION, this.executorService());
        Function<String, CompletableFuture<DirectSegmentAccess>> segmentSupplier =
                containerEventProcessor.getOrCreateInternalSegment(container, container.metadataStore, TIMEOUT_EVENT_PROCESSOR_ITERATION);
        long segmentId = segmentSupplier.apply("dummySegment").join().getSegmentId();
        for (int i = 0; i < 10; i++) {
            DirectSegmentAccess segment = segmentSupplier.apply("dummySegment").join();
            assertTrue(segment.getInfo().isPinned());
            assertEquals(segmentId, segment.getSegmentId());
        }
    }

    /**
     * Attempts to activate the targetSegment in the given Container. Since we do not have access to the internals of the
     * Container, we need to trigger this somehow, hence the need for this complex code. We need to trigger a truncation,
     * so we need an 'appendSegment' to which we continuously append so that the DurableDataLog is truncated. After truncation,
     * the Metadata should have enough leeway in making room for new activation.
     *
     * @return A Future that will complete either with an exception (failure) or SegmentProperties for the targetSegment.
     */
    private CompletableFuture<SegmentProperties> tryActivate(MetadataCleanupContainer localContainer, String targetSegment, String appendSegment) {
        CompletableFuture<SegmentProperties> successfulMap = new CompletableFuture<>();

        // Append continuously to an existing segment in order to trigger truncations (these are necessary for forced evictions).
        val appendFuture = localContainer.appendRandomly(appendSegment, false, () -> !successfulMap.isDone());
        Futures.exceptionListener(appendFuture, successfulMap::completeExceptionally);

        // Repeatedly try to get info on 'segment1' (activate it), until we succeed or time out.
        TimeoutTimer remaining = new TimeoutTimer(TIMEOUT);
        Futures.loop(
                () -> !successfulMap.isDone(),
                () -> Futures
                        .delayedFuture(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT), executorService())
                        .thenCompose(v -> localContainer.getStreamSegmentInfo(targetSegment, TIMEOUT))
                        .thenAccept(successfulMap::complete)
                        .exceptionally(ex -> {
                            if (!(Exceptions.unwrap(ex) instanceof TooManyActiveSegmentsException)) {
                                // Some other error.
                                successfulMap.completeExceptionally(ex);
                            } else if (!remaining.hasRemaining()) {
                                // Waited too long.
                                successfulMap.completeExceptionally(new TimeoutException("No successful activation could be done in the allotted time."));
                            }

                            // Try again.
                            return null;
                        }),
                executorService());
        return successfulMap;
    }

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, TestContext context) {
        checkStorage(segmentContents, lengths, context.container, context.storage);
    }

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, SegmentContainer container, Storage storage) {
        for (String segmentName : segmentContents.keySet()) {
            // 1. Deletion status
            SegmentProperties sp = null;
            try {
                sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                Assert.assertFalse(
                        "Segment is marked as deleted in metadata but was not deleted in Storage " + segmentName,
                        storage.exists(segmentName, TIMEOUT).join());

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            SegmentProperties storageProps = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Metadata and Storage for segment " + segmentName, sp.isSealed(), storageProps.isSealed());

            // 3. Contents.
            long expectedLength = lengths.get(segmentName);
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedLength, storageProps.getLength());

            byte[] expectedData = segmentContents.get(segmentName).toByteArray();
            byte[] actualData = new byte[expectedData.length];
            val readHandle = storage.openRead(segmentName).join();
            int actualLength = storage.read(readHandle, 0, actualData, 0, actualData.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentName, expectedLength, actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentName, expectedData, actualData);
        }
    }

    private void checkReadIndex(Map<String, ByteArrayOutputStream> segmentContents, Map<String, Long> lengths,
                                       TestContext context) throws Exception {
        checkReadIndex(segmentContents, lengths, Collections.emptyMap(), context);
    }

    private static void checkReadIndex(Map<String, ByteArrayOutputStream> segmentContents, Map<String, Long> lengths,
                                       Map<String, Long> truncationOffsets, TestContext context) throws Exception {
        checkReadIndex(segmentContents, lengths, truncationOffsets, context.container);
    }

    private static void checkReadIndex(Map<String, ByteArrayOutputStream> segmentContents, Map<String, Long> lengths,
                                       Map<String, Long> truncationOffsets, SegmentContainer container) throws Exception {
        waitForOperationsInReadIndex(container);
        for (String segmentName : segmentContents.keySet()) {
            long segmentLength = lengths.get(segmentName);
            long startOffset = truncationOffsets.getOrDefault(segmentName, 0L);
            val si = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Unexpected Metadata StartOffset for segment " + segmentName, startOffset, si.getStartOffset());
            Assert.assertEquals("Unexpected Metadata Length for segment " + segmentName, segmentLength, si.getLength());
            byte[] expectedData = segmentContents.get(segmentName).toByteArray();

            long expectedCurrentOffset = startOffset;
            @Cleanup
            ReadResult readResult = container.read(segmentName, expectedCurrentOffset, (int) (segmentLength - startOffset), TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in testSegmentRegularOperations; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName, expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                BufferView readEntryContents = readEntry.getContent().join();
                byte[] actualData = readEntryContents.getCopy();
                AssertExtensions.assertArrayEquals(
                    "Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset,
                    expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

    private void checkActiveSegments(SegmentContainer container, int expectedCount) {
        val initialActiveSegments = container.getActiveSegments();
        int ignoredSegments = 0;
        for (SegmentProperties sp : initialActiveSegments) {
            boolean match = false;
            for (String systemSegment : SystemJournal.getChunkStorageSystemSegments(container.getId())) {
                if (sp.getName().equals(systemSegment)) {
                    match = true;
                    break;
                }
            }
            if (match) {
                ignoredSegments++;
                continue;
            }
            val expectedSp = container.getStreamSegmentInfo(sp.getName(), TIMEOUT).join();
            Assert.assertEquals("Unexpected length (from getActiveSegments) for segment " + sp.getName(), expectedSp.getLength(), sp.getLength());
            Assert.assertEquals("Unexpected sealed (from getActiveSegments) for segment " + sp.getName(), expectedSp.isSealed(), sp.isSealed());
            Assert.assertEquals("Unexpected deleted (from getActiveSegments) for segment " + sp.getName(), expectedSp.isDeleted(), sp.isDeleted());
            SegmentMetadataComparer.assertSameAttributes("Unexpected attributes (from getActiveSegments) for segment " + sp.getName(),
                    expectedSp.getAttributes(), sp, AUTO_ATTRIBUTES);
        }

        Assert.assertEquals("Unexpected result from getActiveSegments with freshly created segments.",
                expectedCount + ignoredSegments, initialActiveSegments.size());
    }

    private void checkAttributeIterators(DirectSegmentAccess segment, List<AttributeId> sortedAttributes, Map<AttributeId, Long> allExpectedValues) throws Exception {
        int skip = sortedAttributes.size() / 10;
        for (int i = 0; i < sortedAttributes.size() / 2; i += skip) {
            AttributeId fromId = sortedAttributes.get(i);
            AttributeId toId = sortedAttributes.get(sortedAttributes.size() - i - 1);
            val expectedValues = allExpectedValues
                    .entrySet().stream()
                    .filter(e -> fromId.compareTo(e.getKey()) <= 0 && toId.compareTo(e.getKey()) >= 0)
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .collect(Collectors.toList());

            val actualValues = new ArrayList<Map.Entry<AttributeId, Long>>();
            val ids = new HashSet<AttributeId>();
            val iterator = segment.attributeIterator(fromId, toId, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            iterator.forEachRemaining(batch ->
                    batch.forEach(attribute -> {
                        Assert.assertTrue("Duplicate key found.", ids.add(attribute.getKey()));
                        actualValues.add(attribute);
                    }), executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            AssertExtensions.assertListEquals("Unexpected iterator result.", expectedValues, actualValues,
                    (e1, e2) -> e1.getKey().equals(e2.getKey()) && e1.getValue().equals(e2.getValue()));
        }
    }

    /**
     * Verifies that a Segment does not exist in Storage, with a reasonable delay. This can be used to verify that
     * a delayed delete is actually processed by the Storage Writer.
     */
    private void checkNotExistsInStorage(String segmentName, TestContext context) {
        int attemptsLeft = 100;
        final long delay = DEFAULT_WRITER_CONFIG.getFlushThresholdTime().toMillis();
        while (attemptsLeft >= 0 && context.storage.exists(segmentName, TIMEOUT).join()) {
            Exceptions.handleInterrupted(() -> Thread.sleep(delay));
            attemptsLeft--;
        }

        Assert.assertTrue("Segment '" + segmentName + "' still exists in Storage.", attemptsLeft >= 0);
    }

    @SneakyThrows
    private void checkAppendLeaks(Collection<RefCountByteArraySegment> appends) {
        Assert.assertTrue("At least one append buffer has never been retained.",
                appends.stream().allMatch(RefCountByteArraySegment::wasRetained));

        AssertExtensions.assertEventuallyEquals(0, () -> (int) appends.stream().mapToInt(RefCountByteArraySegment::getRefCount).sum(), 1000);
    }

    private void appendToParentsAndTransactions(Collection<String> segmentNames,
                                                HashMap<String, ArrayList<String>> transactionsBySegment,
                                                HashMap<String, Long> lengths,
                                                HashMap<String, ByteArrayOutputStream> segmentContents,
                                                TestContext context) throws Exception {
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                RefCountByteArraySegment appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.getLength());
                recordAppend(segmentName, appendData, segmentContents, null);

                boolean emptyTransaction = false;
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    if (!emptyTransaction) {
                        lengths.put(transactionName, 0L);
                        recordAppend(transactionName, new RefCountByteArraySegment(new byte[0]), segmentContents, null);
                        emptyTransaction = true;
                        continue;
                    }

                    appendData = getAppendData(transactionName, i);
                    appendFutures.add(context.container.append(transactionName, appendData, null, TIMEOUT));
                    lengths.put(transactionName, lengths.getOrDefault(transactionName, 0L) + appendData.getLength());
                    recordAppend(transactionName, appendData, segmentContents, null);
                }
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private ArrayList<CompletableFuture<Void>> mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
                                                                 HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context,
                                                                 boolean conditionalMerge) throws Exception {
        ArrayList<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                if (++i % 2 == 0) {
                    // Every other Merge operation, pre-seal the source. We want to verify we correctly handle this situation as well.
                    mergeFutures.add(Futures.toVoid(context.container.sealStreamSegment(transactionName, TIMEOUT)));
                }

                // Use both calls, with and without attribute updates for mergeSegments.
                if (conditionalMerge) {
                    AttributeUpdateCollection attributeUpdates = AttributeUpdateCollection.from(
                            new AttributeUpdate(AttributeId.fromUUID(UUID.nameUUIDFromBytes(transactionName.getBytes())),
                            AttributeUpdateType.ReplaceIfEquals, transactionName.hashCode() + 1, transactionName.hashCode()));
                    mergeFutures.add(Futures.toVoid(context.container.mergeStreamSegment(parentName, transactionName, attributeUpdates, TIMEOUT)));
                } else {
                    mergeFutures.add(Futures.toVoid(context.container.mergeStreamSegment(parentName, transactionName, TIMEOUT)));
                }

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        return mergeFutures;
    }

    private RefCountByteArraySegment getAppendData(String segmentName, int appendId) {
        return new RefCountByteArraySegment(String.format("%s_%d", segmentName, appendId).getBytes());
    }

    private ArrayList<String> createSegments(TestContext context) {
        return createSegments(context.container);
    }

    private ArrayList<String> createSegments(SegmentContainer container) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(container.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT));
        }

        Futures.allOf(futures).join();
        return segmentNames;
    }

    @Test(timeout = 120000)
    public void testLTSRecovery() throws Exception {
        final String segmentName = "segment512";
        final ByteArraySegment appendData = new ByteArraySegment("hello".getBytes());

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext(containerConfig);
        val localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        AtomicReference<OperationLog> durableLog = new AtomicReference<>();

        val watchableOperationLogFactory = new WatchableOperationLogFactory(localDurableLogFactory, durableLog::set);
        FileSystemSimpleStorageFactory storageFactory = createFileSystemStorageFactory();
        try (val container1 = new StreamSegmentContainer(CONTAINER_ID, containerConfig, watchableOperationLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                context.getDefaultExtensions(), executorService())) {
            container1.startAsync().awaitRunning();

            // Create segment and make one append to it.
            container1.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.append(segmentName, appendData, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.flushToStorage(TIMEOUT).join();
            container1.stopAsync().awaitTerminated();
        }

        context = createContext(containerConfig); // new context again
        val localDurableLogFactoryForRecovery = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());

        try (val container2 = new StreamSegmentContainer(CONTAINER_ID, containerConfig, localDurableLogFactoryForRecovery,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                context.getDefaultExtensions(), executorService())) {
            container2.startAsync().awaitRunning();
            ReadResult readResult = container2.read(segmentName, 0, appendData.getLength(), TIMEOUT).join();
            while ( readResult.hasNext() ) {
                  val readResultEntry = readResult.next();
                  readResultEntry.requestContent(TIMEOUT);
                  val entry = readResultEntry.getContent().join();
                  String output = new String(entry.getCopy());
                 Assert.assertEquals(new String(appendData.array()), output);
            }
        }
    }

    @Test (timeout = 120000)
    public void testFlushToStorageForEpochFileAlreadyExists() throws Exception {
        final String segmentName = "segment512";
        final ByteArraySegment appendData = new ByteArraySegment("hello".getBytes());

        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT));

        @Cleanup
        TestContext context = createContext(containerConfig);
        val localDurableLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, context.dataLogFactory, executorService());
        AtomicReference<OperationLog> durableLog = new AtomicReference<>();

        val watchableOperationLogFactory = new WatchableOperationLogFactory(localDurableLogFactory, durableLog::set);
        FileSystemSimpleStorageFactory storageFactory = createFileSystemStorageFactory();
        try (val container1 = new StreamSegmentContainer(CONTAINER_ID, containerConfig, watchableOperationLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                context.getDefaultExtensions(), executorService())) {
            container1.startAsync().awaitRunning();

            // Create segment and make one append to it.
            container1.createStreamSegment(segmentName, getSegmentType(segmentName), null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            container1.append(segmentName, appendData, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            // Test 1: Exercise epoch already exists flow in flushtostorage
            container1.flushToStorage(TIMEOUT).join();
            container1.flushToStorage(TIMEOUT).join();
            // Test 1 ends

            // Test 2: Exercise saved epoch is higher than contaier epoch
            container1.metadata.setContainerEpochAfterRestore(5);
            container1.flushToStorage(TIMEOUT).join();
            container1.metadata.setContainerEpochAfterRestore(3);
            AssertExtensions.assertSuppliedFutureThrows("", () -> container1.flushToStorage(TIMEOUT), ex -> Exceptions.unwrap(ex) instanceof IllegalContainerStateException );
            //Test 2 ends
            container1.stopAsync().awaitTerminated();
        }
    }

    @SneakyThrows
    private FileSystemSimpleStorageFactory createFileSystemStorageFactory() {
        File baseDir = Files.createTempDirectory("TestStorage").toFile().getAbsoluteFile();
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        return new FileSystemSimpleStorageFactory(config, adapterConfig, executorService());
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, TestContext context) {
        // Create the Transaction.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        HashMap<String, ArrayList<String>> transactions = new HashMap<>();
        for (String segmentName : segmentNames) {
            val txnList = new ArrayList<String>(TRANSACTIONS_PER_SEGMENT);
            transactions.put(segmentName, txnList);
            // Transaction segments should have the same type as their parents, otherwise our checks won't work properly.
            val segmentType = getSegmentType(segmentName);
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                String txnName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                txnList.add(txnName);
                futures.add(context.container.createStreamSegment(txnName, segmentType, null, TIMEOUT));
            }
        }

        Futures.allOf(futures).join();
        return transactions;
    }

    private void recordAppend(String segmentName, RefCountByteArraySegment data, HashMap<String, ByteArrayOutputStream> segmentContents, ArrayList<RefCountByteArraySegment> appends) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        data.copyTo(contents);
        if (appends != null) {
            appends.add(data);
        }
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private SegmentType getSegmentType(String segmentName) {
        // "Randomize" through all the segment types, but using a deterministic function so we can check results later.
        return SEGMENT_TYPES[Math.abs(segmentName.hashCode() % SEGMENT_TYPES.length)];
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, TestContext context) {
        return waitForSegmentsInStorage(segmentNames, context.container, context);
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, SegmentContainer container, TestContext context) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, context));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties metadataProps, TestContext context) {
        if (metadataProps.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // Check if the Storage Segment is caught up. If sealed, we want to make sure that both the Segment and its
        // Attribute Segment are sealed (or the latter has been deleted - for transactions). For all other, we want to
        // ensure that the length and truncation offsets have  caught up.
        BiFunction<SegmentProperties, SegmentProperties, Boolean> meetsConditions = (segmentProps, attrProps) ->
                metadataProps.isSealed() == (segmentProps.isSealed() && (attrProps.isSealed() || attrProps.isDeleted()))
                        && segmentProps.getLength() >= metadataProps.getLength()
                        && context.storageFactory.truncationOffsets.getOrDefault(metadataProps.getName(), 0L) >= metadataProps.getStartOffset();

        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataProps.getName());
        AtomicBoolean canContinue = new AtomicBoolean(true);
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        return Futures.loop(
                canContinue::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(metadataProps.getName(), timer, context);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, context);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                if (meetsConditions.apply(segInfo.join(), attrInfo.join())) {
                                    canContinue.set(false);
                                    return CompletableFuture.completedFuture(null);
                                } else if (!timer.hasRemaining()) {
                                    return Futures.failedFuture(new TimeoutException());
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(10), executorService());
                                }
                            }).thenRun(Runnables.doNothing());
                },
                executorService());
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, TestContext context) {
        return Futures.exceptionallyExpecting(
                context.storage.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                ex -> ex instanceof StreamSegmentNotExistsException,
                StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }

    private AttributeUpdateCollection createAttributeUpdates(AttributeId[] attributes) {
        return Arrays.stream(attributes)
                .map(a -> new AttributeUpdate(a, AttributeUpdateType.Replace, System.nanoTime()))
                .collect(Collectors.toCollection(AttributeUpdateCollection::new));
    }

    private void applyAttributes(Collection<AttributeUpdate> updates, Map<AttributeId, Long> target) {
        updates.forEach(au -> target.put(au.getAttributeId(), au.getValue()));
    }

    /**
     * Ensures that all Segments defined in the given collection are loaded up into the Container's metadata.
     * This is used to simplify a few tests that do not expect interference from MetadataStore's assignment logic
     * (that is, they execute operations in a certain order and assume that those ops are added to OperationProcessor queue
     * in that order; if MetadataStore interferes, there is no guarantee as to what this order will be).
     */
    @SneakyThrows
    private void activateAllSegments(Collection<String> segmentNames, TestContext context) {
        val futures = segmentNames.stream()
                                  .map(s -> activateSegment(s, context.container))
                                  .collect(Collectors.toList());
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> activateSegment(String segmentName, SegmentContainer container) {
        return container.read(segmentName, 0, 1, TIMEOUT).thenAccept(ReadResult::close);
    }

    /**
     * Blocks until all operations processed so far have been added to the ReadIndex and InMemoryOperationLog.
     * This is needed to simplify test verification due to the fact that the the OperationProcessor commits operations to
     * the ReadIndex and InMemoryOperationLog asynchronously, after those operations were ack-ed. This method makes use
     * of the fact that the OperationProcessor/MemoryStateUpdater will still commit such operations in sequence; it
     * creates a new segment, writes 1 byte to it and issues a read (actual/future) and waits until it's completed - when
     * it is, it is guaranteed that everything prior to that has been committed.
     */
    private static void waitForOperationsInReadIndex(SegmentContainer container) throws Exception {
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        String segmentName = "test" + System.nanoTime();
        container.createStreamSegment(segmentName, BASIC_TYPE, null, timer.getRemaining())
                .thenCompose(v -> container.append(segmentName, new ByteArraySegment(new byte[1]), null, timer.getRemaining()))
                .thenCompose(v -> container.read(segmentName, 0, 1, timer.getRemaining()))
                .thenCompose(rr -> {
                    ReadResultEntry rre = rr.next();
                    rre.requestContent(TIMEOUT);
                    return rre.getContent().thenRun(rr::close);
                })
                 .thenCompose(v -> container.deleteStreamSegment(segmentName, timer.getRemaining()))
                 .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    }

    @SneakyThrows
    private void await(Future<?> f) {
        f.get();

        //region TestContext
    }

    TestContext createContext() {
        return new TestContext(DEFAULT_CONFIG, null);
    }

    TestContext createContext(ContainerConfig config) {
        return new TestContext(config, null);
    }

    private class TestContext implements AutoCloseable {
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
            this(config, DEFAULT_DURABLE_LOG_CONFIG, DEFAULT_WRITER_CONFIG, createAdditionalExtensions);
        }

        TestContext(ContainerConfig config, DurableLogConfig durableLogConfig, WriterConfig writerConfig,
                    SegmentContainerFactory.CreateExtensions createAdditionalExtensions) {
            this.storageFactory = new WatchableInMemoryStorageFactory(executorService());
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService());
            this.operationLogFactory = new DurableLogFactory(durableLogConfig, dataLogFactory, executorService());
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService());
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, executorService());
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, executorService());
            this.writerFactory = new StorageWriterFactory(writerConfig, executorService());
            this.containerFactory = new StreamSegmentContainerFactory(config, this.operationLogFactory,
                    this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, this.storageFactory,
                    createExtensions(createAdditionalExtensions), executorService());
            this.container = this.containerFactory.createStreamSegmentContainer(CONTAINER_ID);
            this.storage = this.storageFactory.createStorageAdapter();
        }

        SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(TableExtensionConfig.builder().build(), c, this.cacheManager, e);
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

    //endregion

    //region MetadataCleanupContainer

    private static class MetadataCleanupContainer extends StreamSegmentContainer {
        private Consumer<Collection<String>> metadataCleanupFinishedCallback = null;
        private final ScheduledExecutorService executor;

        MetadataCleanupContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                 ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                                 WriterFactory writerFactory, StorageFactory storageFactory,
                                 SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
            super(streamSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                    storageFactory, createExtensions, executor);
            this.executor = executor;
        }

        @Override
        protected void notifyMetadataRemoved(Collection<SegmentMetadata> metadatas) {
            super.notifyMetadataRemoved(metadatas);

            Consumer<Collection<String>> c = this.metadataCleanupFinishedCallback;
            if (c != null) {
                c.accept(metadatas.stream().map(SegmentMetadata::getName).collect(Collectors.toList()));
            }
        }

        /**
         * Triggers at least one attribute cleanup for the given segment by continuously appending data (with no attributes)
         * for that segment until a cleanup is detected
         *
         * @param segmentName The segment we are trying to evict attributes for.
         */
        CompletableFuture<Void> triggerAttributeCleanup(String segmentName, int expectedExtendedAttributeCount) {
            CompletableFuture<Void> cleanupTask = Futures.futureWithTimeout(TIMEOUT, this.executor);
            SegmentMetadata sm = super.metadata.getStreamSegmentMetadata(super.metadata.getStreamSegmentId(segmentName, false));

            // Inject this callback into the MetadataCleaner callback, which was setup for us in createMetadataCleaner().
            this.metadataCleanupFinishedCallback = ignored -> {
                int extendedAttributeCount = sm.getAttributes((k, v) -> !Attributes.isCoreAttribute(k)).size();
                if (extendedAttributeCount <= expectedExtendedAttributeCount) {
                    cleanupTask.complete(null);
                }
            };

            CompletableFuture<Void> af = appendRandomly(segmentName, false, () -> !cleanupTask.isDone());
            Futures.exceptionListener(af, cleanupTask::completeExceptionally);
            return cleanupTask;
        }

        /**
         * Triggers a number of metadata cleanups by repeatedly appending to a random new segment until a cleanup task is detected.
         *
         * @param expectedSegmentNames The segments that we are expecting to evict.
         */
        CompletableFuture<Void> triggerMetadataCleanup(Collection<String> expectedSegmentNames) {
            String tempSegmentName = getSegmentName(Long.hashCode(System.nanoTime()));
            HashSet<String> remainingSegments = new HashSet<>(expectedSegmentNames);
            CompletableFuture<Void> cleanupTask = Futures.futureWithTimeout(TIMEOUT, this.executor);

            // Inject this callback into the MetadataCleaner callback, which was setup for us in createMetadataCleaner().
            this.metadataCleanupFinishedCallback = evictedSegmentNames -> {
                remainingSegments.removeAll(evictedSegmentNames);
                if (remainingSegments.size() == 0) {
                    cleanupTask.complete(null);
                }
            };

            CompletableFuture<Void> af = appendRandomly(tempSegmentName, true, () -> !cleanupTask.isDone());
            Futures.exceptionListener(af, cleanupTask::completeExceptionally);
            return cleanupTask;
        }

        /**
         * Appends continuously to a random new segment in the given container, as long as the given condition holds.
         */
        CompletableFuture<Void> appendRandomly(String segmentName, boolean createSegment, Supplier<Boolean> canContinue) {
            byte[] appendData = new byte[1];
            return (createSegment ? createStreamSegment(segmentName, BASIC_TYPE, null, TIMEOUT) : CompletableFuture.completedFuture(null))
                    .thenCompose(v -> Futures.loop(
                            canContinue,
                            () -> Futures.toVoid(append(segmentName, new ByteArraySegment(appendData), null, TIMEOUT)),
                            this.executor))
                    .thenCompose(v -> createSegment ? deleteStreamSegment(segmentName, TIMEOUT) : CompletableFuture.completedFuture(null));
        }
    }

    //endregion

    //region TestContainerConfig

    private static class TestContainerConfig extends ContainerConfig {
        @Getter
        @Setter
        private Duration segmentMetadataExpiration;

        @Getter
        @Setter
        private int maxCachedExtendedAttributeCount;

        TestContainerConfig() throws ConfigurationException {
            super(new TypedProperties(new Properties(), "ns"));
        }
    }

    //endregion

    //region Helper Classes

    @RequiredArgsConstructor
    private static class TestSegmentContainerExtension implements SegmentContainerExtension {
        final AtomicBoolean closed = new AtomicBoolean();
        final Collection<WriterSegmentProcessor> writerSegmentProcessors;

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
            return this.writerSegmentProcessors;
        }
    }

    @RequiredArgsConstructor
    private static class TestWriterProcessor implements WriterSegmentProcessor {
        final Consumer<SegmentOperation> addHandler;

        @Override
        public void add(SegmentOperation operation) {
            this.addHandler.accept(operation);
        }

        @Override
        public void close() {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public long getLowestUncommittedSequenceNumber() {
            return 0;
        }

        @Override
        public boolean mustFlush() {
            return false;
        }

        @Override
        public CompletableFuture<WriterFlushResult> flush(boolean force, Duration timeout) {
            return CompletableFuture.completedFuture(new WriterFlushResult());
        }
    }

    private static class WatchableInMemoryStorageFactory extends InMemoryStorageFactory {
        private final ConcurrentHashMap<String, Long> truncationOffsets = new ConcurrentHashMap<>();
        // Allow tests to run a custom callback after write() method is invoked in Storage.
        @Getter
        private final AtomicReference<BiConsumer<SegmentHandle, Long>> postWriteCallback = new AtomicReference<>(null);


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
            public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
                return (postWriteCallback.get() == null) ? super.write(handle, offset, data, length, timeout) :
                   super.write(handle, offset, data, length, timeout).thenCompose(v -> {
                        postWriteCallback.get().accept(handle, offset);
                        return null;
                    });
            }

            @Override
            public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
                return super.truncate(handle, offset, timeout)
                            .thenRun(() -> truncationOffsets.put(handle.getSegmentName(), offset));
            }
        }
    }

    @RequiredArgsConstructor
    private static class WatchableOperationLogFactory implements OperationLogFactory {
        private final OperationLogFactory wrappedFactory;
        private final Consumer<OperationLog> onCreateLog;

        @Override
        public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, ReadIndex readIndex) {
            OperationLog log = this.wrappedFactory.createDurableLog(containerMetadata, readIndex);
            this.onCreateLog.accept(log);
            return log;
        }
    }

    static class TestStreamSegmentMetadata extends StreamSegmentMetadata {
        /**
         * Creates a new instance of the StreamSegmentMetadata class for a StreamSegment.
         *
         * @param streamSegmentName The name of the StreamSegment.
         * @param streamSegmentId   The Id of the StreamSegment.
         * @param containerId       The Id of the Container this StreamSegment belongs to.
         * @throws IllegalArgumentException If either of the arguments are invalid.
         */
        public TestStreamSegmentMetadata(String streamSegmentName, long streamSegmentId, int containerId) {
            super(streamSegmentName, streamSegmentId, containerId);
        }

        public synchronized void copyFrom(SegmentMetadata base) {
            Exceptions.checkArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentId).");
            Exceptions.checkArgument(this.getName().equals(base.getName()), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentName).");

            setStorageLength(base.getStorageLength());
            setLength(base.getLength());

            // Update StartOffset after (potentially) updating the length, since he Start Offset must be less than or equal to Length.
            setStartOffset(base.getStartOffset());
            setLastModified(base.getLastModified());
            updateAttributes(base.getAttributes());
            refreshDerivedProperties();

            if (base.isSealed()) {
                markSealed();
                if (base.isSealedInStorage()) {
                    markSealedInStorage();
                }
            }

            if (base.isMerged()) {
                markMerged();
            }

            if (base.isDeleted()) {
                markDeleted();
                if (base.isDeletedInStorage()) {
                    markDeletedInStorage();
                }
            }

            if (base.isPinned()) {
                markPinned();
            }
        }
    }

    private static class FailedWriterFactory implements WriterFactory {
        @Override
        public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex,
                                   ContainerAttributeIndex attributeIndex, Storage storage, CreateProcessors createProcessors) {
            return new FailedWriter();
        }

        private static class FailedWriter extends AbstractService implements Writer {
            @Override
            protected void doStart() {
                notifyFailed(new IntentionalException());
            }

            @Override
            protected void doStop() {
                notifyStopped();
            }

            @Override
            public void close() {

            }

            @Override
            public CompletableFuture<Boolean> forceFlush(long upToSequenceNumber, Duration timeout) {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class NoOpWriterFactory implements WriterFactory {
        @Override
        public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex,
                                   ContainerAttributeIndex attributeIndex, Storage storage, CreateProcessors createProcessors) {
            return new NoOpWriter();
        }

        private static class NoOpWriter extends AbstractService implements Writer {
            @Override
            protected void doStart() {
                notifyStarted();
            }

            @Override
            protected void doStop() {
                notifyStopped();
            }

            @Override
            public void close() {
                stopAsync().awaitTerminated();
            }

            @Override
            public CompletableFuture<Boolean> forceFlush(long upToSequenceNumber, Duration timeout) {
                return CompletableFuture.completedFuture(true);
            }
        }
    }


    private static class RefCountByteArraySegment extends ByteArraySegment {
        private final AtomicInteger refCount = new AtomicInteger();
        private final AtomicBoolean retained = new AtomicBoolean();

        RefCountByteArraySegment(byte[] array) {
            super(array);
        }

        int getRefCount() {
            return this.refCount.get();
        }

        boolean wasRetained() {
            return this.retained.get();
        }

        @Override
        public void retain() {
            this.refCount.incrementAndGet();
            this.retained.set(true);
        }

        @Override
        public void release() {
            this.refCount.decrementAndGet();
        }
    }

    //endregion
}
