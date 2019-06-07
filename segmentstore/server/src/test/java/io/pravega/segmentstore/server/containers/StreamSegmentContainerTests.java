/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
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
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.reading.TestReadResultHandler;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
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
import java.util.function.Consumer;
import java.util.function.Function;
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

/**
 * Tests for StreamSegmentContainer class.
 * These are not really unit tests. They are more like integration/end-to-end tests, since they test a real StreamSegmentContainer
 * using a real DurableLog, real ReadIndex and real StorageWriter - but all against in-memory mocks of Storage and
 * DurableDataLog.
 */
public class StreamSegmentContainerTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_COUNT = 100;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int ATTRIBUTE_UPDATES_PER_SEGMENT = 50;
    private static final int CONTAINER_ID = 1234567;
    private static final int EXPECTED_PINNED_SEGMENT_COUNT = 1;
    private static final long EXPECTED_METADATA_SEGMENT_ID = 1L;
    private static final String EXPECTED_METADATA_SEGMENT_NAME = StreamSegmentNameUtils.getMetadataSegmentName(CONTAINER_ID);
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final int TEST_TIMEOUT_MILLIS = 100 * 1000;
    private static final int EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT = 250; // Good for majority of tests.
    private static final int EVICTION_SEGMENT_EXPIRATION_MILLIS_LONG = 4 * EVICTION_SEGMENT_EXPIRATION_MILLIS_SHORT; // For heavy tests.
    private static final Duration TIMEOUT = Duration.ofMillis(TEST_TIMEOUT_MILLIS);
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
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

    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
            .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
            .build();

    private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .build();

    @Rule
    public Timeout globalTimeout = Timeout.millis(TEST_TIMEOUT_MILLIS);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the createSegment, append, updateAttributes, read, getSegmentInfo, getActiveSegments.
     */
    @Test
    public void testSegmentRegularOperations() throws Exception {
        final UUID attributeAccumulate = UUID.randomUUID();
        final UUID attributeReplace = UUID.randomUUID();
        final UUID attributeReplaceIfGreater = UUID.randomUUID();
        final UUID attributeReplaceIfEquals = UUID.randomUUID();
        final UUID attributeNoUpdate = UUID.randomUUID();
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
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals,
                        i == 0 ? AttributeUpdateType.Replace : AttributeUpdateType.ReplaceIfEquals, i + 1, i));
                byte[] appendData = getAppendData(segmentName, i);
                opFutures.add(context.container.append(segmentName, appendData, attributeUpdates, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
            }
        }

        // 2.1 Update some of the attributes.
        for (String segmentName : segmentNames) {
            // Record a one-off update.
            opFutures.add(context.container.updateAttributes(
                    segmentName,
                    Collections.singleton(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, expectedAttributeValue)),
                    TIMEOUT));

            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
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
        }

        checkActiveSegments(context.container, segmentNames.size());

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

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
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, Long> truncationOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                opFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                long truncateOffset = truncationOffsets.getOrDefault(segmentName, 0L) + appendData.length / 2 + 1;
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
        }

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, truncationOffsets, context);

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to set attributes (via append() or updateAttributes()), then fetch them back using getAttributes(),
     * emphasizing on Extended Attributes that are dumped into Storage and cleared from memory.
     */
    @Test
    public void testAttributes() throws Exception {
        final List<UUID> extendedAttributes = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
        final UUID coreAttribute = Attributes.EVENT_COUNT;
        final List<UUID> allAttributes = Stream.concat(extendedAttributes.stream(), Stream.of(coreAttribute)).collect(Collectors.toList());
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
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

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(localContainer);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                Collection<AttributeUpdate> attributeUpdates = allAttributes
                        .stream()
                        .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                        .collect(Collectors.toList());
                opFutures.add(localContainer.append(segmentName, getAppendData(segmentName, i), attributeUpdates, TIMEOUT));
            }
        }

        // 2.1 Update some of the attributes.
        for (String segmentName : segmentNames) {
            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                Collection<AttributeUpdate> attributeUpdates = allAttributes
                        .stream()
                        .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                        .collect(Collectors.toList());
                opFutures.add(localContainer.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }
        }

        Futures.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo
        for (String segmentName : segmentNames) {
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
        }

        // Force these segments out of memory, so that we may verify that extended attributes are still recoverable.
        localContainer.triggerMetadataCleanup(segmentNames).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        for (String segmentName : segmentNames) {
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
            val missingAttributeId = UUID.randomUUID();
            val attributesToCache = new ArrayList<UUID>(allAttributes);
            attributesToCache.add(missingAttributeId);
            val attributesToCacheValues = new HashMap<UUID, Long>(allAttributeValues);
            attributesToCacheValues.put(missingAttributeId, Attributes.NULL_ATTRIBUTE_VALUE);
            Map<UUID, Long> allAttributeValuesWithCache;
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

        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to run attribute iterators over all or a subset of attributes in a segment.
     */
    @Test
    public void testAttributeIterators() throws Exception {
        final List<UUID> sortedAttributes = IntStream.range(0, 100).mapToObj(i -> new UUID(i, i)).sorted().collect(Collectors.toList());
        final Map<UUID, Long> expectedValues = sortedAttributes.stream().collect(Collectors.toMap(id -> id, UUID::getMostSignificantBits));
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
        localContainer.createStreamSegment(segmentName, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val segment1 = localContainer.forSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2. Set some initial attribute values and verify in-memory iterator.
        Collection<AttributeUpdate> attributeUpdates = sortedAttributes
                .stream()
                .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Replace, expectedValues.get(attributeId)))
                .collect(Collectors.toList());
        segment1.updateAttributes(attributeUpdates, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment1, sortedAttributes, expectedValues);

        // 3. Force these segments out of memory and verify out-of-memory iterator.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val segment2 = localContainer.forSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment2, sortedAttributes, expectedValues);

        // 4. Update some values, and verify mixed iterator.
        attributeUpdates.clear();
        for (int i = 0; i < sortedAttributes.size(); i += 3) {
            UUID attributeId = sortedAttributes.get(i);
            expectedValues.put(attributeId, expectedValues.get(attributeId) + 1);
            attributeUpdates.add(new AttributeUpdate(attributeId, AttributeUpdateType.Replace, expectedValues.get(attributeId)));
        }
        segment2.updateAttributes(attributeUpdates, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkAttributeIterators(segment2, sortedAttributes, expectedValues);

        localContainer.stopAsync().awaitTerminated();
    }

    /**
     * Test conditional updates for Extended Attributes when they are not loaded in memory (i.e., they will need to be
     * auto-fetched from the AttributeIndex so that the operation may succeed).
     */
    @Test
    public void testExtendedAttributesConditionalUpdates() throws Exception {
        final UUID ea1 = new UUID(0, 1);
        final UUID ea2 = new UUID(0, 2);
        final List<UUID> allAttributes = Stream.of(ea1, ea2).collect(Collectors.toList());

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
            Collection<AttributeUpdate> attributeUpdates = allAttributes
                    .stream()
                    .map(attributeId -> new AttributeUpdate(attributeId, AttributeUpdateType.Accumulate, 1))
                    .collect(Collectors.toList());
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
                                Collections.singleton(new AttributeUpdate(ea1, AttributeUpdateType.ReplaceIfEquals, set, compare - 1)), TIMEOUT),
                        ex -> (ex instanceof BadAttributeUpdateException) && !((BadAttributeUpdateException) ex).isPreviousValueMissing());
                AssertExtensions.assertSuppliedFutureThrows(
                        "Conditional update-attributes succeeded with incorrect compare value.",
                        () -> localContainer.updateAttributes(segmentName,
                                Collections.singleton(new AttributeUpdate(ea2, AttributeUpdateType.ReplaceIfEquals, set, compare - 1)), TIMEOUT),
                        ex -> (ex instanceof BadAttributeUpdateException) && !((BadAttributeUpdateException) ex).isPreviousValueMissing());

            }

            opFutures.add(localContainer.append(segmentName, getAppendData(segmentName, 0),
                    Collections.singleton(new AttributeUpdate(ea1, AttributeUpdateType.ReplaceIfEquals, set, compare)), TIMEOUT));
            opFutures.add(localContainer.updateAttributes(segmentName,
                    Collections.singleton(new AttributeUpdate(ea2, AttributeUpdateType.ReplaceIfEquals, set, compare)), TIMEOUT));
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
        final UUID attributeAccumulate = UUID.randomUUID();
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
                Collection<AttributeUpdate> attributeUpdates = Collections.singleton(
                        new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                byte[] appendData = new byte[appendLength];
                Arrays.fill(appendData, (byte) (fillValue + 1));
                opFutures.add(context.container.append(segmentName, appendData, attributeUpdates, TIMEOUT));
                expectedLength.addAndGet(appendData.length);
            }));
        }

        // 2.1 Update the attribute.
        for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
            submitFutures.add(testExecutor.submit(() -> {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
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
            ReadResultEntryContents readEntryContents = readEntry.getContent().join();
            AssertExtensions.assertLessThanOrEqual("Too much to read.", actualData.length, offset + actualData.length);
            StreamHelpers.readAll(readEntryContents.getData(), actualData, offset, actualData.length);
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
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        int appendId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                long offset = lengths.getOrDefault(segmentName, 0L);
                appendFutures.add(context.container.append(segmentName, offset, appendData, null, TIMEOUT));

                lengths.put(segmentName, offset + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
                appendId++;
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2.1 Verify that if we pass wrong offsets, the append is failed.
        for (String segmentName : segmentNames) {
            byte[] appendData = getAppendData(segmentName, appendId);
            long offset = lengths.get(segmentName) + (appendId % 2 == 0 ? 1 : -1);

            AssertExtensions.assertSuppliedFutureThrows(
                    "append did not fail with the appropriate exception when passed a bad offset.",
                    () -> context.container.append(segmentName, offset, appendData, null, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);
            appendId++;
        }

        // 3. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

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
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();

        for (String segmentName : segmentNames) {
            ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
            segmentContents.put(segmentName, segmentStream);
            for (int i = 0; i < appendsPerSegment; i++) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                segmentStream.write(appendData);
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
                        context.container.append(segmentName, "foo".getBytes(), null, TIMEOUT)::join,
                        ex -> ex instanceof StreamSegmentSealedException);
            } else {
                Assert.assertFalse("Segment is sealed when it shouldn't be " + segmentName, sp.isSealed());

                // Verify we can still append to these segments.
                byte[] appendData = "foo".getBytes();
                context.container.append(segmentName, appendData, null, TIMEOUT).join();
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
                    Assert.assertEquals("Unexpected value for isEndOfStreamSegment when reaching the end of sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    AssertExtensions.assertSuppliedFutureThrows(
                            "ReadResultEntry.getContent() returned a result when reached the end of sealed segment " + segmentName,
                            readEntry::getContent,
                            ex -> ex instanceof IllegalStateException);
                } else {
                    Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment before reaching end of sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                    ReadResultEntryContents readEntryContents = readEntry.getContent().join();
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
                context.container.append(segmentName, "foo".getBytes(), null, TIMEOUT)::join,
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
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();

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
                            context.container.append(sn, "foo".getBytes(), null, TIMEOUT)::join,
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
                    context.container.append(sn, "foo".getBytes(), null, TIMEOUT).join();

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
        mergeTransactions(transactionsBySegment, lengths, segmentContents, context);

        // 4. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                // Verify that we can no longer append to Transaction.
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    AssertExtensions.assertThrows(
                            "An append was allowed to a merged Transaction " + transactionName,
                            context.container.append(transactionName, "foo".getBytes(), null, TIMEOUT)::join,
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
        mergeTransactions(transactionsBySegment, lengths, segmentContents, context);

        // 5. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Void>> operationFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                operationFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
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
        final UUID[] attributes = new UUID[]{Attributes.CREATION_TIME, Attributes.EVENT_COUNT, UUID.randomUUID()};
        final byte[] appendData = "hello".getBytes();
        Map<UUID, Long> expectedAttributes = new HashMap<>();

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
        localContainer.createStreamSegment(segmentName, initialAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after segment creation.", expectedAttributes, sp);

        // Add one append with some attribute changes and verify they were set correctly.
        val appendAttributes = createAttributeUpdates(attributes);
        applyAttributes(appendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData, appendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after append.", expectedAttributes, sp);

        // Wait until the segment is forgotten.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now get attributes again and verify them.
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        expectedAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp);

        // Append again, and make sure we can append at the right offset.
        val secondAppendAttributes = createAttributeUpdates(attributes);
        applyAttributes(secondAppendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData.length, appendData, secondAppendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length from segment after eviction & resurrection.", 2 * appendData.length, sp.getLength());
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp);

        // Seal.
        localContainer.sealStreamSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after seal.", expectedAttributes, sp);

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
        localContainer.createStreamSegment(segmentName, newAttributes, TIMEOUT)
                      .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        expectedAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after deletion and re-creation.", expectedAttributes, sp);
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
        final byte[] appendData = "hello".getBytes();

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
            container1.createStreamSegment(segmentName, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
            localContainer.createStreamSegment(name, null, TIMEOUT).join();
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
        final UUID[] attributes = new UUID[]{Attributes.EVENT_COUNT, new UUID(0, 1), new UUID(0, 2), new UUID(0, 3)};
        Map<UUID, Long> expectedAttributes = new HashMap<>();

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
        localContainer.createStreamSegment(segmentName, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Add one append with some attribute changes and verify they were set correctly.
        val appendAttributes = createAttributeUpdates(attributes);
        applyAttributes(appendAttributes, expectedAttributes);
        localContainer.updateAttributes(segmentName, appendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after initial updateAttributes() call.", expectedAttributes, sp);

        // Wait until the attributes are forgotten
        localContainer.triggerAttributeCleanup(segmentName).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now get attributes again and verify them.
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val coreAttributes = Attributes.getCoreNonNullAttributes(expectedAttributes); // We expect extended attributes to be dropped in this case.
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction.", coreAttributes, sp);

        val allAttributes = localContainer.getAttributes(segmentName, expectedAttributes.keySet(), true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertMapEquals("Unexpected attributes after eviction & reload.", expectedAttributes, allAttributes);
        sp = localContainer.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & reload+getInfo.", expectedAttributes, sp);
    }

    /**
     * Tests the behavior of the SegmentContainer when another instance of the same container is activated and fences out
     * the first one.
     */
    @Test
    public void testWriteFenceOut() throws Exception {
        final Duration shutdownTimeout = Duration.ofSeconds(5);
        @Cleanup
        TestContext context = createContext();
        val container1 = context.container;
        container1.startAsync().awaitRunning();
        val segmentNames = createSegments(context);
        @Cleanup
        val container2 = context.containerFactory.createStreamSegmentContainer(CONTAINER_ID);
        container2.startAsync().awaitRunning();

        AssertExtensions.assertSuppliedFutureThrows(
                "Original container did not reject an append operation after being fenced out.",
                () -> container1.append(segmentNames.get(0), new byte[1], null, TIMEOUT),
                ex -> ex instanceof DataLogWriterNotPrimaryException      // Write fenced.
                        || ex instanceof ObjectClosedException            // Write accepted, but OperationProcessor shuts down while processing it.
                        || ex instanceof IllegalContainerStateException); // Write rejected due to Container not running.

        // Verify we can still write to the second container.
        container2.append(segmentNames.get(0), 0, new byte[1], null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify container1 is shutting down (give it some time to complete) and that it ends up in a Failed state.
        ServiceListeners.awaitShutdown(container1, shutdownTimeout, false);
        Assert.assertEquals("Container1 is not in a failed state after fence-out detected.", Service.State.FAILED, container1.state());
        Assert.assertTrue("Container1 did not fail with the correct exception.",
                Exceptions.unwrap(container1.failureCause()) instanceof DataLogWriterNotPrimaryException);
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
                creationFutures.add(container.createStreamSegment(segmentName, null, TIMEOUT));
            }

            // Wait for the segments to be created first.
            Futures.allOf(creationFutures).join();

            val opFutures = new ArrayList<CompletableFuture<Void>>();
            for (int i = 0; i < APPENDS_PER_SEGMENT / 2; i++) {
                for (String segmentName : segmentNames) {
                    byte[] appendData = getAppendData(segmentName, i);
                    opFutures.add(container.append(segmentName, appendData, null, TIMEOUT));
                    lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                    recordAppend(segmentName, appendData, segmentContents);
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
                    () -> container.append("foo", new byte[1], null, TIMEOUT),
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
                    byte[] appendData = getAppendData(segmentName, i);
                    opFutures.add(container.append(segmentName, appendData, null, TIMEOUT));
                    lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                    recordAppend(segmentName, appendData, segmentContents);
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
        byte[] data = getAppendData(segmentName, 0);

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
        context.container.createStreamSegment(segmentName, null, TIMEOUT).join();
        context.container.append(segmentName, data, null, TIMEOUT).join();
        val rawOp = operationProcessed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Unexpected operation type.", rawOp instanceof CachedStreamSegmentAppendOperation);

        // Our operation has been transformed into a CachedStreamSegmentAppendOperation, which means it just points to
        // a location in the cache. We do not have access to that cache, so we can only verify its metadata.
        val appendOp = (CachedStreamSegmentAppendOperation) rawOp;
        Assert.assertEquals("Unexpected offset.", 0, appendOp.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected data length.", data.length, appendOp.getLength());
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
    public void testForSegment() throws Exception {
        UUID attributeId1 = UUID.randomUUID();
        UUID attributeId2 = UUID.randomUUID();
        @Cleanup
        val context = createContext();
        context.container.startAsync().awaitRunning();

        // Create the StreamSegments.
        val segmentNames = createSegments(context);

        // Add some appends.
        for (String segmentName : segmentNames) {
            byte[] appendData = ("Append_" + segmentName).getBytes();

            val dsa = context.container.forSegment(segmentName, TIMEOUT).join();
            dsa.append(appendData, Collections.singleton(new AttributeUpdate(attributeId1, AttributeUpdateType.None, 1L)), TIMEOUT).join();
            dsa.updateAttributes(Collections.singleton(new AttributeUpdate(attributeId2, AttributeUpdateType.None, 2L)), TIMEOUT).join();
            dsa.seal(TIMEOUT).join();
            dsa.truncate(1, TIMEOUT).join();

            // Check metadata.
            val info = dsa.getInfo();
            Assert.assertEquals("Unexpected name.", segmentName, info.getName());
            Assert.assertEquals("Unexpected length.", appendData.length, info.getLength());
            Assert.assertEquals("Unexpected startOffset.", 1, info.getStartOffset());
            Assert.assertEquals("Unexpected attribute count.", 2, info.getAttributes().size());
            Assert.assertEquals("Unexpected attribute 1.", 1L, (long) info.getAttributes().get(attributeId1));
            Assert.assertEquals("Unexpected attribute 2.", 2L, (long) info.getAttributes().get(attributeId2));
            Assert.assertTrue("Unexpected isSealed.", info.isSealed());

            // Check written data.
            byte[] readBuffer = new byte[appendData.length - 1];
            @Cleanup
            val readResult = dsa.read(1, readBuffer.length, TIMEOUT);
            val firstEntry = readResult.next();
            firstEntry.requestContent(TIMEOUT);
            val entryContents = firstEntry.getContent().join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, entryContents.getLength());
            StreamHelpers.readAll(entryContents.getData(), readBuffer, 0, readBuffer.length);
            AssertExtensions.assertArrayEquals("Unexpected data read back.", appendData, 1, readBuffer, 0, readBuffer.length);
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
        for (String segmentName : segmentContents.keySet()) {
            // 1. Deletion status
            SegmentProperties sp = null;
            try {
                sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                Assert.assertFalse(
                        "Segment is marked as deleted in metadata but was not deleted in Storage " + segmentName,
                        context.storage.exists(segmentName, TIMEOUT).join());

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            SegmentProperties storageProps = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Metadata and Storage for segment " + segmentName, sp.isSealed(), storageProps.isSealed());

            // 3. Contents.
            long expectedLength = lengths.get(segmentName);
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedLength, storageProps.getLength());

            byte[] expectedData = segmentContents.get(segmentName).toByteArray();
            byte[] actualData = new byte[expectedData.length];
            val readHandle = context.storage.openRead(segmentName).join();
            int actualLength = context.storage.read(readHandle, 0, actualData, 0, actualData.length, TIMEOUT).join();
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
        waitForOperationsInReadIndex(context.container);
        for (String segmentName : segmentContents.keySet()) {
            long segmentLength = lengths.get(segmentName);
            long startOffset = truncationOffsets.getOrDefault(segmentName, 0L);
            val si = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Unexpected Metadata StartOffset for segment " + segmentName, startOffset, si.getStartOffset());
            Assert.assertEquals("Unexpected Metadata Length for segment " + segmentName, segmentLength, si.getLength());
            byte[] expectedData = segmentContents.get(segmentName).toByteArray();

            long expectedCurrentOffset = startOffset;
            @Cleanup
            ReadResult readResult = context.container.read(segmentName, expectedCurrentOffset, (int) (segmentLength - startOffset), TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in testSegmentRegularOperations; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName, expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset, expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

    private void checkActiveSegments(SegmentContainer container, int expectedCount) {
        val initialActiveSegments = container.getActiveSegments();
        int ignoredSegments = 0;
        for (SegmentProperties sp : initialActiveSegments) {
            if (sp.getName().equals(EXPECTED_METADATA_SEGMENT_NAME)) {
                ignoredSegments++;
                continue;
            }

            val expectedSp = container.getStreamSegmentInfo(sp.getName(), TIMEOUT).join();
            Assert.assertEquals("Unexpected length (from getActiveSegments) for segment " + sp.getName(), expectedSp.getLength(), sp.getLength());
            Assert.assertEquals("Unexpected sealed (from getActiveSegments) for segment " + sp.getName(), expectedSp.isSealed(), sp.isSealed());
            Assert.assertEquals("Unexpected deleted (from getActiveSegments) for segment " + sp.getName(), expectedSp.isDeleted(), sp.isDeleted());
            SegmentMetadataComparer.assertSameAttributes("Unexpected attributes (from getActiveSegments) for segment " + sp.getName(),
                    expectedSp.getAttributes(), sp);
        }

        Assert.assertEquals("Unexpected result from getActiveSegments with freshly created segments.",
                expectedCount + ignoredSegments, initialActiveSegments.size());
    }

    private void checkAttributeIterators(DirectSegmentAccess segment, List<UUID> sortedAttributes, Map<UUID, Long> allExpectedValues) throws Exception {
        int skip = sortedAttributes.size() / 10;
        for (int i = 0; i < sortedAttributes.size() / 2; i += skip) {
            UUID fromId = sortedAttributes.get(i);
            UUID toId = sortedAttributes.get(sortedAttributes.size() - i - 1);
            val expectedValues = allExpectedValues
                    .entrySet().stream()
                    .filter(e -> fromId.compareTo(e.getKey()) <= 0 && toId.compareTo(e.getKey()) >= 0)
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .collect(Collectors.toList());

            val actualValues = new ArrayList<Map.Entry<UUID, Long>>();
            val ids = new HashSet<UUID>();
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

    private void appendToParentsAndTransactions(Collection<String> segmentNames, HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                boolean emptyTransaction = false;
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    if (!emptyTransaction) {
                        lengths.put(transactionName, 0L);
                        recordAppend(transactionName, new byte[0], segmentContents);
                        emptyTransaction = true;
                        continue;
                    }

                    appendData = getAppendData(transactionName, i);
                    appendFutures.add(context.container.append(transactionName, appendData, null, TIMEOUT));
                    lengths.put(transactionName, lengths.getOrDefault(transactionName, 0L) + appendData.length);
                    recordAppend(transactionName, appendData, segmentContents);
                }
            }
        }

        Futures.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                if (++i % 2 == 0) {
                    // Every other Merge operation, pre-seal the source. We want to verify we correctly handle this situation as well.
                    mergeFutures.add(Futures.toVoid(context.container.sealStreamSegment(transactionName, TIMEOUT)));
                }

                mergeFutures.add(Futures.toVoid(context.container.mergeStreamSegment(parentName, transactionName, TIMEOUT)));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        Futures.allOf(mergeFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private byte[] getAppendData(String segmentName, int appendId) {
        return String.format("%s_%d", segmentName, appendId).getBytes();
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
            futures.add(container.createStreamSegment(segmentName, null, TIMEOUT));
        }

        Futures.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, TestContext context) {
        // Create the Transaction.
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        HashMap<String, ArrayList<String>> transactions = new HashMap<>();
        for (String segmentName : segmentNames) {
            val txnList = new ArrayList<String>(TRANSACTIONS_PER_SEGMENT);
            transactions.put(segmentName, txnList);
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                String txnName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                txnList.add(txnName);
                futures.add(context.container.createStreamSegment(txnName, null, TIMEOUT));
            }
        }

        Futures.allOf(futures).join();
        return transactions;
    }

    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, TestContext context) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, context));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties metadataProps, TestContext context) {
        if (metadataProps.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        Function<SegmentProperties, Boolean> meetsConditions = storageProps ->
                storageProps.isSealed() == metadataProps.isSealed()
                        && storageProps.getLength() >= metadataProps.getLength()
                        && context.storageFactory.truncationOffsets.getOrDefault(metadataProps.getName(), 0L) >= metadataProps.getStartOffset();

        AtomicBoolean canContinue = new AtomicBoolean(true);
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        return Futures.loop(
                canContinue::get,
                () -> Futures.exceptionallyExpecting(
                        context.storage.getStreamSegmentInfo(metadataProps.getName(), TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(metadataProps.getName()).build())
                        .thenCompose(storageProps -> {
                            if (meetsConditions.apply(storageProps)) {
                                canContinue.set(false);
                                     return CompletableFuture.completedFuture(null);
                            } else if (!timer.hasRemaining()) {
                                return Futures.failedFuture(new TimeoutException());
                            } else {
                                return Futures.delayedFuture(Duration.ofMillis(10), executorService());
                            }
                        }).thenRun(Runnables.doNothing()),
                executorService());
    }

    private Collection<AttributeUpdate> createAttributeUpdates(UUID[] attributes) {
        return Arrays.stream(attributes)
                     .map(a -> new AttributeUpdate(a, AttributeUpdateType.Replace, System.nanoTime()))
                     .collect(Collectors.toList());
    }

    private void applyAttributes(Collection<AttributeUpdate> updates, Map<UUID, Long> target) {
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

    private CompletableFuture<Void> activateSegment(String segmentName, StreamSegmentStore container) {
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
        container.createStreamSegment(segmentName, null, timer.getRemaining())
                .thenCompose(v -> container.append(segmentName, new byte[1], null, timer.getRemaining()))
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
        private final CacheFactory cacheFactory;
        private final CacheManager cacheManager;
        private final Storage storage;

        TestContext(ContainerConfig config, SegmentContainerFactory.CreateExtensions createAdditionalExtensions) {
            this.storageFactory = new WatchableInMemoryStorageFactory(executorService());
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService());
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService());
            this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheFactory, this.cacheManager, executorService());
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheFactory, this.cacheManager, executorService());
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, executorService());
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
            return new ContainerTableExtensionImpl(c, this.cacheFactory, this.cacheManager, e);
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
        CompletableFuture<Void> triggerAttributeCleanup(String segmentName) {
            CompletableFuture<Void> cleanupTask = Futures.futureWithTimeout(TIMEOUT, this.executor);
            SegmentMetadata sm = super.metadata.getStreamSegmentMetadata(super.metadata.getStreamSegmentId(segmentName, false));

            // Inject this callback into the MetadataCleaner callback, which was setup for us in createMetadataCleaner().
            this.metadataCleanupFinishedCallback = ignored -> {
                boolean onlyCoreAttributes = sm.getAttributes().keySet().stream().allMatch(Attributes::isCoreAttribute);
                if (onlyCoreAttributes) {
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
            return (createSegment ? createStreamSegment(segmentName, null, TIMEOUT) : CompletableFuture.completedFuture(null))
                    .thenCompose(v -> Futures.loop(
                            canContinue,
                            () -> append(segmentName, appendData, null, TIMEOUT),
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
        public CompletableFuture<WriterFlushResult> flush(Duration timeout) {
            return CompletableFuture.completedFuture(new WriterFlushResult());
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
        }
    }

    //endregion
}
