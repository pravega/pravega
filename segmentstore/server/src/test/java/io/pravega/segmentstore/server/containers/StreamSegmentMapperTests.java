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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import io.pravega.common.MathHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for StreamSegmentMapper class.
 */
public class StreamSegmentMapperTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 123;
    private static final int ATTRIBUTE_COUNT = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the ability of the StreamSegmentMapper to create a new StreamSegment.
     */
    @Test
    public void testCreateNewStreamSegment() {
        final int segmentCount = 50;

        @Cleanup
        TestContext context = new TestContext();
        HashMap<String, SegmentRollingPolicy> storageSegments = new HashMap<>();
        HashMap<String, Integer> expectedRolloverSizes = new HashMap<>();
        setupStorageCreateHandler(context, storageSegments);
        setupStorageGetHandler(context, storageSegments.keySet(), segmentName -> StreamSegmentInformation.builder().name(segmentName).build());

        // Create some Segments and verify they are properly created and registered.
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            val segmentAttributes = createAttributes(ATTRIBUTE_COUNT);
            if (i % 2 == 0) {
                segmentAttributes.add(new AttributeUpdate(Attributes.ROLLOVER_SIZE, AttributeUpdateType.Replace, i + 100));
                expectedRolloverSizes.put(segmentName, i + 100);
            }

            context.mapper.createNewStreamSegment(segmentName, segmentAttributes, TIMEOUT).join();
            assertSegmentCreated(segmentName, segmentAttributes, context);
        }

        for (val e : storageSegments.entrySet()) {
            if (e.getValue() == null) {
                Assert.assertFalse("Segment was expected to have a rollover policy defined.", expectedRolloverSizes.containsKey(e.getKey()));
            } else {
                long expectedValue = expectedRolloverSizes.containsKey(e.getKey())
                        ? expectedRolloverSizes.get(e.getKey())
                        : SegmentRollingPolicy.NO_ROLLING.getMaxLength();
                Assert.assertEquals("Unexpected rollover policy set.", expectedValue, e.getValue().getMaxLength());
            }
        }
    }

    /**
     * Tests the ability of the StreamSegmentMapper to create a new StreamSegment if the Segment already exists (partially
     * or fully).
     */
    @Test
    public void testCreateSegmentAlreadyExists() {
        final String segmentName = "NewSegment";
        testCreateAlreadyExists(segmentName, (mapper, attributes) -> mapper.createNewStreamSegment(segmentName, attributes, TIMEOUT));
    }

    /**
     * Tests the ability of the StreamSegmentMapper to create a new StreamSegment if there are Storage and/or OperationLog Failures.
     * This tests failures other than StreamSegmentExistsException, which is handled in a different test.
     */
    @Test
    public void testCreateSegmentWithFailures() {
        final String segmentName = "NewSegment";

        @Cleanup
        TestContext context = new TestContext();
        context.storage.createHandler = (name, policy) -> Futures.failedFuture(new IntentionalException());
        AssertExtensions.assertSuppliedFutureThrows(
                "createNewStreamSegment did not fail when random exception was thrown.",
                () -> context.mapper.createNewStreamSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Tests GetStreamSegmentInfo with various scenarios.
     */
    @Test
    public void testGetStreamSegmentInfo() {
        final String segmentName = "segment";
        final long segmentId = 1;

        @Cleanup
        TestContext context = new TestContext();
        HashSet<String> storageSegments = new HashSet<>();

        // Segment not exists in Metadata or Storage.
        setupStorageGetHandler(context, storageSegments, sn -> {
            throw new CompletionException(new StreamSegmentNotExistsException(sn));
        });
        setSavedState(segmentName, segmentId, 0, ATTRIBUTE_COUNT, context);
        val segmentState = context.stateStore.get(segmentName, TIMEOUT).join();
        Map<UUID, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();

        AssertExtensions.assertSuppliedFutureThrows(
                "getStreamSegmentInfo did not throw correct exception when segment does not exist in Metadata or Storage.",
                () -> context.mapper.getStreamSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Segment does not exist in Metadata, but does so in Storage.
        // Since we do not setup an OperationLog, we guarantee that there is no attempt to map this in the metadata.
        val segmentInfo = StreamSegmentInformation.builder()
                                                  .name(segmentName)
                                                  .length(123)
                                                  .sealed(true)
                                                  .build();
        storageSegments.add(segmentName);
        setupStorageGetHandler(context, storageSegments, sn -> segmentInfo);
        val inStorageInfo = context.mapper.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        assertEquals("Unexpected SegmentInfo when Segment exists in Storage.", segmentInfo, inStorageInfo);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes when Segment exists in Storage", expectedAttributes, inStorageInfo);
        Assert.assertEquals("Not expecting any segments to be mapped.", 0, context.metadata.getAllStreamSegmentIds().size());

        // Segment exists in Metadata (and in Storage too) - here, we set different values in the Metadata to verify that
        // the info is fetched from there.
        val sm = context.metadata.mapStreamSegmentId(segmentName, segmentId);
        sm.setLength(segmentInfo.getLength() + 1);
        sm.updateAttributes(Collections.singletonMap(UUID.randomUUID(), 12345L));
        val inMetadataInfo = context.mapper.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        assertEquals("Unexpected SegmentInfo when Segment exists in Metadata.", sm, inMetadataInfo);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes when Segment exists in Metadata.",
                sm.getAttributes(), inMetadataInfo);

        // Segment exists in Metadata, but is marked as deleted.
        sm.markDeleted();
        AssertExtensions.assertSuppliedFutureThrows(
                "getStreamSegmentInfo did not throw correct exception when segment is marked as Deleted in metadata.",
                () -> context.mapper.getStreamSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests GetStreamSegmentInfo when it is invoked in parallel with a Segment assignment.
     */
    @Test
    public void testGetStreamSegmentInfoWithConcurrency() throws Exception {
        final String segmentName = "Segment";
        final long segmentId = 1;
        final SegmentProperties storageInfo = StreamSegmentInformation.builder().name(segmentName).length(123).sealed(true).build();
        final long metadataLength = storageInfo.getLength() + 1;

        HashSet<String> storageSegments = new HashSet<>();
        storageSegments.add(segmentName);

        @Cleanup
        TestContext context = new TestContext();
        AtomicInteger storageGetCount = new AtomicInteger();
        setupStorageGetHandler(context, storageSegments, sn -> {
            storageGetCount.incrementAndGet();
            return storageInfo;
        });
        setSavedState(segmentName, segmentId, 0L, ATTRIBUTE_COUNT, context);
        val segmentState = context.stateStore.get(segmentName, TIMEOUT).join();
        Map<UUID, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();

        CompletableFuture<Void> addInvoked = new CompletableFuture<>();
        context.operationLog.addHandler = op -> {
            addInvoked.join();
            // Need to set SegmentId on operation.
            StreamSegmentMapOperation sop = (StreamSegmentMapOperation) op;
            UpdateableSegmentMetadata segmentMetadata = context.metadata.mapStreamSegmentId(segmentName, segmentId);
            segmentMetadata.setStorageLength(sop.getLength());
            segmentMetadata.setLength(metadataLength);
            segmentMetadata.updateAttributes(expectedAttributes);
            if (sop.isSealed()) {
                segmentMetadata.markSealed();
            }

            return CompletableFuture.completedFuture(null);
        };

        // Second call is designed to hit when the first call still tries to assign the id, hence we test normal queueing.
        context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT, id -> CompletableFuture.completedFuture(null));

        // Concurrently with the map, request a Segment Info.
        CompletableFuture<SegmentProperties> segmentInfoFuture = context.mapper.getStreamSegmentInfo(segmentName, TIMEOUT);
        Assert.assertFalse("getSegmentInfo returned a completed future.", segmentInfoFuture.isDone());

        // Release the OperationLog add and verify the Segment Info has been served with information from the Metadata.
        addInvoked.complete(null);
        SegmentProperties segmentInfo = segmentInfoFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val expectedInfo = context.metadata.getStreamSegmentMetadata(segmentId);
        assertEquals("Unexpected Segment Info returned.", expectedInfo, segmentInfo);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes returned.",
                expectedInfo.getAttributes(), segmentInfo);
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment, as well as
     * retrieving existing attributes.
     */
    @Test
    public void testGetOrAssignStreamSegmentId() {
        final long minSegmentLength = 1;
        final int segmentCount = 50;
        final long noSegmentId = ContainerMetadata.NO_STREAM_SEGMENT_ID;
        AtomicLong currentSegmentId = new AtomicLong(Integer.MAX_VALUE);
        Supplier<Long> nextSegmentId = () -> currentSegmentId.decrementAndGet() % 2 == 0 ? noSegmentId : currentSegmentId.get();
        Function<String, Long> getSegmentLength = segmentName -> minSegmentLength + (long) MathHelpers.abs(segmentName.hashCode());
        Function<String, Long> getSegmentStartOffset = segmentName -> getSegmentLength.apply(segmentName) / 2;

        @Cleanup
        TestContext context = new TestContext();

        HashSet<String> storageSegments = new HashSet<>();
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            storageSegments.add(segmentName);
            setSavedState(segmentName, nextSegmentId.get(), getSegmentStartOffset.apply(segmentName), storageSegments.size() % ATTRIBUTE_COUNT, context);
        }

        // We setup all necessary handlers, except the one for create. We do not need to create new Segments here.
        setupOperationLog(context);
        Predicate<String> isSealed = segmentName -> segmentName.hashCode() % 2 == 0;
        setupStorageGetHandler(context, storageSegments, segmentName ->
                StreamSegmentInformation.builder()
                                        .name(segmentName)
                                        .length(getSegmentLength.apply(segmentName))
                                        .sealed(isSealed.test(segmentName))
                                        .build());

        for (String name : storageSegments) {
            long id = context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT).join();
            Assert.assertNotEquals("No id was assigned for StreamSegment " + name, ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
            SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(id);
            Assert.assertNotNull("No metadata was created for StreamSegment " + name, sm);
            long expectedLength = getSegmentLength.apply(name);
            boolean expectedSeal = isSealed.test(name);
            Assert.assertEquals("Metadata does not have the expected length for StreamSegment " + name, expectedLength, sm.getLength());
            Assert.assertEquals("Metadata does not have the expected value for isSealed for StreamSegment " + name, expectedSeal, sm.isSealed());

            val segmentState = context.stateStore.get(name, TIMEOUT).join();
            Map<UUID, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();
            SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in metadata for StreamSegment " + name, expectedAttributes, sm);
            long expectedStartOffset = segmentState == null ? 0 : segmentState.getStartOffset();
            Assert.assertEquals("Unexpected StartOffset in metadata for " + name, expectedStartOffset, sm.getStartOffset());
        }
    }

    /**
     * Tests the behavior of getOrAssignStreamSegmentId when the requested StreamSegment has been deleted.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWhenDeleted() {
        final int segmentCount = 1;

        HashSet<String> storageSegments = new HashSet<>();
        for (int i = 0; i < segmentCount; i++) {
            storageSegments.add(getName(i));
        }

        // We setup all necessary handlers, except the one for create. We do not need to create new Segments here.
        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context);
        setupStorageGetHandler(context, storageSegments, segmentName -> StreamSegmentInformation.builder().name(segmentName).build());

        // Map all the segments, then delete them, then verify behavior.
        for (String name : storageSegments) {
            context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT).join();
            context.metadata.deleteStreamSegment(name);
            AssertExtensions.assertSuppliedFutureThrows(
                    "getOrAssignStreamSegmentId did not return appropriate exception when the segment has been deleted.",
                    () -> context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment, when dealing
     * with Storage failures (or inexistent StreamSegments).
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithFailures() {
        final String segmentName = "Segment";

        HashSet<String> storageSegments = new HashSet<>();
        storageSegments.add(segmentName);

        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context);

        // 1. Unable to access storage.
        context.storage.getInfoHandler = sn -> Futures.failedFuture(new IntentionalException());
        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignStreamSegmentId did not throw the right exception when the Storage access failed.",
                () -> context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT),
                ex -> ex instanceof IntentionalException);

        // 2. StreamSegmentNotExists
        setupStorageGetHandler(context, storageSegments, sn -> StreamSegmentInformation.builder().name(sn).build());
        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a non-existent StreamSegment.",
                () -> context.mapper.getOrAssignStreamSegmentId(segmentName + "foo", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 3. Attribute fetch failure.
        val testStateStore = new TestStateStore();
        val badMapper = new StreamSegmentMapper(context.metadata, context.operationLog, testStateStore, context.noOpMetadataCleanup, context.storage, executorService());
        val segmentName2 = segmentName + "2";
        context.storage.getInfoHandler = sn -> CompletableFuture.completedFuture(StreamSegmentInformation.builder().name(sn).build());
        testStateStore.getHandler = () -> Futures.failedFuture(new IntentionalException("intentional"));

        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a Segment when attributes could not be retrieved.",
                () -> badMapper.getOrAssignStreamSegmentId(segmentName2, TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Tests the ability of getOrAssignStreamSegmentId to handle the TooManyActiveSegmentsException.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithMetadataLimit() {
        List<String> storageSegments = Arrays.asList("S1", "S2", "S3");

        @Cleanup
        TestContext context = new TestContext();
        setupStorageGetHandler(context, new HashSet<>(storageSegments), name -> StreamSegmentInformation.builder().name(name).build());

        // 1. Verify the behavior when even after the retry we still cannot map.
        AtomicInteger exceptionCounter = new AtomicInteger();
        AtomicBoolean cleanupInvoked = new AtomicBoolean();

        // We use 'containerId' as a proxy for the exception id (to make sure we collect the right one).
        context.operationLog.addHandler = op -> Futures.failedFuture(new TooManyActiveSegmentsException(exceptionCounter.incrementAndGet(), 0));
        Supplier<CompletableFuture<Void>> noOpCleanup = () -> {
            if (!cleanupInvoked.compareAndSet(false, true)) {
                return Futures.failedFuture(new AssertionError("Cleanup invoked multiple times."));
            }
            return CompletableFuture.completedFuture(null);
        };
        val mapper1 = new StreamSegmentMapper(context.metadata, context.operationLog, context.stateStore, noOpCleanup, context.storage, executorService());
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected outcome when trying to map a segment to a full metadata that cannot be cleaned.",
                () -> mapper1.getOrAssignStreamSegmentId(storageSegments.get(0), TIMEOUT),
                ex -> ex instanceof TooManyActiveSegmentsException && ((TooManyActiveSegmentsException) ex).getContainerId() == exceptionCounter.get());
        Assert.assertEquals("Unexpected number of attempts to map.", 2, exceptionCounter.get());
        Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());

        // Now with a Segment 3.
        exceptionCounter.set(0);
        cleanupInvoked.set(false);
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected outcome when trying to map a Segment to a full metadata that cannot be cleaned.",
                () -> mapper1.getOrAssignStreamSegmentId(storageSegments.get(2), TIMEOUT),
                ex -> ex instanceof TooManyActiveSegmentsException && ((TooManyActiveSegmentsException) ex).getContainerId() == exceptionCounter.get());
        Assert.assertEquals("Unexpected number of attempts to map.", 2, exceptionCounter.get());
        Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());

        // 2. Verify the behavior when the first call fails, but the second one succeeds.
        exceptionCounter.set(0);
        cleanupInvoked.set(false);
        Supplier<CompletableFuture<Void>> workingCleanup = () -> {
            if (!cleanupInvoked.compareAndSet(false, true)) {
                return Futures.failedFuture(new AssertionError("Cleanup invoked multiple times."));
            }

            setupOperationLog(context); // Setup the OperationLog to function correctly.
            return CompletableFuture.completedFuture(null);
        };

        val mapper2 = new StreamSegmentMapper(context.metadata, context.operationLog, context.stateStore, workingCleanup, context.storage, executorService());
        long id = mapper2.getOrAssignStreamSegmentId(storageSegments.get(0), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of attempts to map.", 1, exceptionCounter.get());
        Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());
        Assert.assertNotEquals("No valid SegmentId assigned.", ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment, with concurrent requests.
     * Also tests the ability to execute such callbacks in the order in which they were received.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithConcurrency() {
        final String segmentName = "Segment";
        final long segmentId = 12345;
        final String firstResult = "first";
        final String secondResult = "second";
        final String thirdResult = "third";

        HashSet<String> storageSegments = new HashSet<>();
        storageSegments.add(segmentName);

        @Cleanup
        TestContext context = new TestContext();
        setupStorageGetHandler(context, storageSegments, sn -> StreamSegmentInformation.builder().name(sn).build());
        CompletableFuture<Void> initialAddFuture = new CompletableFuture<>();
        CompletableFuture<Void> addInvoked = new CompletableFuture<>();
        AtomicBoolean operationLogInvoked = new AtomicBoolean(false);
        context.operationLog.addHandler = op -> {
            if (!(op instanceof StreamSegmentMapOperation)) {
                return Futures.failedFuture(new IllegalArgumentException("unexpected operation"));
            }
            if (operationLogInvoked.getAndSet(true)) {
                return Futures.failedFuture(new IllegalStateException("multiple calls to OperationLog.add"));
            }

            // Need to set SegmentId on operation.
            ((StreamSegmentMapOperation) op).setStreamSegmentId(segmentId);
            UpdateableSegmentMetadata segmentMetadata = context.metadata.mapStreamSegmentId(segmentName, segmentId);
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setLength(0);
            addInvoked.complete(null);
            return initialAddFuture;
        };

        List<Integer> invocationOrder = Collections.synchronizedList(new ArrayList<>());

        // Second call is designed to hit when the first call still tries to assign the id, hence we test normal queueing.
        CompletableFuture<String> firstCall = context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (first).", segmentId, (long) id);
                    invocationOrder.add(1);
                    return CompletableFuture.completedFuture(firstResult);
                });

        CompletableFuture<String> secondCall = context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (second).", segmentId, (long) id);
                    invocationOrder.add(2);
                    return CompletableFuture.completedFuture(secondResult);
                });

        // Wait for the metadata to be updated properly.
        addInvoked.join();
        Assert.assertFalse("getOrAssignStreamSegmentId (first call) returned before OperationLog finished.", firstCall.isDone());
        Assert.assertFalse("getOrAssignStreamSegmentId (second call) returned before OperationLog finished.", secondCall.isDone());

        // Third call is designed to hit after the metadata has been updated, but prior to the other callbacks being invoked.
        // It verifies that even in that case it still executes in order.
        CompletableFuture<String> thirdCall = context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (second).", segmentId, (long) id);
                    invocationOrder.add(3);
                    return CompletableFuture.completedFuture(thirdResult);
                });
        initialAddFuture.complete(null);

        Assert.assertEquals("Unexpected result from firstCall.", firstResult, firstCall.join());
        Assert.assertEquals("Unexpected result from secondCall.", secondResult, secondCall.join());
        Assert.assertEquals("Unexpected result from thirdCall.", thirdResult, thirdCall.join());
        val expectedOrder = Arrays.asList(1, 2, 3);
        AssertExtensions.assertListEquals("", expectedOrder, invocationOrder, Integer::equals);
    }

    /**
     * General test for verifying behavior when a Segment is attempted to be created but it already exists.
     *
     * @param segmentName   The name of the segment to create.
     * @param createSegment A BiFunction that is given an instance of a StreamSegmentMapper and a Collection of AttributeUpdates
     *                      that, when invoked, will create the given segment.
     */
    private void testCreateAlreadyExists(String segmentName, BiFunction<StreamSegmentMapper, Collection<AttributeUpdate>, CompletableFuture<?>> createSegment) {
        final String stateSegmentName = StreamSegmentNameUtils.getStateSegmentName(segmentName);
        final Map<UUID, Long> originalAttributes = ImmutableMap.of(UUID.randomUUID(), 123L, Attributes.EVENT_COUNT, 1L);
        final Map<UUID, Long> expectedAttributes = Attributes.getCoreNonNullAttributes(originalAttributes);
        final Collection<AttributeUpdate> correctAttributeUpdates =
                originalAttributes.entrySet().stream()
                                 .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                                 .collect(Collectors.toList());
        final Map<UUID, Long> badAttributes = Collections.singletonMap(UUID.randomUUID(), 456L);
        final Collection<AttributeUpdate> badAttributeUpdates =
                badAttributes.entrySet().stream()
                             .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                             .collect(Collectors.toList());

        @Cleanup
        TestContext context = new TestContext();
        @Cleanup
        val storage = InMemoryStorageFactory.newStorage(executorService());
        storage.initialize(1);
        val store = new SegmentStateStore(storage, executorService());
        val mapper = new StreamSegmentMapper(context.metadata, context.operationLog, store, context.noOpMetadataCleanup, storage, executorService());

        // 1. Segment Exists, and so does State File (and it's not corrupted) -> Exception must be bubbled up.
        createSegment.apply(mapper, correctAttributeUpdates).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "createNewStreamSegment did not fail when Segment already exists.",
                () -> createSegment.apply(mapper, badAttributeUpdates),
                ex -> ex instanceof StreamSegmentExistsException);
        val state1 = store.get(segmentName, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes after failed attempt to recreate correctly created segment",
                expectedAttributes, state1.getAttributes());

        // 2. Segment Exists, but with empty State File: State file re-created & no exception bubbled up.
        storage.openWrite(stateSegmentName)
               .thenCompose(handle -> storage.delete(handle, TIMEOUT))
               .thenCompose(v -> storage.create(stateSegmentName, TIMEOUT))
               .join();
        Assert.assertNull("Expected a null SegmentState.", store.get(segmentName, TIMEOUT).join());
        createSegment.apply(mapper, correctAttributeUpdates).join();
        val state2 = store.get(segmentName, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes after successful attempt to complete segment creation (missing state file)",
                expectedAttributes, state2.getAttributes());

        // 3. Segment Exists, but with corrupted State File: State file re-created & no exception bubbled up.
        storage.openWrite(stateSegmentName)
                .thenCompose(handle -> storage.delete(handle, TIMEOUT))
                .thenCompose(v -> storage.create(stateSegmentName, TIMEOUT))
                .thenCompose(v -> storage.openWrite(stateSegmentName))
                .thenCompose(handle -> storage.write(handle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT))
                .join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected a DataCorruptionException when reading a corrupted State File.",
                () -> store.get(segmentName, TIMEOUT),
                ex -> ex instanceof DataCorruptionException);
        createSegment.apply(mapper, correctAttributeUpdates).join();
        val state3 = store.get(segmentName, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes after successful attempt to complete segment creation (corrupted state file)",
                expectedAttributes, state3.getAttributes());

        // 4. Segment Exists with non-zero length, but with empty/corrupted State File: State File re-created and exception thrown.
        storage.openWrite(stateSegmentName)
               .thenCompose(handle -> storage.delete(handle, TIMEOUT))
               .thenCompose(v -> storage.create(stateSegmentName, TIMEOUT))
               .thenCompose(v -> storage.openWrite(segmentName))
               .thenCompose(handle -> storage.write(handle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT))
               .join();
        AssertExtensions.assertSuppliedFutureThrows(
                "createNewStreamSegment did not fail when Segment already exists (non-zero length, missing state file).",
                () -> createSegment.apply(mapper, correctAttributeUpdates),
                ex -> ex instanceof StreamSegmentExistsException);
        val state4 = store.get(segmentName, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes after failed attempt to recreate segment with non-zero length",
                expectedAttributes, state4.getAttributes());
    }

    private String getName(long segmentId) {
        return String.format("Segment_%d", segmentId);
    }

    private Collection<AttributeUpdate> createAttributes(int count) {
        Collection<AttributeUpdate> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            AttributeUpdateType ut = AttributeUpdateType.values()[i % AttributeUpdateType.values().length];
            result.add(new AttributeUpdate(UUID.randomUUID(), ut, i, i));
        }

        return result;
    }

    private void setSavedState(String segmentName, long segmentId, long startOffset, int attributeCount, TestContext context) {
        // We intentionally do not save anything if no attributes - this helps us test the case when there is no state
        // file to load from.
        if (attributeCount != 0) {
            val attributes = createAttributes(attributeCount)
                    .stream()
                    .collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
            val segmentInfo = StreamSegmentInformation.builder()
                                                      .name(segmentName)
                                                      .attributes(attributes)
                                                      .startOffset(startOffset)
                                                      .length(startOffset + 1) // Arbitrary; will be ignored anyway.
                                                      .build();
            context.stateStore.put(segmentName, new SegmentState(segmentId, segmentInfo), TIMEOUT).join();
        }
    }

    private void assertSegmentCreated(String segmentName, Collection<AttributeUpdate> attributeUpdates, TestContext context) {
        SegmentProperties sp = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertNotNull("No segment has been created in the Storage for " + segmentName, sp);

        long segmentId = context.metadata.getStreamSegmentId(segmentName, false);
        Assert.assertEquals("Segment '" + segmentName + "' has been registered in the metadata.", ContainerMetadata.NO_STREAM_SEGMENT_ID, segmentId);

        val attributes = Attributes.getCoreNonNullAttributes(attributeUpdates.stream()
                .collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue)));
        val actualAttributes = context.stateStore.get(segmentName, TIMEOUT).join().getAttributes();
        AssertExtensions.assertMapEquals("Wrong attributes.", attributes, actualAttributes);
    }

    private void setupOperationLog(TestContext context) {
        AtomicLong seqNo = new AtomicLong();
        context.operationLog.addHandler = op -> {
            long currentSeqNo = seqNo.incrementAndGet();
            UpdateableSegmentMetadata sm;
            Assert.assertTrue("Unexpected operation type.", op instanceof StreamSegmentMapOperation);
            StreamSegmentMapOperation mop = (StreamSegmentMapOperation) op;
            if (mop.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                mop.setStreamSegmentId(currentSeqNo);
            }

            sm = context.metadata.mapStreamSegmentId(mop.getStreamSegmentName(), mop.getStreamSegmentId());
            sm.setStorageLength(0);
            sm.setLength(mop.getLength());
            sm.setStartOffset(mop.getStartOffset());
            if (mop.isSealed()) {
                sm.markSealed();
            }

            sm.updateAttributes(mop.getAttributes());

            return CompletableFuture.completedFuture(null);
        };
    }

    private void setupStorageCreateHandler(TestContext context, HashMap<String, SegmentRollingPolicy> storageSegments) {
        context.storage.createHandler = (segmentName, policy) -> {
            synchronized (storageSegments) {
                if (storageSegments.containsKey(segmentName)) {
                    return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
                } else {
                    storageSegments.put(segmentName, policy);
                    return CompletableFuture.completedFuture(StreamSegmentInformation.builder().name(segmentName).build());
                }
            }
        };
    }

    private void setupStorageGetHandler(TestContext context, Set<String> storageSegments, Function<String, SegmentProperties> infoGetter) {
        context.storage.getInfoHandler = segmentName -> {
            synchronized (storageSegments) {
                if (!storageSegments.contains(segmentName)) {
                    return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
                } else {
                    return CompletableFuture.completedFuture(infoGetter.apply(segmentName));
                }
            }
        };
    }

    private void assertEquals(String message, SegmentProperties expected, SegmentProperties actual) {
        Assert.assertEquals(message + " getName() mismatch.", expected.getName(), actual.getName());
        Assert.assertEquals(message + " isDeleted() mismatch.", expected.isDeleted(), actual.isDeleted());
        Assert.assertEquals(message + " getLength() mismatch.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " isSealed() mismatch.", expected.isSealed(), actual.isSealed());
    }

    //region TestContext

    private class TestContext implements AutoCloseable {
        final Supplier<CompletableFuture<Void>> noOpMetadataCleanup = () -> CompletableFuture.completedFuture(null);
        final UpdateableContainerMetadata metadata;
        final TestStorage storage;
        final TestOperationLog operationLog;
        final InMemoryStateStore stateStore;
        final StreamSegmentMapper mapper;

        TestContext() {
            this.storage = new TestStorage();
            this.operationLog = new TestOperationLog();
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            this.stateStore = new InMemoryStateStore();
            this.mapper = new StreamSegmentMapper(this.metadata, this.operationLog, this.stateStore, noOpMetadataCleanup, this.storage, executorService());
        }

        @Override
        public void close() {
            this.storage.close();
            this.operationLog.close();
        }
    }

    //endregion

    //region TestOperationLog

    private static class TestOperationLog implements OperationLog {
        Function<Operation, CompletableFuture<Void>> addHandler;

        @Override
        public CompletableFuture<Void> add(Operation operation, Duration timeout) {
            return addHandler.apply(operation);
        }

        //region Unimplemented Methods

        @Override
        public int getId() {
            return -1;
        }

        @Override
        public boolean isOffline() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public CompletableFuture<Void> truncate(long upToSequence, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> operationProcessingBarrier(Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> awaitOnline() {
            return null;
        }

        @Override
        public Service startAsync() {
            return null;
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public State state() {
            return null;
        }

        @Override
        public Service stopAsync() {
            return null;
        }

        @Override
        public void awaitRunning() {

        }

        @Override
        public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public void awaitTerminated() {

        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public Throwable failureCause() {
            return null;
        }

        @Override
        public void addListener(Listener listener, Executor executor) {

        }

        //endregion
    }

    //endregion

    //region TestStorage

    private static class TestStorage implements Storage {
        BiFunction<String, SegmentRollingPolicy, CompletableFuture<SegmentProperties>> createHandler;
        Function<String, CompletableFuture<SegmentProperties>> getInfoHandler;

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
            return create(streamSegmentName, null, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, SegmentRollingPolicy rollingPolicy, Duration timeout) {
            return this.createHandler.apply(streamSegmentName, rollingPolicy);
        }

        @Override
        public CompletableFuture<SegmentHandle> openRead(String streamSegmentName) {
            return CompletableFuture.completedFuture(InMemoryStorage.newHandle(streamSegmentName, true));
        }

        @Override
        public CompletableFuture<SegmentHandle> openWrite(String streamSegmentName) {
            return CompletableFuture.completedFuture(InMemoryStorage.newHandle(streamSegmentName, false));
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return this.getInfoHandler.apply(streamSegmentName);
        }

        //region Unimplemented methods

        @Override
        public void initialize(long epoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> concat(SegmentHandle targetHandle, long offset, String sourceSegment, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> delete(SegmentHandle handle, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsTruncation() {
            return false;
        }

        @Override
        public void close() {

        }

        //endregion
    }

    //endregion

    //region TestStateStore

    private static class TestStateStore implements AsyncMap<String, SegmentState> {
        Supplier<CompletableFuture<SegmentState>> getHandler;

        @Override
        public CompletableFuture<SegmentState> get(String key, Duration timeout) {
            return this.getHandler.get();
        }

        @Override
        public CompletableFuture<Void> remove(String key, Duration timeout) {
            // Not needed.
            return null;
        }

        @Override
        public CompletableFuture<Void> put(String key, SegmentState value, Duration timeout) {
            // Not needed.
            return null;
        }
    }

    //endregion
}
