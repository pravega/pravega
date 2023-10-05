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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.MathHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for MetadataStore class.
 */
public abstract class MetadataStoreTestBase extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int CONTAINER_ID = 123;
    private static final int ATTRIBUTE_COUNT = 10;
    private static final SegmentType SEGMENT_TYPE = SegmentType.builder().internal().build();

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the ability of the MetadataStore to create a new Segment.
     */
    @Test
    public void testCreateSegment() {
        final int segmentCount = 50;
        val segmentTypes = Arrays.asList(
                SegmentType.STREAM_SEGMENT,
                SegmentType.builder().system().build(),
                SegmentType.builder().critical().internal().build());

        @Cleanup
        TestContext context = createTestContext();

        // Create some Segments and verify they are properly created and registered.
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            val segmentType = segmentTypes.get(i % segmentTypes.size());
            val segmentAttributes = createAttributeUpdates(ATTRIBUTE_COUNT);
            context.getMetadataStore().createSegment(segmentName, segmentType, segmentAttributes, TIMEOUT).join();
            assertSegmentCreated(segmentName, segmentType, segmentAttributes, context);
        }
    }

    /**
     * Tests the ability of the MetadataStore to create a new Transient Segment.
     */
    @Test
    public void testCreateTransientSegment() {
        final int transientSegmentCount = 50;
        @Cleanup
        TestContext context = createTestContext();
        // Create some Segments and verify they are properly created and registered.
        for (int i = 0; i < transientSegmentCount; i++) {
            String transientSegmentName = getTransientName(i);
            Collection<AttributeUpdate> segmentAttributes = Set.of(
                    new AttributeUpdate(Attributes.EVENT_COUNT, AttributeUpdateType.None, 0)
            );
            context.getMetadataStore().createSegment(transientSegmentName, SegmentType.TRANSIENT_SEGMENT, segmentAttributes, TIMEOUT);
            assertTransientSegmentCreated(transientSegmentName, context);
        }
    }

    /**
     * Verifies that {@link MetadataStore#createSegment} validates the {@link Attributes#ATTRIBUTE_ID_LENGTH} value, if
     * passed as an argument.
     */
    @Test
    public void testCreateSegmentValidateAttributeIdLength() {
        @Cleanup
        TestContext context = createTestContext();

        // Try to create a segment with invalid values.
        val s1 = getName(0);
        AssertExtensions.assertSuppliedFutureThrows(
                "ATTRIBUTE_ID_LENGTH was accepted as negative.",
            () -> context.getMetadataStore()
                         .createSegment(s1, SegmentType.STREAM_SEGMENT,
                                        AttributeUpdateCollection.from(new AttributeUpdate(Attributes.ATTRIBUTE_ID_LENGTH, AttributeUpdateType.None, -1L)),
                                        TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertSuppliedFutureThrows(
                "ATTRIBUTE_ID_LENGTH was accepted with too high value.",
                () -> context.getMetadataStore()
                             .createSegment(s1, SegmentType.STREAM_SEGMENT, 
                                            AttributeUpdateCollection.from(new AttributeUpdate(Attributes.ATTRIBUTE_ID_LENGTH, AttributeUpdateType.None, AttributeId.MAX_LENGTH + 1)), 
                                            TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Create a segment with valid values.
        val validAttributes = AttributeUpdateCollection.from(
                new AttributeUpdate(Attributes.ATTRIBUTE_ID_LENGTH, AttributeUpdateType.None, AttributeId.MAX_LENGTH / 2));
        context.getMetadataStore().createSegment(s1, SegmentType.STREAM_SEGMENT, validAttributes, TIMEOUT).join();
        assertSegmentCreated(s1, SegmentType.STREAM_SEGMENT, validAttributes, context);

        // Now verify that it also works if the attribute is not set.
        val noAttributes = new AttributeUpdateCollection();
        val s2 = getName(1);
        context.getMetadataStore().createSegment(s2, SegmentType.STREAM_SEGMENT, noAttributes, TIMEOUT).join();
        assertSegmentCreated(s2, SegmentType.STREAM_SEGMENT, noAttributes, context);

    }

    /**
     * Tests the ability of the MetadataStore to create a new Segment if the Segment already exists.
     */
    @Test
    public void testCreateSegmentAlreadyExists() {
        final String segmentName = "NewSegment";
        final Map<AttributeId, Long> originalAttributes = ImmutableMap.of(AttributeId.randomUUID(), 123L, Attributes.EVENT_COUNT, 1L);
        final Map<AttributeId, Long> expectedAttributes = getExpectedCoreAttributes(originalAttributes, SEGMENT_TYPE);
        final Collection<AttributeUpdate> correctAttributeUpdates =
                originalAttributes.entrySet().stream()
                        .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                        .collect(Collectors.toList());
        final Map<AttributeId, Long> badAttributes = Collections.singletonMap(AttributeId.randomUUID(), 456L);
        final Collection<AttributeUpdate> badAttributeUpdates =
                badAttributes.entrySet().stream()
                             .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.Replace, e.getValue()))
                             .collect(Collectors.toList());

        @Cleanup
        TestContext context = createTestContext();

        // Create a segment.
        context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, correctAttributeUpdates, TIMEOUT).join();

        // Try to create it again.
        AssertExtensions.assertSuppliedFutureThrows(
                "createSegment did not fail when Segment already exists.",
                () -> context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, badAttributeUpdates, TIMEOUT),
                ex -> ex instanceof StreamSegmentExistsException);

        val si = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected attributes after failed attempt to recreate correctly created segment",
                expectedAttributes, si.getAttributes());
    }

    /**
     * Tests the ability of the MetadataStore to create a new Segment if there are underlying failures.
     * This tests failures other than {@link StreamSegmentExistsException}, which is handled in a different test.
     */
    @Test
    public abstract void testCreateSegmentWithFailures();

    /**
     * Tests the ability of the MetadataStore to delete a Segment in the following scenarios:
     * 1. It doesn't exist.
     * 2. It exists, but has no Id assigned.
     * 3. It exists, has an Id assigned, but is not mapped in the metadata.
     * 4. It exists, has an Id and is mapped in the metadata. Optionally marked as isDeleted().
     */
    @Test
    public void testDeleteSegment() {
        final String inexistentSegment = "inexistent"; // This Segment does not exist.
        final String noIdSegment = "noid"; // Segment exists, but no id assigned.
        final String unmappedSegment = "unmapped"; // Segment exists, has id, but is not mapped in metadata.
        final String mappedSegment = "mapped"; // Segment exists, has id, and is mapped in metadata.
        final String mappedDeletedSegment = "mappeddeleted"; // Segment is mapped in metadata but marked as isDeleted.

        @Cleanup
        TestContext context = createTestContext();

        // Create all Segments that are supposed to exist.
        Stream.of(noIdSegment, unmappedSegment, mappedSegment, mappedDeletedSegment)
                .forEach(name -> context.getMetadataStore().createSegment(name, SEGMENT_TYPE, null, TIMEOUT).join());

        // Assign Ids to all Segments that are supposed to have one.
        Stream.of(unmappedSegment, mappedSegment, mappedDeletedSegment)
              .forEach(name -> context.getMetadataStore().getOrAssignSegmentId(name, TIMEOUT).join());

        // Evict the unmapped segment from the metadata - it ended up in there by means of Id assignment above.
        val toEvict = context.getMetadata().getStreamSegmentMetadata(context.getMetadata().getStreamSegmentId(unmappedSegment, false));
        context.getMetadata().removeTruncationMarkers(context.getMetadata().getOperationSequenceNumber());
        context.getMetadata().cleanup(Collections.singleton(toEvict), context.getMetadata().getOperationSequenceNumber());

        // Mark the segment that's supposed to be deleted as isDeleted.
        context.getMetadata().getStreamSegmentMetadata(context.getMetadata().getStreamSegmentId(mappedDeletedSegment, false)).markDeleted();

        // Start deleting...
        // These are the Segments which truly do not exist in the Metadata Store.
        Stream.of(inexistentSegment)
                .forEach(name -> {
                    boolean deleted = context.getMetadataStore().deleteSegment(name, TIMEOUT).join();
                    Assert.assertFalse("deleteSegment() returned true for inexistent segment: " + name, deleted);
                });

        // Delete the Segments which exist in the Metadata Store, but may or may not exist in Storage/Metadata.
        Stream.of(noIdSegment, unmappedSegment, mappedSegment, mappedDeletedSegment)
                .forEach(name -> {
                    boolean deleted = context.getMetadataStore().deleteSegment(name, TIMEOUT).join();
                    Assert.assertTrue("deleteSegment() returned false for existing segment: " + name, deleted);
                });

        // Verify all show up as inexistent.
        Stream.of(inexistentSegment, noIdSegment, unmappedSegment, mappedSegment, mappedDeletedSegment)
                .forEach(name -> AssertExtensions.assertSuppliedFutureThrows(
                        "Segment '" + name + "' was not deleted properly",
                        () -> context.getMetadataStore().getSegmentInfo(name, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException));

        // Segments which are not in the metadata should be deleted directly.
        Stream.of(inexistentSegment, noIdSegment, unmappedSegment)
                .forEach(name -> {
                    Assert.assertEquals("Not expecting previously unmapped segments to be activated in metadata.",
                            ContainerMetadata.NO_STREAM_SEGMENT_ID, context.getMetadata().getStreamSegmentId(name, false));
                    Assert.assertEquals("Expected a directDelete invocation for segment: " + name,
                            1, context.connector.getDirectDeleteCount(name));
                });

        // The ones which are in the metadata should be have isDeleted()==true.
        Stream.of(mappedSegment, mappedDeletedSegment)
              .forEach(name -> {
                  Assert.assertTrue("Expected isDeleted() to be true for segment: " + name,
                          context.getMetadata().getStreamSegmentMetadata(context.getMetadata().getStreamSegmentId(name, false)).isDeleted());
                  Assert.assertEquals("Not expecting directDelete invocation for segment: " + name,
                          0, context.connector.getDirectDeleteCount(name));
              });
    }

    /**
     * Tests GetStreamSegmentInfo with various scenarios.
     */
    @Test
    public void testGetStreamSegmentInfo() {
        final String segmentName = "segment";
        final long segmentId = 1234;

        @Cleanup
        TestContext context = createTestContext();

        // Segment not exists at all.
        AssertExtensions.assertSuppliedFutureThrows(
                "getSegmentInfo did not throw correct exception when segment does not exist in Metadata or Storage.",
                () -> context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Segment is not active in the in Metadata, but it does exist.
        // Since we do not setup an OperationLog, we guarantee that there is no attempt to map this in the metadata.
        context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, null, TIMEOUT).join();

        // Persist the segment info in the Store.
        val updateAttributes = toAttributes(createAttributeUpdates(ATTRIBUTE_COUNT));
        SEGMENT_TYPE.intoAttributes(updateAttributes);
        val segmentInfo = StreamSegmentInformation.builder()
                .name(segmentName).length(123).sealed(true).attributes(updateAttributes).build();
        context.getMetadataStore().updateSegmentInfo(toMetadata(segmentId, segmentInfo), TIMEOUT).join();

        // Get the segment info from the Store.
        val inStorageInfo = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).join();
        assertEquals("Unexpected SegmentInfo when Segment exists in Storage.", segmentInfo, inStorageInfo);

        // We only expect Core Attributes to have been serialized.
        val expectedAttributes = Attributes.getCoreNonNullAttributes(segmentInfo.getAttributes());
        SEGMENT_TYPE.intoAttributes(expectedAttributes);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes when Segment exists in Storage", expectedAttributes, inStorageInfo);
        Assert.assertEquals("Not expecting any segments to be mapped.", 0, context.getNonPinnedMappedSegmentCount());

        // Segment exists in Metadata (and in Storage too) - here, we set different values in the Metadata to verify that
        // the info is fetched from there.
        val sm = context.getMetadata().mapStreamSegmentId(segmentName, segmentId);
        sm.setLength(segmentInfo.getLength() + 1);
        sm.updateAttributes(Collections.singletonMap(AttributeId.randomUUID(), 12345L));
        val inMetadataInfo = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).join();
        assertEquals("Unexpected SegmentInfo when Segment exists in Metadata.", sm, inMetadataInfo);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes when Segment exists in Metadata.",
                sm.getAttributes(), inMetadataInfo);

        // Segment exists in Metadata, but is marked as deleted.
        sm.markDeleted();
        AssertExtensions.assertSuppliedFutureThrows(
                "getSegmentInfo did not throw correct exception when segment is marked as Deleted in metadata.",
                () -> context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests GetStreamSegmentInfo when it is invoked in parallel with a Segment assignment.
     * @throws Exception if the test failed.
     */
    @Test
    public void testGetStreamSegmentInfoWithConcurrency() throws Exception {
        final String segmentName = "Segment";
        final long segmentId = 123;
        final SegmentProperties storageInfo = StreamSegmentInformation.builder().name(segmentName).length(123).sealed(true).build();
        final long metadataLength = storageInfo.getLength() + 1;

        @Cleanup
        TestContext context = createTestContext();
        val initialSegmentInfo = StreamSegmentInformation
                .builder()
                .name(segmentName)
                .startOffset(0L)
                .length(1L)
                .attributes(toAttributes(createAttributeUpdates(ATTRIBUTE_COUNT)))
                .build();
        context.getMetadataStore().updateSegmentInfo(toMetadata(segmentId, initialSegmentInfo), TIMEOUT).join();
        Map<AttributeId, Long> expectedAttributes = initialSegmentInfo.getAttributes();

        CompletableFuture<Void> addInvoked = new CompletableFuture<>();
        context.connector.setMapSegmentId((id, sp, pin, timeout) -> {
            addInvoked.join();
            UpdateableSegmentMetadata segmentMetadata = context.getMetadata().mapStreamSegmentId(segmentName, segmentId);
            segmentMetadata.setStorageLength(sp.getLength());
            segmentMetadata.setLength(metadataLength);
            segmentMetadata.updateAttributes(expectedAttributes);
            if (sp.isSealed()) {
                segmentMetadata.markSealed();
            }
            if (pin) {
                segmentMetadata.markPinned();
            }

            return CompletableFuture.completedFuture(segmentId);
        });

        // Second call is designed to hit when the first call still tries to assign the id, hence we test normal queueing.
        context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT, id -> CompletableFuture.completedFuture(null));

        // Concurrently with the map, request a Segment Info.
        CompletableFuture<SegmentProperties> segmentInfoFuture = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT);
        Assert.assertFalse("getSegmentInfo returned a completed future.", segmentInfoFuture.isDone());

        // Release the OperationLog add and verify the Segment Info has been served with information from the Metadata.
        addInvoked.complete(null);
        SegmentProperties segmentInfo = segmentInfoFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val expectedInfo = context.getMetadata().getStreamSegmentMetadata(segmentId);
        assertEquals("Unexpected Segment Info returned.", expectedInfo, segmentInfo);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes returned.", expectedInfo.getAttributes(), segmentInfo);

        int storageGetCount = context.getStoreReadCount();
        Assert.assertEquals("Unexpected number of Storage.read() calls.", 1, storageGetCount);
    }

    /**
     * Tests the ability of the MetadataStore to generate/return the Id of an existing StreamSegment, as well as
     * retrieving existing attributes.
     */
    @Test
    public void testGetOrAssignStreamSegmentId() {
        final long baseSegmentId = 1000;
        final long minSegmentLength = 1;
        final int segmentCount = 50;
        Function<String, Long> getSegmentLength = segmentName -> minSegmentLength + MathHelpers.abs(segmentName.hashCode());
        Function<String, Long> getSegmentStartOffset = segmentName -> getSegmentLength.apply(segmentName) / 2;

        @Cleanup
        TestContext context = createTestContext();

        HashSet<String> segmentNames = new HashSet<>();
        HashSet<String> sealedSegments = new HashSet<>();
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            segmentNames.add(segmentName);

            val si = StreamSegmentInformation.builder()
                                             .name(segmentName)
                                             .length(getSegmentLength.apply(segmentName))
                                             .startOffset(getSegmentStartOffset.apply(segmentName))
                                             .sealed(i % 2 == 0)
                                             .attributes(toAttributes(createAttributeUpdates(ATTRIBUTE_COUNT)))
                                             .build();

            if (si.isSealed()) {
                sealedSegments.add(segmentName);
            }

            context.getMetadataStore().updateSegmentInfo(toMetadata(baseSegmentId + i, si), TIMEOUT).join();
        }

        Predicate<String> isSealed = sealedSegments::contains;
        for (String name : segmentNames) {
            long id = context.getMetadataStore().getOrAssignSegmentId(name, TIMEOUT).join();
            Assert.assertNotEquals("No id was assigned for StreamSegment " + name, ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
            SegmentMetadata sm = context.getMetadata().getStreamSegmentMetadata(id);
            Assert.assertNotNull("No metadata was created for StreamSegment " + name, sm);
            long expectedLength = getSegmentLength.apply(name);
            boolean expectedSeal = isSealed.test(name);
            Assert.assertEquals("Metadata does not have the expected length for StreamSegment " + name, expectedLength, sm.getLength());
            Assert.assertEquals("Metadata does not have the expected value for isSealed for StreamSegment " + name, expectedSeal, sm.isSealed());

            val segmentState = context.getMetadataStore().getSegmentInfo(name, TIMEOUT).join();
            Map<AttributeId, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();
            SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in metadata for StreamSegment " + name, expectedAttributes, sm);
            long expectedStartOffset = segmentState == null ? 0 : segmentState.getStartOffset();
            Assert.assertEquals("Unexpected StartOffset in metadata for " + name, expectedStartOffset, sm.getStartOffset());
        }
    }

    /**
     * Tests the behavior of getOrAssignSegmentId when the requested StreamSegment has been deleted.
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
        TestContext context = createTestContext();

        // Map all the segments, then delete them, then verify behavior.
        for (String name : storageSegments) {
            context.getMetadataStore().createSegment(name, SEGMENT_TYPE, null, TIMEOUT).join();
            //            context.getMetadataStore().getOrAssignSegmentId(name, TIMEOUT).join();
            context.getMetadataStore().deleteSegment(name, TIMEOUT).join();
            AssertExtensions.assertSuppliedFutureThrows(
                    "getOrAssignSegmentId did not return appropriate exception when the segment has been deleted.",
                    () -> context.getMetadataStore().getOrAssignSegmentId(name, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the ability of the MetadataStore to generate/return the Id of an existing StreamSegment, when dealing
     * with Storage failures (or inexistent StreamSegments).
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithFailures() {
        final String segmentName = "Segment";

        @Cleanup
        TestContext context = createTestContext();

        // 1. Unable to access storage
        context.setGetInfoErrorInjectorAsync(new ErrorInjector<>(i -> true, IntentionalException::new));
        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignSegmentId did not throw the right exception for an async exception.",
                () -> context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        context.setGetInfoErrorInjectorAsync(null); // Clear it.

        // 2. StreamSegmentNotExists
        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignSegmentId did not throw the right exception for a non-existent StreamSegment.",
                () -> context.getMetadataStore().getOrAssignSegmentId(segmentName + "foo", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 3. Synchrounous errors.
        context.setGetInfoErrorInjectorSync(new ErrorInjector<>(i -> true, IntentionalException::new));
        AssertExtensions.assertSuppliedFutureThrows(
                "getOrAssignSegmentId did not throw the right exception for a synchronous exception",
                () -> context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        context.setGetInfoErrorInjectorSync(null); // Clear it.
    }

    /**
     * Tests the ability of getOrAssignSegmentId to handle the TooManyActiveSegmentsException.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithMetadataLimit() {
        List<String> segmentNames = Arrays.asList("S1", "S2", "S3");

        AtomicBoolean cleanupInvoked = new AtomicBoolean();
        AtomicInteger exceptionCounter = new AtomicInteger();
        Supplier<CompletableFuture<Void>> noOpCleanup = () -> {
            if (!cleanupInvoked.compareAndSet(false, true)) {
                return Futures.failedFuture(new AssertionError("Cleanup invoked multiple times."));
            }
            return CompletableFuture.completedFuture(null);
        };

        try (TestContext context = createTestContext(noOpCleanup)) {
            for (val s : segmentNames) {
                context.getMetadataStore().createSegment(s, SEGMENT_TYPE, null, TIMEOUT).join();
            }

            // 1. Verify the behavior when even after the retry we still cannot map.
            // We use 'containerId' as a proxy for the exception id (to make sure we collect the right one).
            context.connector.setMapSegmentId((id, sp, pin, timeout) ->
                    Futures.failedFuture(new TooManyActiveSegmentsException(exceptionCounter.incrementAndGet(), 0)));

            AssertExtensions.assertSuppliedFutureThrows(
                    "Unexpected outcome when trying to map a segment to a full metadata that cannot be cleaned.",
                    () -> context.getMetadataStore().getOrAssignSegmentId(segmentNames.get(0), TIMEOUT),
                    ex -> ex instanceof TooManyActiveSegmentsException && ((TooManyActiveSegmentsException) ex).getContainerId() == exceptionCounter.get());
            Assert.assertEquals("Unexpected number of attempts to map.", 2, exceptionCounter.get());
            Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());

            // Now with a Segment 3.
            exceptionCounter.set(0);
            cleanupInvoked.set(false);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Unexpected outcome when trying to map a Segment to a full metadata that cannot be cleaned.",
                    () -> context.getMetadataStore().getOrAssignSegmentId(segmentNames.get(2), TIMEOUT),
                    ex -> ex instanceof TooManyActiveSegmentsException && ((TooManyActiveSegmentsException) ex).getContainerId() == exceptionCounter.get());
            Assert.assertEquals("Unexpected number of attempts to map.", 2, exceptionCounter.get());
            Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());
        }

        // 2. Verify the behavior when the first call fails, but the second one succeeds.
        exceptionCounter.set(0);
        cleanupInvoked.set(false);

        Supplier<CompletableFuture<Void>> workingCleanup = () -> {
            if (!cleanupInvoked.compareAndSet(false, true)) {
                return Futures.failedFuture(new AssertionError("Cleanup invoked multiple times."));
            }

            return CompletableFuture.completedFuture(null);
        };

        try (TestContext context = createTestContext(workingCleanup)) {
            for (val s : segmentNames) {
                context.getMetadataStore().createSegment(s, SEGMENT_TYPE, null, TIMEOUT).join();
            }

            // 1. Verify the behavior when even after the retry we still cannot map.
            // We use 'containerId' as a proxy for the exception id (to make sure we collect the right one).
            context.connector.setMapSegmentId((id, sp, pin, timeout) -> {
                context.connector.reset();
                return Futures.failedFuture(new TooManyActiveSegmentsException(exceptionCounter.incrementAndGet(), 0));
            });

            long id = context.getMetadataStore().getOrAssignSegmentId(segmentNames.get(0), TIMEOUT).join();
            Assert.assertEquals("Unexpected number of attempts to map.", 1, exceptionCounter.get());
            Assert.assertTrue("Cleanup was not invoked.", cleanupInvoked.get());
            Assert.assertNotEquals("No valid SegmentId assigned.", ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
        }
    }

    /**
     * Tests the ability of the MetadataStore to generate/return the Id of an existing StreamSegment, with concurrent requests.
     * Also tests the ability to execute such callbacks in the order in which they were received.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithConcurrency() {
        final String segmentName = "Segment";
        final long segmentId = 12345;
        final String firstResult = "first";
        final String secondResult = "second";
        final String thirdResult = "third";

        @Cleanup
        TestContext context = createTestContext();
        context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, null, TIMEOUT).join();
        CompletableFuture<Void> addInvoked = new CompletableFuture<>();
        AtomicBoolean mapSegmentIdInvoked = new AtomicBoolean(false);
        context.connector.setMapSegmentId((id, sp, pin, timeout) -> {
            if (mapSegmentIdInvoked.getAndSet(true)) {
                return Futures.failedFuture(new IllegalStateException("multiple calls to mapSegmentId"));
            }

            // Need to set SegmentId on operation.
            UpdateableSegmentMetadata segmentMetadata = context.getMetadata().mapStreamSegmentId(segmentName, segmentId);
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setLength(0);
            addInvoked.complete(null);
            return CompletableFuture.completedFuture(segmentId);
        });

        List<Integer> invocationOrder = new Vector<>();

        // Second call is designed to hit when the first call still tries to assign the id, hence we test normal queueing.
        CompletableFuture<String> firstCall = context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (first).", segmentId, (long) id);
                    invocationOrder.add(1);
                    return CompletableFuture.completedFuture(firstResult);
                });

        CompletableFuture<String> secondCall = context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (second).", segmentId, (long) id);
                    invocationOrder.add(2);
                    return CompletableFuture.completedFuture(secondResult);
                });

        // Wait for the metadata to be updated properly.
        addInvoked.join();

        // Third call is designed to hit after the metadata has been updated, but prior to the other callbacks being invoked.
        // It verifies that even in that case it still executes in order.
        CompletableFuture<String> thirdCall = context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT,
                id -> {
                    Assert.assertEquals("Unexpected SegmentId (second).", segmentId, (long) id);
                    invocationOrder.add(3);
                    return CompletableFuture.completedFuture(thirdResult);
                });

        Assert.assertEquals("Unexpected result from firstCall.", firstResult, firstCall.join());
        Assert.assertEquals("Unexpected result from secondCall.", secondResult, secondCall.join());
        Assert.assertEquals("Unexpected result from thirdCall.", thirdResult, thirdCall.join());
        val expectedOrder = Arrays.asList(1, 2, 3);
        AssertExtensions.assertListEquals("", expectedOrder, invocationOrder, Integer::equals);
    }

    /**
     * Tests the following scenario, which is typical of how the Controller bootstraps its metadata in the Segment Store:
     * 0. Metadata segment does not currently exist.
     * 1. Metadata segment is queried (StreamSegmentNotExists is returned).
     * 2. Metadata segment is immediately created.
     * 3. Metadata segment is queried again (in which case it should be returned normally).
     */
    @Test
    public void testCreateSegmentAfterFailedAssignment() {
        final String segmentName = "Segment";

        @Cleanup
        TestContext context = createTestContext();

        // 1. Query for an inexistent segment, and pass a completion callback that is not yet completed.
        val f1 = new CompletableFuture<Void>();
        AssertExtensions.assertSuppliedFutureThrows(
                "Expecting assignment to be rejected.",
                () -> context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT, id -> f1),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 2. Create the segment.
        context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, null, TIMEOUT).join();

        // 3. Issue the second assignment. This should not fail.
        context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT).join();

        // Finally, complete the first callback - this should have no effect on anything above.
        f1.complete(null);
    }

    /**
     * Tests the ability of the MetadataStore to cancel pending assignment requests when {@link MetadataStore#close} is
     * invoked.
     */
    @Test
    public void testClose() {
        final String segmentName = "Segment";
        @Cleanup
        TestContext context = createTestContext();
        context.getMetadataStore().createSegment(segmentName, SEGMENT_TYPE, null, TIMEOUT).join();
        CompletableFuture<Long> mapResult = new CompletableFuture<>();
        CompletableFuture<Void> mapIdInvoked = new CompletableFuture<>();
        context.connector.setMapSegmentId(
                (id, sp, pin, timeout) -> {
                    mapIdInvoked.complete(null);
                    return mapResult;
                });

        CompletableFuture<String> firstCall = context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT,
                id -> Futures.failedFuture(new AssertionError("This should not be invoked (1).")));
        CompletableFuture<String> secondCall = context.getMetadataStore().getOrAssignSegmentId(segmentName, TIMEOUT,
                id -> Futures.failedFuture(new AssertionError("This should not be invoked (2).")));

        // Wait for the map invocation to occur before moving forward, otherwise we won't be testing the correct scenario.
        mapIdInvoked.join();

        // Close the MetadataStore and verify that pending requests (initial + subsequent) are failed appropriately.
        context.getMetadataStore().close();
        AssertExtensions.assertSuppliedFutureThrows(
                "First call did not fail when close() was invoked.",
                () -> firstCall,
                ex -> ex instanceof ObjectClosedException);
        AssertExtensions.assertSuppliedFutureThrows(
                "Second call did not fail when close() was invoked.",
                () -> secondCall,
                ex -> ex instanceof ObjectClosedException);

        // Nobody should be waiting on this anymore, but in case anyone does, fail it now.
        mapResult.completeExceptionally(new AssertionError("This should not happen"));
    }

    /**
     * Checks that we can create and register a pinned Segment via {@link MetadataStore}.
     */
    @Test
    public void testPinSegmentToMemory() {
        final String segmentName = "PinnedSegment";
        @Cleanup
        TestContext context = createTestContext();
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name(segmentName).build();
        context.connector.getMapSegmentId().apply(123, segmentProperties, false, TIMEOUT).join();
        Assert.assertFalse(context.connector.getContainerMetadata().getStreamSegmentMetadata(123).isPinned());
        Assert.assertEquals((long) context.getMetadataStore().pinSegmentToMemory(segmentName, TIMEOUT).join(), 123);
        Assert.assertTrue(context.connector.getContainerMetadata().getStreamSegmentMetadata(123).isPinned());
        AssertExtensions.assertThrows(StreamSegmentNotExistsException.class, () -> context.getMetadataStore().pinSegmentToMemory("nonExistent", TIMEOUT).join());
    }

    /**
     * Verifies that a {@link SegmentType} marked as {@link SegmentType#isTransientSegment()} is rejected if it has an improperly
     * formatted name.
     *
     * @throws ExecutionException ExecutionException if any occurs.
     * @throws InterruptedException InterruptedException if any occurs.
     * @throws TimeoutException TimeoutException if any occurs.
     */
    @Test
    public void testTransientSegmentFormat() throws ExecutionException, InterruptedException, TimeoutException {
        final String validTransientSegment = "scope/stream/transient#transient.00000000000000000000000000000000";
        final String invalidTransientSegment = "scope/stream/transient";
        @Cleanup
        TestContext context = createTestContext();
        // Should complete successfully.
        context.getMetadataStore().createSegment(validTransientSegment, SegmentType.TRANSIENT_SEGMENT, new ArrayList<>(), TIMEOUT).get();
        // Make sure our Transient Segment name maps to a valid Segment ID.
        TestUtils.await(() -> {
            return context.getMetadata().getStreamSegmentId(validTransientSegment, false) != ContainerMetadata.NO_STREAM_SEGMENT_ID;
        }, 1000, TIMEOUT.toMillis());
        // Attempt to create a Transient Segment with an ill-formatted name.
        AssertExtensions.assertThrows(
            "createSegment did not throw an exception given an invalid Transient Segment name.",
                () -> context.getMetadataStore().createSegment(invalidTransientSegment, SegmentType.TRANSIENT_SEGMENT, null, TIMEOUT).get(),
                ex -> ex instanceof IllegalArgumentException
        );
    }

    private String getName(long segmentId) {
        return String.format("Segment_%d", segmentId);
    }

    private String getTransientName(long writerId) {
        return NameUtils.getTransientNameFromId("segment", new UUID(0, writerId));
    }

    private Collection<AttributeUpdate> createAttributeUpdates(int count) {
        Collection<AttributeUpdate> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            boolean isCore = i % 2 == 0;
            AttributeId id = isCore ? AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX, i + 10000) : AttributeId.randomUUID();
            AttributeUpdateType ut = AttributeUpdateType.values()[i % AttributeUpdateType.values().length];
            result.add(new AttributeUpdate(id, ut, i, i));
        }

        return result;
    }

    private Map<AttributeId, Long> toAttributes(Collection<AttributeUpdate> updates) {
        return updates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
    }

    private StreamSegmentMetadata toMetadata(long segmentId, SegmentProperties sp) {
        StreamSegmentMetadata sm = new StreamSegmentMetadata(sp.getName(), segmentId, CONTAINER_ID);
        sm.updateAttributes(sp.getAttributes());
        sm.setLength(sp.getLength());
        sm.setStartOffset(sp.getStartOffset());
        if (sp.isSealed()) {
            sm.markSealed();
        }
        if (sp.isDeleted()) {
            sm.markDeleted();
        }

        sm.refreshDerivedProperties();
        return sm;
    }

    @SneakyThrows
    private void assertSegmentCreated(String segmentName, SegmentType segmentType, Collection<AttributeUpdate> attributeUpdates, TestContext context) {
        SegmentProperties sp;
        try {
            sp = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Throwable ex) {
            ex = Exceptions.unwrap(ex);
            if (ex instanceof StreamSegmentNotExistsException) {
                Assert.fail("No segment has been created in the Storage for " + segmentName);
            }

            throw ex;
        }

        long segmentId = context.getMetadata().getStreamSegmentId(segmentName, false);
        Assert.assertEquals("Segment '" + segmentName + "' has been registered in the metadata.", ContainerMetadata.NO_STREAM_SEGMENT_ID, segmentId);

        val attributes = getExpectedCoreAttributes(toAttributes(attributeUpdates), segmentType);
        AssertExtensions.assertMapEquals("Wrong attributes.", attributes, sp.getAttributes());
    }

    // Transient Segments are written directly to in-memory metadata without being persisted in Tier 1.
    // Therefore we expect the container metadata to not report an id of NO_STREAM_SEGMENT_ID.
    private void assertTransientSegmentCreated(String segmentName, TestContext context) {
        long segmentId = context.getMetadata().getStreamSegmentId(segmentName, false);
        Assert.assertNotEquals("Transient Segment '" + segmentName + "' has not been registered in the metadata.", ContainerMetadata.NO_STREAM_SEGMENT_ID, segmentId);
    }

    protected void assertEquals(String message, SegmentProperties expected, SegmentProperties actual) {
        Assert.assertEquals(message + " getName() mismatch.", expected.getName(), actual.getName());
        Assert.assertEquals(message + " isDeleted() mismatch.", expected.isDeleted(), actual.isDeleted());
        Assert.assertEquals(message + " getLength() mismatch.", expected.getLength(), actual.getLength());
        Assert.assertEquals(message + " isSealed() mismatch.", expected.isSealed(), actual.isSealed());
        Assert.assertEquals(message + " getStartOffset() mismatch.", expected.getStartOffset(), actual.getStartOffset());
    }

    private Map<AttributeId, Long> getExpectedCoreAttributes(Map<AttributeId, Long> attributes, SegmentType segmentType) {
        attributes = Attributes.getCoreNonNullAttributes(attributes);
        segmentType.intoAttributes(attributes);
        return attributes;
    }

    //region TestContext

    protected TestContext createTestContext() {
        return createTestContext(() -> CompletableFuture.completedFuture(null));
    }

    protected TestContext createTestContext(Supplier<CompletableFuture<Void>> runCleanup) {
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID).build();

        // Setup a connector with default, functioning callbacks.
        TestConnector connector = new TestConnector(
                metadata,
                (id, sp, pin, timeout) -> {
                    if (id == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                        id = metadata.nextOperationSequenceNumber();
                    }

                    UpdateableSegmentMetadata sm = metadata.mapStreamSegmentId(sp.getName(), id);
                    sm.setStorageLength(0);
                    sm.setLength(sp.getLength());
                    sm.setStartOffset(sp.getStartOffset());
                    if (sp.isSealed()) {
                        sm.markSealed();
                    }
                    if (pin) {
                        sm.markPinned();
                    }
                    sm.updateAttributes(sp.getAttributes());
                    sm.refreshDerivedProperties();
                    return CompletableFuture.completedFuture(id);
                },
                (id, timeout) -> {
                    Assert.assertNotEquals("DeleteSegmentOperation with no segment id.", ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
                    UpdateableSegmentMetadata sm = metadata.getStreamSegmentMetadata(id);
                    if (sm != null) {
                        sm.markDeleted();
                    }
                    return CompletableFuture.completedFuture(null);
                },
                runCleanup,
                (id, sp, pin, timeout) -> {
                    Assert.assertNotEquals("PinSegmentOperation with no segment id.", ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
                    UpdateableSegmentMetadata sm = metadata.getStreamSegmentMetadata(id);
                    if (pin) {
                        sm.markPinned();
                    }
                    return CompletableFuture.completedFuture(pin);
                });

        return createTestContext(connector);
    }

    protected abstract TestContext createTestContext(TestConnector connector);

    @RequiredArgsConstructor
    protected abstract class TestContext implements AutoCloseable {
        final TestConnector connector;

        StreamSegmentContainerMetadata getMetadata() {
            return (StreamSegmentContainerMetadata) this.connector.getContainerMetadata();
        }

        abstract MetadataStore getMetadataStore();

        abstract int getStoreReadCount();

        abstract void setGetInfoErrorInjectorSync(ErrorInjector<Exception> ei);

        abstract void setGetInfoErrorInjectorAsync(ErrorInjector<Exception> ei);

        int getNonPinnedMappedSegmentCount() {
            val m = this.connector.getContainerMetadata();
            return (int) m.getAllStreamSegmentIds().stream()
                          .filter(id -> !m.getStreamSegmentMetadata(id).isPinned())
                          .count();
        }

        @Override
        public void close() {
        }
    }

    /**
     * Extension of MetadataStore.Connector that allows overriding all collbacks at any given time.
     */
    static class TestConnector extends MetadataStore.Connector {
        @GuardedBy("directDeleteCount")
        private final HashMap<String, Integer> directDeleteCount = new HashMap<>();
        @Getter
        @Setter
        private MapSegmentId mapSegmentId;
        @Getter
        @Setter
        private Supplier<CompletableFuture<Void>> metadataCleanup;
        @Getter
        private final MetadataStore.Connector.DirectDeleteSegment directDeleteSegment = (name, timeout) -> {
            synchronized (this.directDeleteCount) {
                this.directDeleteCount.put(name, this.directDeleteCount.getOrDefault(name, 0) + 1);
            }
            return CompletableFuture.completedFuture(null);
        };
        @Getter
        @Setter
        private LazyDeleteSegment lazyDeleteSegment;

        TestConnector(UpdateableContainerMetadata metadata, MapSegmentId mapSegmentId, LazyDeleteSegment lazyDeleteSegment,
                      Supplier<CompletableFuture<Void>> runCleanup, PinSegment pinSegment) {
            super(metadata, mapSegmentId, (name, timeout) -> CompletableFuture.completedFuture(null), lazyDeleteSegment, runCleanup, pinSegment);
            reset();
        }

        void reset() {
            this.mapSegmentId = super.getMapSegmentId();
            this.metadataCleanup = super.getMetadataCleanup();
            this.lazyDeleteSegment = super.getLazyDeleteSegment();
        }

        int getDirectDeleteCount(String segmentName) {
            synchronized (this.directDeleteCount) {
                return this.directDeleteCount.getOrDefault(segmentName, 0);
            }
        }
    }

    //endregion
}
