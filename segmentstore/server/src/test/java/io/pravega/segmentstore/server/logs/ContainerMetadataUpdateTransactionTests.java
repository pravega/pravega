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
package io.pravega.segmentstore.server.logs;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.DynamicAttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.DynamicAttributeValue;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.CheckpointOperationBase;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ContainerMetadataUpdateTransaction and SegmentMetadataUpdateTransaction classes.
 */
public class ContainerMetadataUpdateTransactionTests {
    private static final int CONTAINER_ID = 1234567;
    private static final String SEGMENT_NAME = "Segment_123";
    private static final String TRANSIENT_SEGMENT_NAME = "scope/stream/transient#transient.00000000000000000000000000000000";
    private static final long TRANSIENT_SEGMENT_ID = 456;
    private static final int TRANSIENT_ATTRIBUTE_REMAINING = 2;
    private static final long SEGMENT_ID = 123;
    private static final String SEALED_SOURCE_NAME = "Segment_123#Source_Sealed";
    private static final long SEALED_SOURCE_ID = 567;
    private static final String NOTSEALED_SOURCE_NAME = "Segment_123#Source_NotSealed";
    private static final long NOTSEALED_SOURCE_ID = 890;
    private static final long SEALED_SOURCE_LENGTH = 12;
    private static final long SEGMENT_LENGTH = 1234567;
    private static final ByteArraySegment DEFAULT_APPEND_DATA = new ByteArraySegment("hello".getBytes());
    private static final AttributeUpdateType[] ATTRIBUTE_UPDATE_TYPES = new AttributeUpdateType[]{
            AttributeUpdateType.Replace, AttributeUpdateType.Accumulate};
    private static final Supplier<Long> NEXT_ATTRIBUTE_VALUE = System::nanoTime;
    private static final SegmentType DEFAULT_TYPE = SegmentType.STREAM_SEGMENT;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);
    private ManualTimer timeProvider;

    @Before
    public void before() {
        this.timeProvider = new ManualTimer();
    }

    //region StreamSegmentAppendOperation

    /**
     * Tests the preProcess method with StreamSegmentAppend operations.
     * Scenarios:
     * * Recovery Mode
     * * Non-recovery mode
     * * StreamSegment is Merged (both in-transaction and in-metadata)
     * * StreamSegment is Sealed (both in-transaction and in-metadata)
     */
    @Test
    public void testPreProcessStreamSegmentAppend() throws Exception {
        val metadata = createMetadata();
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When everything is OK (in recovery mode) - nothing should change.
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.preProcessOperation(appendOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentOffset after call to preProcess in recovery mode.",
                0, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state in recovery mode.",
                SEGMENT_LENGTH, txn1.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata in recovery mode.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                SEGMENT_LENGTH, txn2.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());

        // When StreamSegment is merged (via transaction).
        StreamSegmentAppendOperation transactionAppendOp = new StreamSegmentAppendOperation(SEALED_SOURCE_ID, DEFAULT_APPEND_DATA, null);
        MergeSegmentOperation mergeOp = createMerge();
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in transaction).",
                () -> txn2.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in metadata).",
                () -> txn2.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        StreamSegmentSealOperation sealOp = createSeal();
        txn2.preProcessOperation(sealOp);
        txn2.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in transaction).",
                () -> txn2.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in metadata).",
                () -> txn2.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentAppend operations.
     */
    @Test
    public void testAcceptStreamSegmentAppend() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(appendOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception.",
                SEGMENT_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());

        // When all is good.
        txn.preProcessOperation(appendOp);
        txn.acceptOperation(appendOp);
        Assert.assertEquals("acceptOperation did not update the transaction.",
                SEGMENT_LENGTH + appendOp.getData().getLength(), txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to process (and accept) StreamSegmentAppendOperations with
     * predefined offsets.
     */
    @Test
    public void testStreamSegmentAppendWithOffset() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Append #1 (at offset 0).
        long offset = metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength();
        StreamSegmentAppendOperation appendOp = createAppendWithOffset(offset);
        txn.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                offset, txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                offset, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        txn.acceptOperation(appendOp);

        // Append #2 (after Append #1)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength();
        appendOp = createAppendWithOffset(offset);
        txn.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                offset, txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        txn.acceptOperation(appendOp);

        // Append #3 (wrong offset)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength() - 1;
        StreamSegmentAppendOperation badAppendOp = createAppendWithOffset(offset);
        AssertExtensions.assertThrows(
                "preProcessOperations accepted an append with the wrong offset.",
                () -> txn.preProcessOperation(badAppendOp),
                ex -> ex instanceof BadOffsetException);

        // Append #4 (wrong offset + wrong attribute). AS PER SEGMENT STORE CONTRACT, BadAttributeUpdateException takes precedence.
        badAppendOp.getAttributeUpdates().add(new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.ReplaceIfEquals, 1, 1234));
        AssertExtensions.assertThrows(
                "preProcessOperations failed with wrong exception when append has both bad offset and bad attribute.",
                () -> txn.preProcessOperation(badAppendOp),
                ex -> ex instanceof BadAttributeUpdateException);

        AssertExtensions.assertThrows(
                "acceptOperation accepted an append that was rejected during preProcessing.",
                () -> txn.acceptOperation(badAppendOp),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to handle StreamSegmentAppends with valid Attribute Updates.
     */
    @Test
    public void testStreamSegmentAppendWithAttributes() throws Exception {
        testWithAttributes(attributeUpdates -> new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, attributeUpdates));
        testWithAttributesByReference(attributeUpdates -> new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, attributeUpdates));
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to reject StreamSegmentAppends with invalid Attribute Updates.
     */
    @Test
    public void testStreamSegmentAppendWithBadAttributes() throws Exception {
        Consumer<Operation> checkAfterSuccess = op -> {
            val a = (StreamSegmentAppendOperation) op;
            Assert.assertEquals("Expected SegmentOffset to have been updated.", SEGMENT_LENGTH, a.getStreamSegmentOffset());
        };
        Consumer<Operation> checkAfterRejection = op -> {
            val a = (StreamSegmentAppendOperation) op;
            AssertExtensions.assertLessThan("Not expected SegmentOffset to have been updated.", 0, a.getStreamSegmentOffset());
        };
        testWithBadAttributes(attributeUpdates -> new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, attributeUpdates),
                checkAfterSuccess, checkAfterRejection);
    }

    //endregion

    //region UpdateAttributesOperation

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to handle UpdateAttributeOperations with valid Attribute Updates.
     */
    @Test
    public void testUpdateAttributes() throws Exception {
        testWithAttributes(attributeUpdates -> new UpdateAttributesOperation(SEGMENT_ID, attributeUpdates));
        testWithAttributesByReference(attributeUpdates -> new UpdateAttributesOperation(SEGMENT_ID, attributeUpdates));
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to reject UpdateAttributeOperations with invalid Attribute Updates.
     */
    @Test
    public void testUpdateAttributesWithBadValues() throws Exception {
        testWithBadAttributes(attributeUpdates -> new UpdateAttributesOperation(SEGMENT_ID, attributeUpdates));
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to reject UpdateAttributeOperations that attempt to
     * modify immutable Attributes
     */
    @Test
    public void testUpdateAttributesImmutable() {
        final AttributeId immutableAttribute = Attributes.ATTRIBUTE_SEGMENT_TYPE;
        Assert.assertTrue(Attributes.isUnmodifiable(immutableAttribute));

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        val coreUpdate = new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(new AttributeUpdate(immutableAttribute, AttributeUpdateType.Replace, 1L)));
        AssertExtensions.assertThrows(
                "Immutable attribute update succeeded.",
                () -> txn.preProcessOperation(coreUpdate),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to handle UpdateAttributeOperations after the segment
     * has been sealed.
     */
    @Test
    public void testUpdateAttributesSealedSegment() throws Exception {
        final AttributeId coreAttribute = Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER;
        final AttributeId extAttribute = AttributeId.randomUUID();

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Seal the segment.
        val sealOp = createSeal();
        txn.preProcessOperation(sealOp);
        txn.acceptOperation(sealOp);

        // 1. Core attribute, but not internal.
        val coreUpdate = new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(new AttributeUpdate(coreAttribute, AttributeUpdateType.Replace, 1L)));
        AssertExtensions.assertThrows(
                "Non-internal update of core attribute succeeded.",
                () -> txn.preProcessOperation(coreUpdate),
                ex -> ex instanceof StreamSegmentSealedException);

        // 2. Core attribute, internal update.
        coreUpdate.setInternal(true);
        txn.preProcessOperation(coreUpdate);
        txn.acceptOperation(coreUpdate);
        Assert.assertEquals("Core attribute was not updated.",
                1L, (long) txn.getStreamSegmentMetadata(SEGMENT_ID).getAttributes().get(coreAttribute));

        // 3. Extended attributes.
        val extUpdate1 = new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(new AttributeUpdate(extAttribute, AttributeUpdateType.Replace, 1L)));
        extUpdate1.setInternal(true);
        AssertExtensions.assertThrows(
                "Extended attribute update succeeded.",
                () -> txn.preProcessOperation(extUpdate1),
                ex -> ex instanceof StreamSegmentSealedException);

        val extUpdate2 = new UpdateAttributesOperation(SEGMENT_ID, AttributeUpdateCollection.from(
                new AttributeUpdate(coreAttribute, AttributeUpdateType.Replace, 2L),
                new AttributeUpdate(extAttribute, AttributeUpdateType.Replace, 3L)));
        extUpdate1.setInternal(true);
        AssertExtensions.assertThrows(
                "Mixed attribute update succeeded.",
                () -> txn.preProcessOperation(extUpdate2),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the ability to validate the type and lengths of Attribute Ids coming in via appends or update attributes,
     * in accordance with the Segment's declared attribute id length.
     */
    @Test
    public void testAttributeIdLengthValidation() throws Exception {
        final AttributeId coreAttribute = Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER;
        final AttributeId extAttributeUUID = AttributeId.randomUUID();
        final AttributeId extAttributeShort1 = AttributeId.random(AttributeId.UUID.ATTRIBUTE_ID_LENGTH);
        final AttributeId extAttributeShort2 = AttributeId.random(AttributeId.UUID.ATTRIBUTE_ID_LENGTH);
        final AttributeId extAttributeLong1 = AttributeId.random(AttributeId.Variable.MAX_LENGTH);

        Function<AttributeId, UpdateAttributesOperation> createOp = id -> new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(new AttributeUpdate(id, AttributeUpdateType.Replace, 1L)));

        val metadata = createMetadata();
        val sm = metadata.getStreamSegmentMetadata(SEGMENT_ID);

        // 1. All UUIDs
        val txn1 = createUpdateTransaction(metadata);
        txn1.preProcessOperation(createOp.apply(coreAttribute)); // Core attributes must always be allowed.
        txn1.preProcessOperation(createOp.apply(extAttributeUUID)); // Extended UUID attribute should be allowed in this case.
        AssertExtensions.assertThrows(
                "Variable-Length accepted when no length declared",
                () -> txn1.preProcessOperation(createOp.apply(extAttributeShort1)),
                ex -> ex instanceof AttributeIdLengthMismatchException);

        // 2. Declare UUID, try Variable length.
        sm.updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_ID_LENGTH, 0L));
        sm.refreshDerivedProperties();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(createOp.apply(coreAttribute)); // Core attributes must always be allowed.
        txn2.preProcessOperation(createOp.apply(extAttributeUUID)); // Extended UUID attribute should be allowed in this case.
        AssertExtensions.assertThrows(
                "Variable-Length accepted when length declared to be 0 (UUID).",
                () -> txn2.preProcessOperation(createOp.apply(extAttributeShort1)),
                ex -> ex instanceof AttributeIdLengthMismatchException);

        // 3. Variable Lengths declared
        sm.updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_ID_LENGTH, (long) extAttributeShort1.byteCount()));
        sm.refreshDerivedProperties();
        val txn3 = createUpdateTransaction(metadata);
        txn3.preProcessOperation(createOp.apply(coreAttribute)); // Core attributes must always be allowed.
        txn3.preProcessOperation(createOp.apply(extAttributeShort1));
        txn3.preProcessOperation(createOp.apply(extAttributeShort2));
        AssertExtensions.assertThrows(
                "UUID accepted when length declared to be Variable.",
                () -> txn3.preProcessOperation(createOp.apply(extAttributeUUID)),
                ex -> ex instanceof AttributeIdLengthMismatchException);
        AssertExtensions.assertThrows(
                "Wrong-length accepted when length declared to be Variable.",
                () -> txn3.preProcessOperation(createOp.apply(extAttributeLong1)),
                ex -> ex instanceof AttributeIdLengthMismatchException);
    }

    private void testWithAttributes(Function<AttributeUpdateCollection, Operation> createOperation) throws Exception {
        final AttributeId attributeNoUpdate = AttributeId.randomUUID();
        final AttributeId attributeAccumulate = AttributeId.randomUUID();
        final AttributeId attributeReplace = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfGreater = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfEquals = AttributeId.randomUUID();

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Update #1.
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 1)); // Initial add, so it's ok.
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.Replace, 1)); // Need to initialize to something.
        val expectedValues = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        expectedValues.put(Attributes.ATTRIBUTE_SEGMENT_TYPE, DEFAULT_TYPE.getValue());

        Operation op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // Verify that the AttributeUpdates still have the same values (there was nothing there prior) and that the updater
        // has internalized the attribute updates.
        verifyAttributeUpdates("after acceptOperation (1)", txn, attributeUpdates, expectedValues);

        // Update #2: update all attributes that can be updated.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1)); // 1 + 1 = 2
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 2, 1));
        expectedValues.put(attributeAccumulate, 2L);
        expectedValues.put(attributeReplace, 2L);
        expectedValues.put(attributeReplaceIfGreater, 2L);
        expectedValues.put(attributeReplaceIfEquals, 2L);

        op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // This is still in the transaction, so we need to add it for comparison sake.
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 1));
        verifyAttributeUpdates("after acceptOperation (2)", txn, attributeUpdates, expectedValues);

        // Update #3: after commit, verify that attributes are committed when they need to.
        val previousAcceptedValues = new HashMap<>(expectedValues);
        txn.commit(metadata);
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1)); // 2 + 1 = 3
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 3));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 3));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 3, 2));
        expectedValues.put(attributeAccumulate, 3L);
        expectedValues.put(attributeReplace, 3L);
        expectedValues.put(attributeReplaceIfGreater, 3L);
        expectedValues.put(attributeReplaceIfEquals, 3L);

        op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after commit+acceptOperation, but prior to second commit.",
                previousAcceptedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
        verifyAttributeUpdates("after commit+acceptOperation", txn, attributeUpdates, expectedValues);

        // Final step: commit Append #3, and verify final segment metadata.
        txn.commit(metadata);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after final commit.",
                expectedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    private void testWithAttributesByReference(Function<AttributeUpdateCollection, Operation> createOperation) throws Exception {
        final AttributeId referenceAttributeId = AttributeId.randomUUID();
        final AttributeId attributeSegmentLength = AttributeId.randomUUID();
        final long initialAttributeValue = 1234567;

        UpdateableContainerMetadata metadata = createMetadata();
        metadata.getStreamSegmentMetadata(SEGMENT_ID)
                .updateAttributes(ImmutableMap.of(referenceAttributeId, initialAttributeValue));

        val txn = createUpdateTransaction(metadata);

        // Update #1.
        AttributeUpdateCollection attributeUpdates = AttributeUpdateCollection.from(
                new AttributeUpdate(referenceAttributeId, AttributeUpdateType.Accumulate, 2),
                new DynamicAttributeUpdate(attributeSegmentLength, AttributeUpdateType.None, DynamicAttributeValue.segmentLength(5)));

        Map<AttributeId, Long> expectedValues = ImmutableMap.of(
                Attributes.ATTRIBUTE_SEGMENT_TYPE, DEFAULT_TYPE.getValue(),
                referenceAttributeId, initialAttributeValue + 2,
                attributeSegmentLength, SEGMENT_LENGTH + 5);

        Operation op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // Verify result.
        verifyAttributeUpdates("after acceptOperation", txn, attributeUpdates, expectedValues);
        txn.commit(metadata);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after final commit.",
                expectedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    private void testWithBadAttributes(Function<AttributeUpdateCollection, Operation> createOperation) throws Exception {
        testWithBadAttributes(createOperation, null, null);
    }

    private void testWithBadAttributes(Function<AttributeUpdateCollection, Operation> createOperation,
                                       Consumer<Operation> checkAfterSuccess, Consumer<Operation> checkAfterRejection) throws Exception {
        final AttributeId attributeNoUpdate = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfGreater = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfEquals = AttributeId.randomUUID();
        final AttributeId attributeReplaceIfEqualsNullValue = AttributeId.randomUUID();
        if (checkAfterSuccess == null) {
            checkAfterSuccess = TestUtils::doNothing;
        }
        if (checkAfterRejection == null) {
            checkAfterRejection = TestUtils::doNothing;
        }

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        BiFunction<Throwable, Boolean, Boolean> exceptionChecker = (ex, expectNoPreviousValue) ->
                (ex instanceof BadAttributeUpdateException) && ((BadAttributeUpdateException) ex).isPreviousValueMissing() == expectNoPreviousValue;

        // Values not set
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.ReplaceIfEquals, 0, 0));
        val op1 = createOperation.apply(attributeUpdates);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to CAS-update an attribute with no previous value.",
                () -> txn.preProcessOperation(op1),
                ex -> exceptionChecker.apply(ex, true));
        checkAfterRejection.accept(op1);

        // Append #1.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 2)); // Initial add, so it's ok.
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.Replace, 2)); // Initial Add.
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEqualsNullValue, AttributeUpdateType.None, Attributes.NULL_ATTRIBUTE_VALUE));
        val expectedValues = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        expectedValues.put(Attributes.ATTRIBUTE_SEGMENT_TYPE, DEFAULT_TYPE.getValue());

        Operation op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);
        checkAfterSuccess.accept(op);

        // ReplaceIfEquals fails when the current attribute value is NULL_ATTRIBUTE_VALUE (this should not set the IsPreviousValueMissing flag).
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEqualsNullValue, AttributeUpdateType.ReplaceIfEquals, 1, 1));
        val op2 = createOperation.apply(attributeUpdates);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to CAS-update an attribute with no previous value.",
                () -> txn.preProcessOperation(op2),
                ex -> exceptionChecker.apply(ex, false));
        checkAfterRejection.accept(op2);

        // Append #2: Try to update attribute that cannot be updated.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 3));
        val op3 = createOperation.apply(attributeUpdates);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an unmodifiable attribute.",
                () -> txn.preProcessOperation(op3),
                ex -> exceptionChecker.apply(ex, false));
        checkAfterRejection.accept(op2);

        // Append #3: Try to update attribute with bad value for ReplaceIfGreater attribute.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 1));
        val op4 = createOperation.apply(attributeUpdates);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an attribute with the wrong value for ReplaceIfGreater.",
                () -> txn.preProcessOperation(op4),
                ex -> exceptionChecker.apply(ex, false));
        checkAfterRejection.accept(op4);

        // Append #4: Try to update attribute with bad value for ReplaceIfEquals attribute.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 3, 3));
        val op5 = createOperation.apply(attributeUpdates);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an attribute with the wrong comparison value for ReplaceIfGreater.",
                () -> txn.preProcessOperation(op5),
                ex -> exceptionChecker.apply(ex, false));
        checkAfterRejection.accept(op5);

        // Reset the attribute update list to its original state so we can do the final verification.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 2, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEqualsNullValue, AttributeUpdateType.None, Attributes.NULL_ATTRIBUTE_VALUE));
        verifyAttributeUpdates("after rejected operations", txn, attributeUpdates, expectedValues);
        txn.commit(metadata);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after commit.",
                expectedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    //endregion

    //region StreamSegmentSealOperation

    /**
     * Tests the preProcess method with StreamSegmentSeal operations.
     * Scenarios:
     * * Recovery Mode
     * * Non-recovery mode
     * * StreamSegment is Merged (both in-transaction and in-metadata)
     * * StreamSegment is Sealed (both in-transaction and in-metadata)
     */
    @Test
    public void testPreProcessStreamSegmentSeal() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        StreamSegmentSealOperation sealOp = createSeal();

        // When everything is OK (in recovery mode) - nothing should change.
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.preProcessOperation(sealOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentLength after call to preProcess in recovery mode.",
                0, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state in recovery mode.",
                txn1.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata in recovery mode.",
                metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(sealOp);
        Assert.assertEquals("Unexpected StreamSegmentLength after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state.",
                txn2.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata.",
                metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When StreamSegment is merged (via transaction).
        StreamSegmentSealOperation transactionSealOp = new StreamSegmentSealOperation(SEALED_SOURCE_ID);
        MergeSegmentOperation mergeOp = createMerge();
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in transaction).",
                () -> txn2.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in metadata).",
                () -> txn2.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        txn2.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in transaction).",
                () -> txn2.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in metadata).",
                () -> txn2.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentSeal operations.
     */
    @Test
    public void testAcceptStreamSegmentSeal() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // Set some attributes.
        val segmentAttributes = createAttributes();
        segmentAttributes.put(Attributes.CREATION_TIME, 1L);
        UpdateableSegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        segmentMetadata.updateAttributes(segmentAttributes);
        val txn = createUpdateTransaction(metadata);
        StreamSegmentSealOperation sealOp = createSeal();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(sealOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertFalse("acceptOperation updated the transaction even if it threw an exception.",
                txn.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When all is good.
        txn.preProcessOperation(sealOp);
        txn.acceptOperation(sealOp);
        Assert.assertTrue("acceptOperation did not update the transaction.", txn.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", segmentMetadata.isSealed());

        txn.commit(metadata);

        // Check attributes.
        DEFAULT_TYPE.intoAttributes(segmentAttributes);
        SegmentMetadataComparer.assertSameAttributes("Unexpected set of attributes after commit.", segmentAttributes, segmentMetadata);
    }

    //endregion

    //region StreamSegmentTruncate

    /**
     * Tests the preProcess method with StreamSegmentTruncate operations.
     * Scenarios:
     * * Recovery Mode
     * * Non-recovery mode
     * * Invalid states or arguments (Segment not sealed, bad offsets, Transaction Segment).
     */
    @Test
    public void testPreProcessStreamSegmentTruncate() throws Exception {
        final UpdateableContainerMetadata metadata = createMetadata();

        // When trying to truncate beyond last offset.
        val txn = createUpdateTransaction(metadata);
        AssertExtensions.assertThrows(
                "preProcess did not throw when offset is too large.",
                () -> txn.preProcessOperation(createTruncate(SEGMENT_LENGTH + 1)),
                ex -> ex instanceof BadOffsetException);

        // Actually truncate the segment, and re-verify bounds.
        val op1 = createTruncate(SEGMENT_LENGTH / 2);
        txn.preProcessOperation(op1);
        txn.acceptOperation(op1);
        txn.commit(metadata);

        AssertExtensions.assertThrows(
                "preProcess did not throw when offset is too small (on truncated segment).",
                () -> txn.preProcessOperation(createTruncate(op1.getStreamSegmentOffset() - 1)),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertThrows(
                "preProcess did not throw when offset is too large (on truncated segment).",
                () -> txn.preProcessOperation(createTruncate(SEGMENT_LENGTH + 1)),
                ex -> ex instanceof BadOffsetException);

        // Now verify that a valid offset does work (not throwing means the test passes).
        txn.preProcessOperation(createTruncate(op1.getStreamSegmentOffset()));

        // Verify it works on a Sealed Segment.
        val sealOp = createSeal();
        txn.preProcessOperation(sealOp);
        txn.acceptOperation(sealOp);
        txn.commit(metadata);

        // Truncate the segment again.
        txn.preProcessOperation(createTruncate(op1.getStreamSegmentOffset() + 1));
        txn.preProcessOperation(createTruncate(SEGMENT_LENGTH));
    }

    /**
     * Tests the acceptOperation method with StreamSegmentTruncate operations.
     */
    @Test
    public void testAcceptStreamSegmentTruncate() throws Exception {
        val metadata = createMetadata();
        val append = createAppendNoOffset();
        val seal = createSeal(); // Here, we also Seal, since in preProcessStreamSegmentTruncate we did not.
        final long truncateOffset = SEGMENT_LENGTH + append.getLength() / 2;
        val truncate = createTruncate(truncateOffset);

        // Apply all operations in order, in the same transaction. This helps verify that, should these operations happen
        // concurrently, they are applied to the metadata in the correct order.
        val txn1 = createUpdateTransaction(metadata);
        for (Operation o : Arrays.asList(append, seal, truncate)) {
            txn1.preProcessOperation(o);
            txn1.acceptOperation(o);
        }

        // Attempt some more invalid truncate operations.
        AssertExtensions.assertThrows(
                "preProcessOperation accepted a truncate operation with wrong offset (smaller).",
                () -> txn1.preProcessOperation(createTruncate(truncateOffset - 1)),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertThrows(
                "preProcessOperation accepted a truncate operation with wrong offset (larger).",
                () -> txn1.preProcessOperation(createTruncate(truncateOffset + append.getLength())),
                ex -> ex instanceof BadOffsetException);

        // Verify the Update Transaction has been updated, but the metadata has not yet been touched.
        val sm = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected StartOffset in UpdateTransaction.",
                truncateOffset, txn1.getStreamSegmentMetadata(SEGMENT_ID).getStartOffset());
        Assert.assertEquals("Unexpected StartOffset in Metadata pre-commit.", 0, sm.getStartOffset());

        // Commit and verify that the metadata has been correctly updated.
        txn1.commit(metadata);
        Assert.assertEquals("Unexpected StartOffset in Metadata post-commit.", truncateOffset, sm.getStartOffset());
        Assert.assertEquals("Unexpected Length in Metadata post-commit.",
                append.getStreamSegmentOffset() + append.getLength(), sm.getLength());
        Assert.assertTrue("Unexpected Sealed status in Metadata post-commit.", sm.isSealed());

        // Verify single truncate operation (check to see that it reads from actual metadata if needed).
        val op2 = createTruncate(truncateOffset + 1);
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(op2);
        txn2.acceptOperation(op2);
        txn2.commit(metadata);
        Assert.assertEquals("Unexpected StartOffset in Metadata post-commit (second).", op2.getStreamSegmentOffset(), sm.getStartOffset());

        // Verify truncating the entire segment.
        val op3 = createTruncate(sm.getLength());
        val txn3 = createUpdateTransaction(metadata);
        txn3.preProcessOperation(op3);
        txn3.acceptOperation(op3);
        txn3.commit(metadata);
        Assert.assertEquals("Unexpected StartOffset in Metadata when truncating entire segment.", sm.getLength(), sm.getStartOffset());
    }

    //endregion

    //region DeleteSegmentOperation

    /**
     * Tests the preProcess and accept method with DeleteSegmentOperations.
     */
    @Test
    public void testProcessDeleteSegmentOperation() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // Get the metadata
        UpdateableSegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        val txn = createUpdateTransaction(metadata);

        // Process the operation.
        DeleteSegmentOperation deleteOp = createDelete();
        txn.preProcessOperation(deleteOp);
        txn.acceptOperation(deleteOp);

        // Verify pre-commit.
        Assert.assertTrue("acceptOperation did not update the transaction.", txn.getStreamSegmentMetadata(SEGMENT_ID).isDeleted());
        Assert.assertFalse("acceptOperation updated the metadata.", segmentMetadata.isDeleted());
        AssertExtensions.assertThrows("preProcess allowed the operation even though the Segment is deleted.",
                () -> txn.preProcessOperation(deleteOp),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Verify post-commit.
        txn.commit(metadata);
        Assert.assertTrue("commit did not update the metadata.", segmentMetadata.isDeleted());
    }

    //endregion


    //region MergeSegmentOperation

    /**
     * Tests the preProcess method with MergeTransactionOperations.
     * Scenarios:
     * * Recovery/non-recovery mode
     * * Target StreamSegment is sealed
     * * Target StreamSegment is a Transaction
     * * Source StreamSegment is already merged
     * * Source StreamSegment is not sealed
     */
    @Test
    public void testPreProcessMergeSegment() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // When everything is OK (recovery mode).
        MergeSegmentOperation recoveryMergeOp = createMerge();
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        AssertExtensions.assertThrows(
                "preProcess(Merge) handled an operation with no Transaction StreamSegment Length set.",
                () -> txn1.preProcessOperation(createMerge()),
                ex -> ex instanceof MetadataUpdateException);

        // In recovery mode, the updater does not set the length; it just validates that it has one.
        recoveryMergeOp.setLength(metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).getLength());
        txn1.preProcessOperation(recoveryMergeOp);
        AssertExtensions.assertLessThan("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.",
                0, recoveryMergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(recoveryMergeOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.",
                txn1.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.",
                metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());

        // When everything is OK (non-recovery mode).
        MergeSegmentOperation mergeOp = createMerge();
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(mergeOp);
        Assert.assertEquals("Unexpected Transaction StreamSegmentLength after call to preProcess in non-recovery mode.",
                SEALED_SOURCE_LENGTH, mergeOp.getLength());
        Assert.assertEquals("Unexpected Target StreamSegmentOffset after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, mergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(mergeOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in non-recovery mode.",
                txn2.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in non-recovery mode.",
                metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());

        // When Target StreamSegment is sealed.
        StreamSegmentSealOperation sealTargetOp = createSeal();
        txn2.preProcessOperation(sealTargetOp);
        txn2.acceptOperation(sealTargetOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is sealed.",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentSealedException);

        txn2.clear(); // Rollback the seal

        // When Transaction is not sealed.
        MergeSegmentOperation mergeNonSealed = new MergeSegmentOperation(NOTSEALED_SOURCE_ID, SEGMENT_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is not sealed.",
                () -> txn2.preProcessOperation(mergeNonSealed),
                ex -> ex instanceof StreamSegmentNotSealedException);

        // When Transaction is already merged.
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in transaction).",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);

        txn2.commit(metadata);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in metadata).",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);
    }

    /**
     * Tests the accept method with MergeTransactionOperations.
     */
    @Test
    public void testAcceptMergeSegment() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        MergeSegmentOperation mergeOp = createMerge();

        // When no pre-process has happened
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(mergeOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception (target segment).",
                SEGMENT_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("acceptOperation updated the metadata (target segment).",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        txn.clear(); // This would naturally happen in case of a failure, so we need to simulate this here too.

        // When all is good.
        txn.preProcessOperation(mergeOp);
        txn.acceptOperation(mergeOp);
        Assert.assertTrue("acceptOperation did not update the transaction(Transaction).",
                txn.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        Assert.assertFalse("acceptOperation updated the metadata (Transaction).",
                metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID).isMerged());
        Assert.assertEquals("acceptOperation did not update the transaction.",
                SEGMENT_LENGTH + SEALED_SOURCE_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getLength());
    }

    //endregion

    //region MetadataOperations

    /**
     * Tests the preProcessOperation and acceptOperation methods with StreamSegmentMap operations.
     */
    @Test
    public void testProcessStreamSegmentMap() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();

        // Part 1: recovery mode (no-op).
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);

        // Brand new Table Segment. Play around with Segment Type to verify that it is properly propagated to the SegmentMetadata.
        val initialAttributes = createAttributes();
        DEFAULT_TYPE.intoAttributes(initialAttributes);
        initialAttributes.put(TableAttributes.INDEX_OFFSET, 0L);
        val expectedSegmentType = SegmentType.builder(DEFAULT_TYPE).tableSegment().build();
        val mapOp = new StreamSegmentMapOperation(StreamSegmentInformation.builder()
                .name(SEGMENT_NAME)
                .length(SEGMENT_LENGTH)
                .startOffset(SEGMENT_LENGTH / 2)
                .sealed(true)
                .attributes(initialAttributes)
                .build());

        txn1.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation did modify the StreamSegmentId on the operation in recovery mode.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        txn2.acceptOperation(mapOp);

        val updaterMetadata = txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getStorageLength());
        Assert.assertEquals("Unexpected Length after call to acceptOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getLength());
        Assert.assertEquals("Unexpected value for isSealed after call to acceptOperation (in transaction).",
                mapOp.isSealed(), updaterMetadata.isSealed());
        Assert.assertEquals("Unexpected value for StartOffset after call to acceptOperation (in transaction).",
                mapOp.getStartOffset(), updaterMetadata.getStartOffset());
        Assert.assertNull("acceptOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // StreamSegmentName already exists (transaction) and we try to map with new id.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in transaction).",
                () -> txn2.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        txn2.commit(metadata);

        val segmentMetadata = metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        Assert.assertFalse("Not expecting segment to be pinned yet.", segmentMetadata.isPinned());

        val expectedAttributes = new HashMap<>(mapOp.getAttributes());
        expectedSegmentType.intoAttributes(expectedAttributes);
        AssertExtensions.assertMapEquals("Unexpected attributes in SegmentMetadata after call to commit().",
                expectedAttributes, segmentMetadata.getAttributes());

        // StreamSegmentName already exists (metadata) and we try to map with new id.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in metadata).",
                () -> txn2.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        val length = segmentMetadata.getLength() + 5;
        val storageLength = segmentMetadata.getStorageLength() + 1;
        val startOffset = segmentMetadata.getStartOffset() + 1;
        segmentMetadata.setLength(length);

        // StreamSegmentName already exists and we try to map with the same id. Verify that we are able to update its
        // StorageLength (if different).
        val updateMap = new StreamSegmentMapOperation(StreamSegmentInformation.builder()
                .name(mapOp.getStreamSegmentName())
                .startOffset(startOffset)
                .length(storageLength)
                .sealed(true)
                .attributes(createAttributes())
                .build());
        updateMap.setStreamSegmentId(mapOp.getStreamSegmentId());
        txn2.preProcessOperation(updateMap);
        txn2.acceptOperation(updateMap);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (in transaction).",
                storageLength, txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        txn2.commit(metadata);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (post-commit).",
                storageLength, metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        Assert.assertEquals("Unexpected Length after call to acceptOperation with remap (post-commit).",
                length, metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getLength());
        Assert.assertEquals("Unexpected StartOffset after call to acceptOperation with remap (post-commit).",
                startOffset, metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStartOffset());

        // Pinned segments.
        val pinnedMap = new StreamSegmentMapOperation(StreamSegmentInformation
                .builder()
                .name(mapOp.getStreamSegmentName() + "_pinned")
                .startOffset(startOffset)
                .length(storageLength)
                .sealed(true)
                .attributes(createAttributes())
                .build());
        pinnedMap.markPinned();
        txn2.preProcessOperation(pinnedMap);
        txn2.acceptOperation(pinnedMap);
        txn2.commit(metadata);
        val pinnedMetadata = metadata.getStreamSegmentMetadata(metadata.getStreamSegmentId(pinnedMap.getStreamSegmentName(), false));
        Assert.assertTrue("Unexpected isPinned for pinned map.", pinnedMetadata.isPinned());

        // Truncate offset is beyond the length.
        val truncateMap = new StreamSegmentMapOperation(StreamSegmentInformation
                .builder()
                .name(mapOp.getStreamSegmentName() + "_truncate")
                .length(storageLength)
                .startOffset(storageLength) // StreamSegmentInformation does not allow us to exceed Length for this value.
                .sealed(true)
                .attributes(createAttributes())
                .build());
        txn2.preProcessOperation(truncateMap);
        txn2.acceptOperation(truncateMap);
        txn2.commit(metadata);
        val truncatedMetadata = metadata.getStreamSegmentMetadata(metadata.getStreamSegmentId(truncateMap.getStreamSegmentName(), false));
        Assert.assertEquals("Unexpected startOffset for over-zealously truncated segment.", truncatedMetadata.getLength(), truncatedMetadata.getStartOffset());
    }

    /**
     * Tests the ability to reject new StreamSegment/Transaction map operations that would exceed the max allowed counts.
     */
    @Test
    public void testSegmentMapMax() throws Exception {
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID)
                .withMaxActiveSegmentCount(3).build();
        metadata.mapStreamSegmentId("a", SEGMENT_ID);
        metadata.mapStreamSegmentId("b", 123457);

        // Non-recovery mode.
        val txn1 = createUpdateTransaction(metadata);

        // Map one segment, which should fill up the quota.
        StreamSegmentMapOperation acceptedMap = createMap();
        txn1.preProcessOperation(acceptedMap);
        txn1.acceptOperation(acceptedMap);

        // Verify non-recovery mode.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a StreamSegment that would exceed the active segment quota.",
                () -> txn1.preProcessOperation(createMap("foo")),
                ex -> ex instanceof TooManyActiveSegmentsException);

        // Verify recovery mode.
        metadata.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        //updater.setOperationSequenceNumber(10000);
        StreamSegmentMapOperation secondMap = createMap("c");
        secondMap.setStreamSegmentId(1234);
        txn2.preProcessOperation(secondMap);
        txn2.acceptOperation(secondMap);

        StreamSegmentMapOperation thirdMap = createTransactionMap("a_txn2");
        thirdMap.setStreamSegmentId(1235);
        txn2.preProcessOperation(thirdMap);
        txn2.acceptOperation(thirdMap);
        txn2.commit(metadata);
        metadata.exitRecoveryMode();

        Assert.assertNotNull("Updater did not create metadata for new segment in recovery mode even if quota is exceeded (1).",
                metadata.getStreamSegmentMetadata(secondMap.getStreamSegmentId()));
        Assert.assertNotNull("Updater did not create metadata for new segment in recovery mode even if quota is exceeded (2).",
                metadata.getStreamSegmentMetadata(thirdMap.getStreamSegmentId()));
    }

    /**
     * Tests the processMetadataOperation method with MetadataCheckpoint operations.
     */
    @Test
    public void testProcessMetadataCheckpoint() throws Exception {
        // When encountering MetadataCheckpoint in non-Recovery Mode, the ContainerMetadataUpdateTransaction serializes a snapshot
        // of the current metadata inside the Operation.
        // When encountering MetadataCheckpoint in Recovery Mode, the ContainerMetadataUpdateTransaction deserializes the snapshot-ted
        // metadata in it and applies it to the container metadata (inside the transaction). All existing metadata updates
        // are cleared.
        String newSegmentName = "NewSegmentId";
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata, and in addition, seal a segment and truncate it.
        this.timeProvider.setElapsedMillis(1234);
        UpdateableContainerMetadata metadata = createMetadata();
        metadata.getStreamSegmentMetadata(SEGMENT_ID).markSealed();
        metadata.getStreamSegmentMetadata(SEGMENT_ID).setStartOffset(SEGMENT_LENGTH / 2);
        val txn = createUpdateTransaction(metadata);

        // Checkpoint 1: original metadata.
        // Checkpoint 2: Checkpoint 1 + 1 StreamSegment and 1 Transaction + 1 Append
        MetadataCheckpointOperation checkpoint1 = createMetadataCheckpoint();
        MetadataCheckpointOperation checkpoint2 = createMetadataCheckpoint();

        // Checkpoint 1 Should have original metadata.
        val checkpoint1Contents = processCheckpointOperation(checkpoint1, txn, seqNo::incrementAndGet);
        Assert.assertNull("Expecting checkpoint contents to be cleared after processing.", checkpoint1.getContents());
        UpdateableContainerMetadata checkpointedMetadata = getCheckpointedMetadata(checkpoint1Contents);
        assertMetadataSame("Unexpected metadata before any operation.", metadata, checkpointedMetadata);

        // Map another StreamSegment, and add an append
        StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(
                StreamSegmentInformation.builder().name(newSegmentName).length(SEGMENT_LENGTH).build());
        processOperation(mapOp, txn, seqNo::incrementAndGet);

        // Add a few Extended Attributes.
        val extendedAttributeUpdates = createAttributeUpdates();
        processOperation(new StreamSegmentAppendOperation(mapOp.getStreamSegmentId(), DEFAULT_APPEND_DATA, extendedAttributeUpdates), txn, seqNo::incrementAndGet);

        // Add a Core Attribute.
        val coreAttributeUpdates = AttributeUpdateCollection.from(new AttributeUpdate(Attributes.EVENT_COUNT, AttributeUpdateType.Replace, 1));
        processOperation(new StreamSegmentAppendOperation(mapOp.getStreamSegmentId(), DEFAULT_APPEND_DATA, coreAttributeUpdates),
                txn, seqNo::incrementAndGet);
        val checkpoint2Contents = processCheckpointOperation(checkpoint2, txn, seqNo::incrementAndGet);
        Assert.assertNull("Expecting checkpoint contents to be cleared after processing.", checkpoint2.getContents());

        // Checkpoint 2 should have Checkpoint 1 + New StreamSegment + Append.
        txn.commit(metadata);
        checkpointedMetadata = getCheckpointedMetadata(checkpoint2Contents);

        // Checkpointing will remove all Extended Attributes. In order to facilitate the comparison, we should remove those
        // too, from the original metadata (after we've serialized the checkpoint).
        val expectedCleared = extendedAttributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, au -> Attributes.NULL_ATTRIBUTE_VALUE));
        metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).updateAttributes(expectedCleared);

        // Now actually check the checkpointed metadata.
        val checkpointedSm = checkpointedMetadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        for (val e : extendedAttributeUpdates) {
            Assert.assertFalse("Extended attribute was serialized.", checkpointedSm.getAttributes().containsKey(e.getAttributeId()));
        }
        checkpointedMetadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).updateAttributes(expectedCleared);
        assertMetadataSame("Unexpected metadata after deserializing checkpoint.", metadata, checkpointedMetadata);
    }

    /**
     * Tests the processMetadataOperation method with StorageMetadataCheckpoint operations.
     */
    @Test
    public void testProcessStorageMetadataCheckpoint() throws Exception {
        // When encountering StorageMetadataCheckpoint in non-Recovery Mode, the ContainerMetadataUpdateTransaction serializes a
        // snapshot of the relevant metadata fields inside the Operation.
        // When encountering StorageMetadataCheckpoint in Recovery Mode, the ContainerMetadataUpdateTransaction deserializes the
        // snapshot-ted metadata in it and applies it to the container metadata (inside the transaction).
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata and seal a segment.
        this.timeProvider.setElapsedMillis(1234);
        val metadata1 = createMetadata();
        val txn1 = createUpdateTransaction(metadata1);
        val segmentMetadata1 = metadata1.getStreamSegmentMetadata(SEGMENT_ID);
        segmentMetadata1.markSealed(); // Need to mark segment sealed before being able to set sealed in storage.

        // Take a full snapshot of the metadata before making any changes.
        val checkpointOperation1 = createMetadataCheckpoint();
        val fullCheckpoint = processCheckpointOperation(checkpointOperation1, txn1, seqNo::incrementAndGet);

        // Update the storage state for this segment: increment its length and mark it as sealed in storage.
        segmentMetadata1.setStorageLength(segmentMetadata1.getStorageLength() + 1);
        segmentMetadata1.markSealedInStorage();

        // Take a storage checkpoint.
        val checkpointOperation2 = createStorageMetadataCheckpoint();
        val storageCheckpoint = processCheckpointOperation(checkpointOperation2, txn1, seqNo::incrementAndGet);
        Assert.assertNull("Expected storage checkpoint operation contents to be null after processing.", checkpointOperation2.getContents());

        // Apply current metadata.
        txn1.commit(metadata1);

        // Create a new metadata and apply the checkpoint during recovery.
        val metadata2 = createBlankMetadata();
        metadata2.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata2);
        processOperation(createCheckpoint(MetadataCheckpointOperation::new, fullCheckpoint, checkpointOperation1.getSequenceNumber()), txn2, () -> 1L);

        txn2.preProcessOperation(createCheckpoint(StorageMetadataCheckpointOperation::new, storageCheckpoint, checkpointOperation2.getSequenceNumber()));
        txn2.commit(metadata2);
        metadata2.exitRecoveryMode();

        // Verify that the Storage Length & StorageSealed status are applied correctly.
        assertMetadataSame("Unexpected metadata after applying storage checkpoint.", metadata1, metadata2);
    }

    /**
     * Tests the processMetadataOperation method with MetadataCheckpoint operations, when such checkpoints are skipped over
     * because they are after other operations.
     */
    @Test
    public void testProcessMetadataCheckpointIgnored() throws Exception {
        long newSegmentId = 897658;
        String newSegmentName = "NewSegmentId";
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata.
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        MetadataCheckpointOperation op1 = createMetadataCheckpoint();
        val checkpoint1 = processCheckpointOperation(op1, txn, seqNo::incrementAndGet);

        // Create a blank metadata, and add an operation to the updater (which would result in mapping a new StreamSegment).
        metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        StreamSegmentMapOperation mapOp = createMap(newSegmentName);
        mapOp.setStreamSegmentId(newSegmentId);
        processOperation(mapOp, txn2, seqNo::incrementAndGet);

        // Now try to process the checkpoint
        MetadataCheckpointOperation op2 = createCheckpoint(MetadataCheckpointOperation::new, checkpoint1, op1.getSequenceNumber());
        processOperation(op2, txn2, seqNo::incrementAndGet);
        Assert.assertNull("Expected checkpoint operation contents to be null after processing.", op2.getContents());
        txn2.commit(metadata);

        // Verify the checkpointed metadata hasn't been applied
        Assert.assertNull("Newly added StreamSegment Id was not removed after applying checkpoint.",
                metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNotNull("Checkpoint seems to have not been applied.", metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    //endregion

    //region Other tests

    /**
     * Tests the behavior of preProcessOperation and acceptOperation when encountering an invalid StreamSegmentId, or
     * when encountering a StreamSegment Id for a deleted StreamSegment.
     */
    @Test
    public void testPreProcessAndAcceptWithInvalidSegmentId() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        val txn = createUpdateTransaction(metadata);

        ArrayList<StorageOperation> testOperations = new ArrayList<>();
        testOperations.add(createAppendNoOffset());
        testOperations.add(createSeal());
        testOperations.add(createMerge());

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for a non-existent Segment: " + op,
                    () -> txn.preProcessOperation(op),
                    ex -> ex instanceof MetadataUpdateException);

            AssertExtensions.assertThrows(
                    "Unexpected behavior from acceptOperation when processing an operation for a non-existent Segment: " + op,
                    () -> txn.acceptOperation(op),
                    ex -> ex instanceof MetadataUpdateException);
        }

        // If the StreamSegment was previously marked as deleted.
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId("foo", SEGMENT_ID);
        segmentMetadata.markDeleted();

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for deleted Segment: " + op,
                    () -> txn.preProcessOperation(op),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to commit all outstanding changes to the same base
     * metadata.
     */
    @Test
    public void testCommitSameTarget() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        testCommit(metadata, metadata);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to commit all outstanding changes to a new target.
     */
    @Test
    public void testCommitNewTarget() throws Exception {
        val referenceMetadata = createMetadata(); // We don't touch this.
        // This just simulates what we're doing in testCommit().
        referenceMetadata.getStreamSegmentMetadata(SEGMENT_ID).updateAttributes(Collections.singletonMap(Attributes.CREATION_TIME, 0L));
        val baseMetadata = createMetadata();
        val newMetadata = createMetadata();
        testCommit(baseMetadata, newMetadata);
        Assert.assertEquals("SequenceNumber changed on base metadata.",
                referenceMetadata.getOperationSequenceNumber(), baseMetadata.getOperationSequenceNumber());
        AssertExtensions.assertContainsSameElements("SegmentIds changed on base metadata",
                referenceMetadata.getAllStreamSegmentIds(), baseMetadata.getAllStreamSegmentIds());
        for (long segmentId : referenceMetadata.getAllStreamSegmentIds()) {
            val referenceSegmentMetadata = referenceMetadata.getStreamSegmentMetadata(segmentId);
            val baseSegmentMetadata = baseMetadata.getStreamSegmentMetadata(segmentId);
            SegmentMetadataComparer.assertEquals("SegmentMetadata changed on base metadata",
                    referenceSegmentMetadata, baseSegmentMetadata);
        }
    }

    /**
     * Tests the rebase() method, albeit in a bit unorthodox manner, since this is not how rebase will really be used -
     * we just want to make sure it actually rebases the UpdateTransaction.
     */
    @Test
    public void testRebase() throws Exception {
        val baseMetadata = createMetadata();
        val newMetadata1 = createMetadata();
        testCommit(baseMetadata, newMetadata1);

        val txn = createUpdateTransaction(baseMetadata);
        txn.rebase(newMetadata1);

        AssertExtensions.assertContainsSameElements("Unexpected SegmentIds after rebase",
                newMetadata1.getAllStreamSegmentIds(), txn.getAllStreamSegmentIds());

        for (long segmentId : newMetadata1.getAllStreamSegmentIds()) {
            val referenceSegmentMetadata = newMetadata1.getStreamSegmentMetadata(segmentId);
            val baseSegmentMetadata = txn.getStreamSegmentMetadata(segmentId);
            SegmentMetadataComparer.assertEquals("Unexpected SegmentMetadata after rebase",
                    referenceSegmentMetadata, baseSegmentMetadata);
        }
    }

    private void testCommit(UpdateableContainerMetadata baseMetadata, UpdateableContainerMetadata targetMetadata) throws Exception {
        // Create a few appends, merge a Transaction and seal the target Segment. Verify all changes have been applied after
        // a call to commit().
        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        int appendAttributeCount = 0;
        for (int i = 0; i < appendCount; i++) {
            StreamSegmentAppendOperation op = createAppendNoOffset();
            operations.add(op);
            appendAttributeCount += op.getAttributeUpdates().size();
        }

        operations.add(createMerge());
        operations.add(createSeal());

        // This is a change we make only to the baseMetadata. This should not propagate to the targetMetadata.
        baseMetadata.getStreamSegmentMetadata(SEGMENT_ID).updateAttributes(Collections.singletonMap(Attributes.CREATION_TIME, 0L));
        val txn = createUpdateTransaction(baseMetadata);
        long expectedLastUsedParent = -1;
        long expectedLastUsedTransaction = -1;
        long seqNo = 0;
        for (StorageOperation op : operations) {
            txn.preProcessOperation(op);
            op.setSequenceNumber(++seqNo);
            txn.acceptOperation(op);
            if (op.getStreamSegmentId() == SEGMENT_ID) {
                expectedLastUsedParent = op.getSequenceNumber();
            }

            if (op instanceof MergeSegmentOperation) {
                expectedLastUsedParent = op.getSequenceNumber();
                expectedLastUsedTransaction = op.getSequenceNumber();
            }
        }

        txn.commit(targetMetadata);
        Assert.assertEquals("commit() seems to have modified the metadata sequence number while not in recovery mode.",
                ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER, targetMetadata.nextOperationSequenceNumber() - 1);

        long expectedLength = SEGMENT_LENGTH + appendCount * DEFAULT_APPEND_DATA.getLength() + SEALED_SOURCE_LENGTH;

        SegmentMetadata parentMetadata = targetMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected Length in metadata after commit.", expectedLength, parentMetadata.getLength());
        Assert.assertTrue("Unexpected value for isSealed in metadata after commit.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Target after commit.", expectedLastUsedParent, parentMetadata);

        SegmentMetadata transactionMetadata = targetMetadata.getStreamSegmentMetadata(SEALED_SOURCE_ID);
        Assert.assertTrue("Unexpected value for isSealed in Source metadata after commit.", transactionMetadata.isSealed());
        Assert.assertTrue("Unexpected value for isMerged in Source metadata after commit.", transactionMetadata.isMerged());
        int expectedParentAttributeCount = appendAttributeCount + (baseMetadata == targetMetadata ? 1 : 0) + 1;
        Assert.assertEquals("Unexpected number of attributes for Target segment.", expectedParentAttributeCount, parentMetadata.getAttributes().size());
        Assert.assertEquals(DEFAULT_TYPE.getValue(), (long) parentMetadata.getAttributes().get(Attributes.ATTRIBUTE_SEGMENT_TYPE));
        Assert.assertEquals("Unexpected number of attributes for Source.", 1, transactionMetadata.getAttributes().size());
        Assert.assertEquals(DEFAULT_TYPE.getValue(), (long) transactionMetadata.getAttributes().get(Attributes.ATTRIBUTE_SEGMENT_TYPE));
        checkLastKnownSequenceNumber("Unexpected lastUsed for Source after commit.", expectedLastUsedTransaction, transactionMetadata);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to set the Sequence Number on the Metadata, when in Recovery Mode.
     */
    @Test
    public void testCommitSequenceNumber() throws Exception {
        final long newSeqNo = 1235;
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.setOperationSequenceNumber(newSeqNo);
        txn1.commit(metadata);
        metadata.exitRecoveryMode();
        Assert.assertEquals("commit() did not set the metadata sequence number while in recovery mode.", newSeqNo + 1, metadata.nextOperationSequenceNumber());

        // Now try again in non-recovery mode.
        val txn2 = createUpdateTransaction(metadata);
        AssertExtensions.assertThrows(
                "setOperationSequence number should not work in non-recovery mode.",
                () -> txn2.setOperationSequenceNumber(newSeqNo + 10),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to rollback all outstanding changes.
     */
    @Test
    public void testRollback() throws Exception {
        // Create a couple of operations, commit them, and then create a few more appends, merge a Transaction and seal
        // the target Segment.
        // Then call rollback(); verify no changes have been applied after the call to commit().

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        StreamSegmentAppendOperation committedAppend = createAppendNoOffset();
        txn.preProcessOperation(committedAppend);
        txn.acceptOperation(committedAppend);
        txn.commit(metadata); // This is the last extent of the modifications to the metadata.

        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppendNoOffset());
        }

        operations.add(createMerge());
        operations.add(createSeal());

        long seqNo = 0;
        for (StorageOperation op : operations) {
            txn.preProcessOperation(op);
            op.setSequenceNumber(++seqNo);
            txn.acceptOperation(op);
        }

        txn.clear();

        long expectedLength = SEGMENT_LENGTH + DEFAULT_APPEND_DATA.getLength();

        // Verify metadata is untouched and that the updater has truly rolled back.
        SegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected Length in metadata after rollback.", expectedLength, parentMetadata.getLength());
        Assert.assertFalse("Unexpected value for isSealed in metadata after rollback.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Target after rollback.", 0, parentMetadata);

        SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(SEALED_SOURCE_ID);
        Assert.assertFalse("Unexpected value for isMerged in transaction segment metadata after rollback.", transactionMetadata.isMerged());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Transaction segment after rollback.", 0, transactionMetadata);

        // Now the updater
        parentMetadata = txn.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected Length in transaction after rollback.", expectedLength, parentMetadata.getLength());
        Assert.assertFalse("Unexpected value for isSealed in transaction after rollback.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Parent (txn) after rollback.", 0, parentMetadata);

        transactionMetadata = txn.getStreamSegmentMetadata(SEALED_SOURCE_ID);
        Assert.assertFalse("Unexpected value for isMerged in transaction segment in update transaction after rollback.", transactionMetadata.isMerged());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Transaction segment in update transaction after rollback.", 0, transactionMetadata);
    }

    /**
     * Tests the correctness of getActiveSegmentCount(), especially in the case when the real ContainerMetadata has
     * changed its value.
     */
    @Test
    public void testGetActiveSegmentCount() throws Exception {
        // Create a base metadata and make sure it has one active segment.
        UpdateableContainerMetadata metadata = createMetadata();
        int expected = 3;
        Assert.assertEquals("Unexpected initial Active Segment Count for base metadata.",
                expected, metadata.getActiveSegmentCount());

        // Create an UpdateTransaction for it and map a new segment.
        val txn1 = new ContainerMetadataUpdateTransaction(metadata, metadata, 0);
        Assert.assertEquals("Unexpected Active Segment Count for first transaction.",
                expected, txn1.getActiveSegmentCount());
        val map1 = createMap("NewSegment1");
        txn1.preProcessOperation(map1);
        map1.setSequenceNumber(metadata.nextOperationSequenceNumber());
        txn1.acceptOperation(map1);
        expected++;
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after map.",
                expected, txn1.getActiveSegmentCount());

        // Create a second UpdateTransaction for it and map a new segment.
        val txn2 = new ContainerMetadataUpdateTransaction(txn1, metadata, 0);

        Assert.assertEquals("Unexpected Active Segment Count for second transaction.",
                expected, txn2.getActiveSegmentCount());
        val map2 = createMap("NewSegment2");
        txn2.preProcessOperation(map2);
        map2.setSequenceNumber(metadata.nextOperationSequenceNumber());
        txn2.acceptOperation(map2);
        expected++;
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after map.",
                expected, txn2.getActiveSegmentCount());

        // Clean up base metadata - simplest way to deactivate segments (even if this will cause a TxnCommit to fail,
        // but we don't test that here).
        metadata.enterRecoveryMode();
        metadata.reset();
        metadata.exitRecoveryMode();
        expected = 0;

        // Check Active Segment Count after the cleanup.
        Assert.assertEquals("Unexpected Active Segment Count for base metadata after cleanup.",
                expected, metadata.getActiveSegmentCount());
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after cleanup.",
                expected + 1, txn1.getActiveSegmentCount());
        Assert.assertEquals("Unexpected Active Segment Count for second transaction after cleanup.",
                expected + 2, txn2.getActiveSegmentCount());
    }

    @Test
    public void testOperationConcurrentSerializationDeletion() throws ContainerException, StreamSegmentException {
        CompletableFuture<Void> deletion = new CompletableFuture<>();

        InstrumentedContainerMetadata metadata = new InstrumentedContainerMetadata(CONTAINER_ID, 1000, deletion);
        // Add some segments to the metadata.
        populateMetadata(metadata);

        metadata.getGetAllStreamSegmentIdsFuture().thenRun(() -> {
            // Cannot call getStreamSegmentMetadata because it is instrumented and will block, so we must manually construct it.
            UpdateableSegmentMetadata segment = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, 0);
            // Ensures it is eligible for eviction.
            segment.markDeleted();
            metadata.cleanup(Set.of(segment), SEGMENT_LENGTH);
        }).thenRun(() -> {
            deletion.complete(null);
        });

        ContainerMetadataUpdateTransaction transaction = createUpdateTransaction(metadata);
        StorageMetadataCheckpointOperation checkpoint = createStorageMetadataCheckpoint();

        // If successful this operation should not throw a NullPointerException.
        transaction.preProcessOperation(checkpoint);
    }

    /**
     * Tests that a Transient Segment may only have {@link SegmentMetadataUpdateTransaction#TRANSIENT_ATTRIBUTE_LIMIT}
     * or fewer Extended Attributes.
     */
    @Test
    public void testTransientSegmentExtendedAttributeLimit() throws ContainerException, StreamSegmentException {
       // Create base metadata with one Transient Segment.
       UpdateableContainerMetadata metadata = createMetadataTransient();
       int expected = 1;
        Assert.assertEquals("Unexpected initial Active Segment Count for base metadata.",
                expected, metadata.getActiveSegmentCount());

        // Create an UpdateTransaction containing updates for Extended Attributes on the Transient Segment -- it should succeed.
        val txn1 = new ContainerMetadataUpdateTransaction(metadata, metadata, 0);
        Assert.assertEquals("Unexpected Active Segment Count for first transaction.",
                expected, txn1.getActiveSegmentCount());
        // Subtract one from remaining due to Segment Type Attribute.
        long id = SegmentMetadataUpdateTransaction.TRANSIENT_ATTRIBUTE_LIMIT - (TRANSIENT_ATTRIBUTE_REMAINING - 1);
        AttributeUpdateCollection attributes = AttributeUpdateCollection.from(
                // The new entry.
                new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, id), AttributeUpdateType.None, id),
                // Update two old entries.
                new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, 0), AttributeUpdateType.Replace, (long) 1),
                new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, 1), AttributeUpdateType.Replace, (long) 2)
        );
        val map1 = createTransientAppend(TRANSIENT_SEGMENT_ID, attributes);
        txn1.preProcessOperation(map1);
        map1.setSequenceNumber(metadata.nextOperationSequenceNumber());
        txn1.acceptOperation(map1);

        int expectedExtendedAttributes = SegmentMetadataUpdateTransaction.TRANSIENT_ATTRIBUTE_LIMIT - (TRANSIENT_ATTRIBUTE_REMAINING - 1);
        SegmentMetadata segmentMetadata = txn1.getStreamSegmentMetadata(TRANSIENT_SEGMENT_ID);
        Assert.assertEquals("Unexpected Extended Attribute count after first transaction.",
                expectedExtendedAttributes, segmentMetadata.getAttributes().size());

        val txn2 = new ContainerMetadataUpdateTransaction(txn1, metadata, 1);
        // Add two new Extended Attributes which should exceed the set limit.
        attributes = AttributeUpdateCollection.from(
                new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, ++id), AttributeUpdateType.None, (long) 0),
                new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, ++id ), AttributeUpdateType.None, (long) 0)
        );
        // Should not fail as there was space before the operation, so accept any new additions.
        val map2 = createTransientAppend(TRANSIENT_SEGMENT_ID, attributes);
        txn2.preProcessOperation(map2);
        txn2.acceptOperation(map2);
        // Since we are now over the limit, enforce that no new Extended Attributes may be added.
        val txn3 = new ContainerMetadataUpdateTransaction(txn2, metadata, 2);
        // Expect another Attribute addition to fail as the limit has been exceeded.
        val map3 = createTransientAppend(TRANSIENT_SEGMENT_ID, AttributeUpdateCollection.from(
            new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, ++id), AttributeUpdateType.None, (long) 0)
        ));
        AssertExtensions.assertThrows(
                "Exception was not thrown when too many Extended Attributes were registered.",
                () -> txn3.preProcessOperation(map3),
                ex -> ex instanceof MetadataUpdateException
        );
    }

    //endregion

    //region Helpers

    private UpdateableContainerMetadata createBlankMetadata() {
        return new MetadataBuilder(CONTAINER_ID).build();
    }

    private UpdateableContainerMetadata populateMetadata(UpdateableContainerMetadata metadata) {
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        segmentMetadata.setLength(SEGMENT_LENGTH);
        segmentMetadata.setStorageLength(SEGMENT_LENGTH - 1); // Different from Length.
        segmentMetadata.refreshDerivedProperties();

        segmentMetadata = metadata.mapStreamSegmentId(SEALED_SOURCE_NAME, SEALED_SOURCE_ID);
        segmentMetadata.setLength(SEALED_SOURCE_LENGTH);
        segmentMetadata.setStorageLength(SEALED_SOURCE_LENGTH);
        segmentMetadata.markSealed();
        segmentMetadata.refreshDerivedProperties();

        segmentMetadata = metadata.mapStreamSegmentId(NOTSEALED_SOURCE_NAME, NOTSEALED_SOURCE_ID);
        segmentMetadata.setLength(0);
        segmentMetadata.setStorageLength(0);
        segmentMetadata.refreshDerivedProperties();

        return metadata;
    }

    private UpdateableContainerMetadata createMetadataTransient() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId(TRANSIENT_SEGMENT_NAME, TRANSIENT_SEGMENT_ID);
        segmentMetadata.setLength(SEGMENT_LENGTH);
        segmentMetadata.setStorageLength(SEGMENT_LENGTH - 1);

        Map<AttributeId, Long> attributes = new HashMap<>();
        attributes.put(Attributes.ATTRIBUTE_SEGMENT_TYPE, SegmentType.TRANSIENT_SEGMENT.getValue());
        for (long i = 0; i < SegmentMetadataUpdateTransaction.TRANSIENT_ATTRIBUTE_LIMIT - (TRANSIENT_ATTRIBUTE_REMAINING + 1); i++) {
            attributes.put(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX + 1, i), i);
        }
        segmentMetadata.updateAttributes(attributes);
        segmentMetadata.refreshDerivedProperties();

        return metadata;
    }

    private UpdateableContainerMetadata createMetadata() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        populateMetadata(metadata);
        return metadata;
    }

    private ContainerMetadataUpdateTransaction createUpdateTransaction(ContainerMetadata metadata) {
        return new ContainerMetadataUpdateTransaction(metadata, metadata, 0);
    }

    private StreamSegmentAppendOperation createAppendNoOffset() {
        return new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, createAttributeUpdates());
    }

    private StreamSegmentAppendOperation createAppendWithOffset(long offset) {
        return new StreamSegmentAppendOperation(SEGMENT_ID, offset, DEFAULT_APPEND_DATA, createAttributeUpdates());
    }

    private AttributeUpdateCollection createAttributeUpdates() {
        return Arrays.stream(ATTRIBUTE_UPDATE_TYPES)
                     .map(ut -> new AttributeUpdate(AttributeId.randomUUID(), ut, NEXT_ATTRIBUTE_VALUE.get()))
                     .collect(Collectors.toCollection(AttributeUpdateCollection::new));
    }

    private Map<AttributeId, Long> createAttributes() {
        return Arrays.stream(ATTRIBUTE_UPDATE_TYPES)
                     .collect(Collectors.toMap(a -> AttributeId.randomUUID(), a -> NEXT_ATTRIBUTE_VALUE.get()));
    }

    private StreamSegmentSealOperation createSeal() {
        return new StreamSegmentSealOperation(SEGMENT_ID);
    }

    private StreamSegmentTruncateOperation createTruncate(long offset) {
        return new StreamSegmentTruncateOperation(SEGMENT_ID, offset);
    }

    private DeleteSegmentOperation createDelete() {
        return new DeleteSegmentOperation(SEGMENT_ID);
    }

    private MergeSegmentOperation createMerge() {
        return new MergeSegmentOperation(SEGMENT_ID, SEALED_SOURCE_ID);
    }

    private StreamSegmentMapOperation createMap() {
        return createMap(SEGMENT_NAME);
    }

    private StreamSegmentMapOperation createMap(String name) {
        return new StreamSegmentMapOperation(StreamSegmentInformation.builder()
                .name(name)
                .length(SEGMENT_LENGTH)
                .startOffset(SEGMENT_LENGTH / 2)
                .sealed(true)
                .attributes(createAttributes())
                .build());
    }

    private StreamSegmentMapOperation createTransactionMap(String name) {
        return new StreamSegmentMapOperation(
                StreamSegmentInformation.builder()
                                        .name(name)
                                        .length(SEALED_SOURCE_LENGTH)
                                        .sealed(true)
                                        .attributes(createAttributes())
                                        .build());
    }

    private StreamSegmentAppendOperation createTransientAppend(long id,  AttributeUpdateCollection attributes) {
        return new StreamSegmentAppendOperation(id,  new ByteArraySegment(new byte[10]), attributes);
    }

    private MetadataCheckpointOperation createMetadataCheckpoint() {
        return new MetadataCheckpointOperation();
    }

    private StorageMetadataCheckpointOperation createStorageMetadataCheckpoint() {
        return new StorageMetadataCheckpointOperation();
    }

    private <T extends CheckpointOperationBase> T createCheckpoint(Supplier<T> constructor, ByteArraySegment contents, long sequenceNumber) {
        val op = constructor.get();
        op.setSequenceNumber(sequenceNumber);
        op.setContents(contents);
        return op;
    }

    private void processOperation(Operation operation, ContainerMetadataUpdateTransaction txn, Supplier<Long> getSeqNo) throws Exception {
        txn.preProcessOperation(operation);
        if (operation.getSequenceNumber() < 0) {
            operation.setSequenceNumber(getSeqNo.get());
        }

        txn.acceptOperation(operation);
    }

    private ByteArraySegment processCheckpointOperation(CheckpointOperationBase operation, ContainerMetadataUpdateTransaction txn, Supplier<Long> getSeqNo) throws Exception {
        txn.preProcessOperation(operation);
        if (operation.getSequenceNumber() < 0) {
            operation.setSequenceNumber(getSeqNo.get());
        }

        ByteArraySegment contents = operation.getContents();
        txn.acceptOperation(operation);
        return contents;
    }

    private UpdateableContainerMetadata getCheckpointedMetadata(ByteArraySegment metadataCheckpointContents) throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn = createUpdateTransaction(metadata);
        val operation = new MetadataCheckpointOperation();
        operation.setSequenceNumber(1L);
        operation.setContents(metadataCheckpointContents);
        processOperation(operation, txn, () -> 1L);
        txn.commit(metadata);
        return metadata;
    }

    private void checkNoSequenceNumberAssigned(Operation op, String context) {
        AssertExtensions.assertLessThan(
                "Unexpected Sequence Number after " + context + ".",
                0,
                op.getSequenceNumber());
    }

    /**
     * Verify that the given ContainerMetadata objects contain the same data.
     */
    static void assertMetadataSame(String message, ContainerMetadata expected, ContainerMetadata actual) {
        Assert.assertEquals("Unexpected ContainerId.", expected.getContainerId(), actual.getContainerId());
        Collection<Long> expectedSegmentIds = expected.getAllStreamSegmentIds();
        Collection<Long> actualSegmentIds = actual.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements(message + " Unexpected Segments mapped.", expectedSegmentIds, actualSegmentIds);
        for (long streamSegmentId : expectedSegmentIds) {
            SegmentMetadata expectedSegmentMetadata = expected.getStreamSegmentMetadata(streamSegmentId);
            SegmentMetadata actualSegmentMetadata = actual.getStreamSegmentMetadata(streamSegmentId);
            Assert.assertNotNull(message + " No metadata for Segment " + streamSegmentId, actualSegmentMetadata);
            SegmentMetadataComparer.assertEquals(message, expectedSegmentMetadata, actualSegmentMetadata);
        }
    }

    private void verifyAttributeUpdates(String stepName, ContainerMetadata containerMetadata, Collection<AttributeUpdate> attributeUpdates, Map<AttributeId, Long> expectedValues) {
        // Verify that the Attribute Updates have their expected values and that the updater has internalized the attribute updates.
        val transactionMetadata = containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        val expectedTransactionAttributes = new HashMap<>(expectedValues);
        attributeUpdates.forEach(au -> expectedTransactionAttributes.put(au.getAttributeId(), au.getValue()));
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in transaction metadata " + stepName + ".",
                expectedTransactionAttributes, transactionMetadata);
        for (AttributeUpdate au : attributeUpdates) {
            Assert.assertEquals("Unexpected updated value for [" + au + "] " + stepName,
                    (long) expectedValues.get(au.getAttributeId()), au.getValue());
        }
    }

    private void checkLastKnownSequenceNumber(String message, long expectedLastUsed, SegmentMetadata metadata) {
        Assert.assertTrue("Unexpected type of metadata.", metadata instanceof StreamSegmentMetadata);
        Assert.assertEquals(message, expectedLastUsed, metadata.getLastUsed());
    }

    //endregion

    private static class InstrumentedContainerMetadata extends StreamSegmentContainerMetadata {

        // This future is provided by the caller to ensure that the getStreamSegmentMetadata call will not make progress
        // until the caller performs its intended duties.
        CompletableFuture<Void> deletion;

        // This future is made visible to allow the caller to ensure that it does not make progress until the
        // getAllStreamSegmentIds call is complete.
        final CompletableFuture<Void> getAllStreamSegmentIdsFuture = new CompletableFuture<>();

        /**
         * Creates a new instance of the StreamSegmentContainerMetadata.
         *
         * @param streamSegmentContainerId The ID of the StreamSegmentContainer.
         * @param maxActiveSegmentCount    The maximum number of segments that can be registered in this metadata at any given time.
         */
        public InstrumentedContainerMetadata(int streamSegmentContainerId, int maxActiveSegmentCount, CompletableFuture<Void> deletion) {
            super(streamSegmentContainerId, maxActiveSegmentCount);
            this.deletion = deletion;
        }

        public CompletableFuture<Void> getGetAllStreamSegmentIdsFuture() {
            return this.getAllStreamSegmentIdsFuture;
        }

        @Override
        public Collection<Long> getAllStreamSegmentIds() {
            Collection<Long> ids = super.getAllStreamSegmentIds();
            // Complete the future to signal this event has happened.
            getAllStreamSegmentIdsFuture.complete(null);

            return ids;
        }

        @Override
        public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
            // Wait for the deletion event to happen.
            return deletion.thenApply(empty -> super.getStreamSegmentMetadata(streamSegmentId)).join();
        }

    }

}
