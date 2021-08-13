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

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.storage.LogAddress;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for OperationMetadataUpdater class.
 */
public class OperationMetadataUpdaterTests {
    private static final int TRANSACTION_COUNT = 100;
    private static final int CONTAINER_ID = 1;
    private static final int MAX_ACTIVE_SEGMENT_COUNT = TRANSACTION_COUNT * 100;
    private static final Supplier<Long> NEXT_ATTRIBUTE_VALUE = System::nanoTime;
    private static final AttributeId PARENT_ID = AttributeId.uuid(1234, 1234);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);
    private final Supplier<Integer> nextAppendLength = () -> Math.max(1, (int) System.nanoTime() % 1000);

    /**
     * Tests the basic functionality of the class, when no UpdateTransactions are explicitly created. Operations tested:
     * * StreamSegmentMapOperation
     * * StreamSegmentAppendOperation
     * * StreamSegmentSealOperation
     * * MergeSegmentOperation
     */
    @Test
    public void testSingleTransaction() throws Exception {
        final int segmentCount = TRANSACTION_COUNT;
        final int transactionsPerSegment = 5;
        final int appendsPerSegment = 10;

        val referenceMetadata = createBlankMetadata();
        val metadata = createBlankMetadata();
        val updater = new OperationMetadataUpdater(metadata);
        val truncationMarkers = new HashMap<Long, LogAddress>();
        for (int i = 0; i < segmentCount; i++) {
            long segmentId = mapSegment(updater, referenceMetadata);
            recordAppends(segmentId, appendsPerSegment, updater, referenceMetadata);

            for (int j = 0; j < transactionsPerSegment; j++) {
                long transactionId = mapTransaction(segmentId, updater, referenceMetadata);
                recordAppends(transactionId, appendsPerSegment, updater, referenceMetadata);
                sealSegment(transactionId, updater, referenceMetadata);
                mergeTransaction(transactionId, updater, referenceMetadata);
            }

            sealSegment(segmentId, updater, referenceMetadata);
            val logAddress = new TestLogAddress(metadata.getOperationSequenceNumber());
            Assert.assertNull("OperationSequenceNumber did not change.", truncationMarkers.put(logAddress.getSequence(), logAddress));
        }

        val blankMetadata = createBlankMetadata();
        ContainerMetadataUpdateTransactionTests.assertMetadataSame("Before commit", blankMetadata, metadata);
        updater.commitAll();

        // Refresh the source metadata (after the commit) to reflect that the ATTRIBUTE_SEGMENT_TYPE may have changed.
        referenceMetadata.getAllStreamSegmentIds().stream().map(referenceMetadata::getStreamSegmentMetadata).forEach(UpdateableSegmentMetadata::refreshDerivedProperties);
        ContainerMetadataUpdateTransactionTests.assertMetadataSame("After commit", referenceMetadata, metadata);
    }

    /**
     * Tests the handling of sealing (and thus creating) empty UpdateTransactions.
     */
    @Test
    public void testSealEmpty() {
        val metadata = createBlankMetadata();
        val updater = new OperationMetadataUpdater(metadata);
        val txn1 = updater.sealTransaction();
        Assert.assertEquals("Unexpected transaction id for first empty transaction.", 0, txn1);
        val txn2 = updater.sealTransaction();
        Assert.assertEquals("Unexpected transaction id for second empty transaction.", 1, txn2);
    }

    /**
     * Tests the ability to successively commit update transactions to the base metadata.
     */
    @Test
    public void testCommit() throws Exception {
        // Commit 1 at a time, then 2, then 3, then 4, etc until we have nothing left to commit.
        // At each step verify that the base metadata has been properly updated.
        val referenceMetadata = createBlankMetadata();
        val metadata = createBlankMetadata();
        val updater = new OperationMetadataUpdater(metadata);
        val lastSegmentId = new AtomicLong(-1);
        val lastSegmentTxnId = new AtomicLong(-1);
        long lastCommittedTxnId = -1;
        int txnGroupSize = 1;

        val updateTransactions = new ArrayList<Map.Entry<Long, ContainerMetadata>>();
        while (updateTransactions.size() < TRANSACTION_COUNT) {
            populateUpdateTransaction(updater, referenceMetadata, lastSegmentId, lastSegmentTxnId);

            long utId = updater.sealTransaction();
            if (updateTransactions.size() > 0) {
                long prevUtId = updateTransactions.get(updateTransactions.size() - 1).getKey();
                Assert.assertEquals("UpdateTransaction.Id is not sequential and increasing.", prevUtId + 1, utId);
            }

            updateTransactions.add(new AbstractMap.SimpleImmutableEntry<>(utId, clone(referenceMetadata)));
        }

        ContainerMetadata previousMetadata = null;
        for (val t : updateTransactions) {
            val utId = t.getKey();
            val expectedMetadata = t.getValue();

            // Check to see if it's time to commit.
            if (utId - lastCommittedTxnId >= txnGroupSize) {
                if (previousMetadata != null) {
                    // Verify no changes to the metadata prior to commit.
                    ContainerMetadataUpdateTransactionTests.assertMetadataSame("Before commit " + utId, previousMetadata, metadata);
                }

                // Commit and verify.
                updater.commit(utId);
                ContainerMetadataUpdateTransactionTests.assertMetadataSame("After commit " + utId, expectedMetadata, metadata);
                lastCommittedTxnId = utId;
                txnGroupSize++;
                previousMetadata = expectedMetadata;
            }
        }
    }

    /**
     * Tests the ability to rollback update transactions.
     */
    @Test
    public void testRollback() throws Exception {
        // 2 out of 3 UpdateTransactions are failed (to verify multi-failure).
        // Commit the rest and verify final metadata is as it should.
        final int failEvery = 3;
        Predicate<Integer> isIgnored = index -> index % failEvery > 0;
        Predicate<Integer> shouldFail = index -> index % failEvery == failEvery - 1;

        val referenceMetadata = createBlankMetadata();
        val metadata = createBlankMetadata();
        val updater = new OperationMetadataUpdater(metadata);
        val lastSegmentId = new AtomicLong(-1);
        val lastSegmentTxnId = new AtomicLong(-1);

        val updateTransactions = new ArrayList<Map.Entry<Long, ContainerMetadata>>();
        for (int i = 0; i < TRANSACTION_COUNT; i++) {
            // Check to see if this UpdateTransaction is going to end up being rolled back. If so, we should not update
            // the reference metadata at all.
            UpdateableContainerMetadata txnReferenceMetadata = isIgnored.test(i) ? null : referenceMetadata;
            populateUpdateTransaction(updater, txnReferenceMetadata, lastSegmentId, lastSegmentTxnId);

            if (shouldFail.test(i)) {
                long prevUtId = updateTransactions.get(updateTransactions.size() - 1).getKey();
                updater.rollback(prevUtId + 1);
            } else if (txnReferenceMetadata != null) {
                // Not failing and not ignored: this UpdateTransaction will survive, so record it.
                long utId = updater.sealTransaction();
                if (updateTransactions.size() > 0) {
                    long prevUtId = updateTransactions.get(updateTransactions.size() - 1).getKey();
                    Assert.assertEquals("Unexpected UpdateTransaction.Id.",
                            prevUtId + failEvery - 1, utId);
                }

                updateTransactions.add(new AbstractMap.SimpleImmutableEntry<>(utId, clone(txnReferenceMetadata)));
            }
        }

        ContainerMetadata previousMetadata = null;
        for (val t : updateTransactions) {
            val utId = t.getKey();
            val expectedMetadata = t.getValue();

            // Check to see if it's time to commit.
            if (previousMetadata != null) {
                // Verify no changes to the metadata prior to commit.
                ContainerMetadataUpdateTransactionTests.assertMetadataSame("Before commit " + utId, previousMetadata, metadata);
            }

            // Commit and verify.
            updater.commit(utId);
            ContainerMetadataUpdateTransactionTests.assertMetadataSame("After commit " + utId, expectedMetadata, metadata);
            previousMetadata = expectedMetadata;
        }
    }

    /**
     * Tests a mixed scenario where we commit one UpdateTransaction and then rollback the next one, one after another.
     * testRollback() verifies a bunch of rollbacks and then commits in sequence; this test alternates one with the other.
     */
    @Test
    public void testCommitRollbackAlternate() throws Exception {
        Predicate<Integer> shouldFail = index -> index % 2 == 1;

        val referenceMetadata = createBlankMetadata();
        val metadata = createBlankMetadata();
        val updater = new OperationMetadataUpdater(metadata);
        val lastSegmentId = new AtomicLong(-1);
        val lastSegmentTxnId = new AtomicLong(-1);

        for (int i = 0; i < TRANSACTION_COUNT; i++) {
            // Check to see if this UpdateTransaction is going to end up being rolled back. If so, we should not update
            // the reference metadata at all.
            UpdateableContainerMetadata txnReferenceMetadata = shouldFail.test(i) ? null : referenceMetadata;
            populateUpdateTransaction(updater, txnReferenceMetadata, lastSegmentId, lastSegmentTxnId);

            if (shouldFail.test(i)) {
                updater.rollback(0);
                ContainerMetadataUpdateTransactionTests.assertMetadataSame("After rollback " + i, referenceMetadata, metadata);
            } else {
                updater.commitAll();

                // Refresh the source metadata (after the commit) to reflect that the ATTRIBUTE_SEGMENT_TYPE may have changed.
                referenceMetadata.getAllStreamSegmentIds().stream().map(referenceMetadata::getStreamSegmentMetadata).forEach(UpdateableSegmentMetadata::refreshDerivedProperties);
                ContainerMetadataUpdateTransactionTests.assertMetadataSame("After commit " + i, referenceMetadata, metadata);
            }
        }
    }

    private void populateUpdateTransaction(OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata,
                                           AtomicLong lastSegmentId, AtomicLong lastSegmentTxnId) throws Exception {
        // Create a segment
        long segmentId = mapSegment(updater, referenceMetadata);

        // Make an append (to each segment known so far.)
        for (long sId : updater.getAllStreamSegmentIds()) {
            val rsm = updater.getStreamSegmentMetadata(sId);
            if (!rsm.isMerged() && !rsm.isSealed()) {
                recordAppend(sId, this.nextAppendLength.get(), updater, referenceMetadata);
            }
        }

        // Create a SegmentTransaction for the segment created in the previous UpdateTransaction
        long txnId = lastSegmentTxnId.get();
        if (lastSegmentId.get() >= 0) {
            txnId = mapTransaction(lastSegmentId.get(), updater, referenceMetadata);
        }

        if (lastSegmentTxnId.get() >= 0) {
            // Seal&Merge the transaction created in the previous UpdateTransaction
            sealSegment(lastSegmentTxnId.get(), updater, referenceMetadata);
            mergeTransaction(lastSegmentTxnId.get(), updater, referenceMetadata);
            lastSegmentTxnId.set(-1); // Txn has been merged - so it doesn't exist anymore.
        }

        if (referenceMetadata != null) {
            // Don't remember any of these changes if we're going to be tossing them away.
            lastSegmentId.set(segmentId);
            lastSegmentTxnId.set(txnId);
        }
    }

    private UpdateableContainerMetadata createBlankMetadata() {
        return new StreamSegmentContainerMetadata(CONTAINER_ID, MAX_ACTIVE_SEGMENT_COUNT);
    }

    private UpdateableContainerMetadata clone(ContainerMetadata base) {
        val metadata = createBlankMetadata();

        base.getAllStreamSegmentIds().stream()
            .map(base::getStreamSegmentMetadata)
            .forEach(bsm -> metadata.mapStreamSegmentId(bsm.getName(), bsm.getId()).copyFrom(bsm));

        return metadata;
    }

    private void mergeTransaction(long transactionId, OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata)
            throws Exception {
        long parentSegmentId = updater.getStreamSegmentMetadata(transactionId).getAttributes().get(PARENT_ID);
        val op = new MergeSegmentOperation(parentSegmentId, transactionId);
        process(op, updater);
        if (referenceMetadata != null) {
            referenceMetadata.getStreamSegmentMetadata(transactionId).markMerged();
            val rsm = referenceMetadata.getStreamSegmentMetadata(parentSegmentId);
            rsm.setLength(rsm.getLength() + op.getLength());
        }
    }

    private void sealSegment(long segmentId, OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata)
            throws Exception {
        val op = new StreamSegmentSealOperation(segmentId);
        process(op, updater);
        if (referenceMetadata != null) {
            referenceMetadata.getStreamSegmentMetadata(segmentId).markSealed();
        }
    }

    private void recordAppends(long segmentId, int count, OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata)
            throws Exception {
        for (int i = 0; i < count; i++) {
            recordAppend(segmentId, this.nextAppendLength.get(), updater, referenceMetadata);
        }
    }

    private void recordAppend(long segmentId, int length, OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata)
            throws Exception {
        byte[] data = new byte[length];
        val attributeUpdates = AttributeUpdateCollection.from(
                new AttributeUpdate(Attributes.CREATION_TIME, AttributeUpdateType.Replace, NEXT_ATTRIBUTE_VALUE.get()),
                new AttributeUpdate(Attributes.EVENT_COUNT, AttributeUpdateType.Accumulate, NEXT_ATTRIBUTE_VALUE.get()));
        val op = new StreamSegmentAppendOperation(segmentId, new ByteArraySegment(data), attributeUpdates);
        process(op, updater);
        if (referenceMetadata != null) {
            val rsm = referenceMetadata.getStreamSegmentMetadata(segmentId);
            rsm.setLength(rsm.getLength() + length);
            val attributes = new HashMap<AttributeId, Long>();
            op.getAttributeUpdates().forEach(au -> attributes.put(au.getAttributeId(), au.getValue()));
            rsm.updateAttributes(attributes);
        }
    }

    private long mapSegment(OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata) throws Exception {
        String segmentName = "Segment_" + updater.nextOperationSequenceNumber();

        val mapOp = new StreamSegmentMapOperation(StreamSegmentInformation.builder().name(segmentName).build());
        process(mapOp, updater);
        if (referenceMetadata != null) {
            val rsm = referenceMetadata.mapStreamSegmentId(segmentName, mapOp.getStreamSegmentId());
            rsm.setLength(0);
            rsm.setStorageLength(0);
        }

        return mapOp.getStreamSegmentId();
    }

    private long mapTransaction(long parentSegmentId, OperationMetadataUpdater updater, UpdateableContainerMetadata referenceMetadata) throws Exception {
        String segmentName = "Transaction_" + updater.nextOperationSequenceNumber();

        val mapOp = new StreamSegmentMapOperation(StreamSegmentInformation
                .builder()
                .name(segmentName)
                .attributes(Collections.singletonMap(PARENT_ID, parentSegmentId))
                .build());
        process(mapOp, updater);
        if (referenceMetadata != null) {
            val rsm = referenceMetadata.mapStreamSegmentId(segmentName, mapOp.getStreamSegmentId());
            rsm.setLength(0);
            rsm.setStorageLength(0);
            rsm.updateAttributes(mapOp.getAttributes());
        }

        return mapOp.getStreamSegmentId();
    }

    private void process(Operation op, OperationMetadataUpdater updater) throws Exception {
        updater.preProcessOperation(op);
        op.setSequenceNumber(updater.nextOperationSequenceNumber());
        updater.acceptOperation(op);
    }
}