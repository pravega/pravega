/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.collect.Iterators;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.segment.StreamSegmentNameUtils;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.InMemoryStateStore;
import io.pravega.segmentstore.server.logs.operations.ProbeOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMapper;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;

/**
 * Base class for all Operation Log-based classes (i.e., DurableLog and OperationProcessor).
 */
abstract class OperationLogTestBase extends ThreadPooledTestSuite {
    protected static final int TEST_TIMEOUT_MILLIS = 30000;
    protected static final Duration TIMEOUT = Duration.ofMillis(TEST_TIMEOUT_MILLIS);
    private static final Supplier<CompletableFuture<Void>> NO_OP_METADATA_CLEANUP = () -> CompletableFuture.completedFuture(null);
    private static final int MAX_SEGMENT_COUNT = 1000 * 1000;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    //region Creating Segments

    /**
     * Updates the given Container Metadata to have a number of StreamSegments. All created StreamSegments will have
     * Ids from 0 to the value of streamSegmentCount.
     */
    HashSet<Long> createStreamSegmentsInMetadata(int streamSegmentCount, UpdateableContainerMetadata containerMetadata) {
        assert streamSegmentCount <= MAX_SEGMENT_COUNT : "cannot have more than " + MAX_SEGMENT_COUNT + " StreamSegments for this test.";
        HashSet<Long> result = new HashSet<>();
        for (long streamSegmentId = 0; streamSegmentId < streamSegmentCount; streamSegmentId++) {
            result.add(streamSegmentId);
            String name = getStreamSegmentName(streamSegmentId);
            UpdateableSegmentMetadata segmentMetadata = containerMetadata.mapStreamSegmentId(name, streamSegmentId);
            segmentMetadata.setDurableLogLength(0);
            segmentMetadata.setStorageLength(0);
        }

        return result;
    }

    /**
     * Creates a number of StreamSegments in the given Metadata and OperationLog.
     */
    HashSet<Long> createStreamSegmentsWithOperations(int streamSegmentCount, ContainerMetadata containerMetadata, OperationLog durableLog, Storage storage) {
        StreamSegmentMapper mapper = new StreamSegmentMapper(containerMetadata, durableLog, new InMemoryStateStore(), NO_OP_METADATA_CLEANUP,
                storage, ForkJoinPool.commonPool());
        HashSet<Long> result = new HashSet<>();
        for (int i = 0; i < streamSegmentCount; i++) {
            String name = getStreamSegmentName(i);
            long streamSegmentId = mapper
                    .createNewStreamSegment(name, null, Duration.ZERO)
                    .thenCompose((v) -> mapper.getOrAssignStreamSegmentId(name, Duration.ZERO)).join();
            result.add(streamSegmentId);
        }

        return result;
    }

    /**
     * Updates the given Container Metadata to have a number of Transactions mapped to the given StreamSegment Ids.
     */
    AbstractMap<Long, Long> createTransactionsInMetadata(HashSet<Long> streamSegmentIds, int transactionsPerStreamSegment, UpdateableContainerMetadata containerMetadata) {
        assert transactionsPerStreamSegment <= MAX_SEGMENT_COUNT : "cannot have more than " + MAX_SEGMENT_COUNT + " Transactions per StreamSegment for this test.";
        HashMap<Long, Long> result = new HashMap<>();
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < transactionsPerStreamSegment; i++) {
                long transactionId = getTransactionId(streamSegmentId, i);
                assert result.put(transactionId, streamSegmentId) == null : "duplicate TransactionId generated: " + transactionId;
                assert !streamSegmentIds.contains(transactionId) : "duplicate StreamSegmentId (Transaction) generated: " + transactionId;
                String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(streamSegmentName, UUID.randomUUID());
                UpdateableSegmentMetadata transactionMetadata = containerMetadata.mapStreamSegmentId(transactionName, transactionId, streamSegmentId);
                transactionMetadata.setDurableLogLength(0);
                transactionMetadata.setStorageLength(0);
            }
        }

        return result;
    }

    /**
     * Creates a number of Transaction Segments in the given Metadata and OperationLog.
     */
    AbstractMap<Long, Long> createTransactionsWithOperations(HashSet<Long> streamSegmentIds, int transactionsPerStreamSegment,
                                                             ContainerMetadata containerMetadata, OperationLog durableLog, Storage storage) {
        HashMap<Long, Long> result = new HashMap<>();
        StreamSegmentMapper mapper = new StreamSegmentMapper(containerMetadata, durableLog, new InMemoryStateStore(), NO_OP_METADATA_CLEANUP,
                storage, ForkJoinPool.commonPool());
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < transactionsPerStreamSegment; i++) {
                long transactionId = mapper
                        .createNewTransactionStreamSegment(streamSegmentName, UUID.randomUUID(), null, Duration.ZERO)
                        .thenCompose(v -> mapper.getOrAssignStreamSegmentId(v, Duration.ZERO)).join();
                result.put(transactionId, streamSegmentId);
            }
        }

        return result;
    }

    private String getStreamSegmentName(long streamSegmentId) {
        return String.format("StreamSegment_%d", streamSegmentId);
    }

    private long getTransactionId(long streamSegmentId, int transactionId) {
        return (streamSegmentId + 1) * MAX_SEGMENT_COUNT + transactionId;
    }

    //endregion

    //region Operation Generation

    /**
     * Generates a List of Log Operations that contains the following operations, in the "correct" order.
     * <ol>
     * <li> A set of StreamSegmentAppend Operations (based on the streamSegmentIds arg).
     * <li> A set of StreamSegmentSeal and MergeTransaction Operations (based on the TransactionIds and mergeTransactions arg).
     * <li> A set of StreamSegmentSeal Operations (based on the sealStreamSegments arg).
     * </ol>
     */
    List<Operation> generateOperations(Collection<Long> streamSegmentIds, Map<Long, Long> transactionIds, int appendsPerStreamSegment, int metadataCheckpointsEvery, boolean mergeTransactions, boolean sealStreamSegments) {
        List<Operation> result = new ArrayList<>();

        // Add some appends.
        int appendId = 0;
        for (long streamSegmentId : streamSegmentIds) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                val attributes = Collections.singletonList(new AttributeUpdate(UUID.randomUUID(), AttributeUpdateType.Replace, i));
                result.add(new StreamSegmentAppendOperation(streamSegmentId, generateAppendData(appendId), attributes));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        addProbe(result);

        for (long transactionId : transactionIds.keySet()) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                val attributes = Collections.singletonList(new AttributeUpdate(UUID.randomUUID(), AttributeUpdateType.Replace, i));
                result.add(new StreamSegmentAppendOperation(transactionId, generateAppendData(appendId), attributes));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        addProbe(result);

        // Merge Transactions.
        if (mergeTransactions) {
            // Key = TransactionId, Value = Parent Id.
            transactionIds.entrySet().forEach(mapping -> {
                result.add(new StreamSegmentSealOperation(mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                result.add(new MergeTransactionOperation(mapping.getValue(), mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
            addProbe(result);
        }

        // Seal the StreamSegments.
        if (sealStreamSegments) {
            streamSegmentIds.forEach(streamSegmentId -> {
                result.add(new StreamSegmentSealOperation(streamSegmentId));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
            addProbe(result);
        }

        return result;
    }

    private byte[] generateAppendData(int appendId) {
        return String.format("Append_%d", appendId).getBytes();
    }

    private void addCheckpointIfNeeded(Collection<Operation> operations, int metadataCheckpointsEvery) {
        if (metadataCheckpointsEvery > 0 && operations.size() % metadataCheckpointsEvery == 0) {
            operations.add(new MetadataCheckpointOperation());
        }
    }

    private void addProbe(Collection<Operation> operations) {
        operations.add(new ProbeOperation());
    }

    //endregion

    //region Verification

    void performMetadataChecks(Collection<Long> streamSegmentIds, Collection<Long> invalidStreamSegmentIds, Map<Long, Long> transactions, Collection<OperationWithCompletion> operations, ContainerMetadata metadata, boolean expectTransactionsMerged, boolean expectSegmentsSealed) {
        // Verify that transactions are merged
        for (long transactionId : transactions.keySet()) {
            SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(transactionId);
            if (invalidStreamSegmentIds.contains(transactionId)) {
                Assert.assertTrue("Unexpected data for a Transaction that was invalid.", transactionMetadata == null || transactionMetadata.getDurableLogLength() == 0);
            } else {
                Assert.assertEquals("Unexpected Transaction seal state for Transaction " + transactionId, expectTransactionsMerged, transactionMetadata.isSealed());
                Assert.assertEquals("Unexpected Transaction merge state for Transaction " + transactionId, expectTransactionsMerged, transactionMetadata.isMerged());
            }
        }

        // Verify the end state of each stream segment (length, sealed).
        AbstractMap<Long, Integer> expectedLengths = getExpectedLengths(operations);
        for (long streamSegmentId : streamSegmentIds) {
            SegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(streamSegmentId);
            if (invalidStreamSegmentIds.contains(streamSegmentId)) {
                Assert.assertTrue("Unexpected data for a StreamSegment that was invalid.", segmentMetadata == null || segmentMetadata.getDurableLogLength() == 0);
            } else {
                Assert.assertEquals("Unexpected seal state for StreamSegment " + streamSegmentId, expectSegmentsSealed, segmentMetadata.isSealed());
                Assert.assertEquals("Unexpected length for StreamSegment " + streamSegmentId, (int) expectedLengths.getOrDefault(streamSegmentId, 0), segmentMetadata.getDurableLogLength());
            }
        }
    }

    void performReadIndexChecks(Collection<OperationWithCompletion> operations, ReadIndex readIndex) throws Exception {
        AbstractMap<Long, Integer> expectedLengths = getExpectedLengths(operations);
        AbstractMap<Long, InputStream> expectedData = getExpectedContents(operations);
        for (Map.Entry<Long, InputStream> e : expectedData.entrySet()) {
            int expectedLength = expectedLengths.getOrDefault(e.getKey(), -1);
            @Cleanup
            ReadResult readResult = readIndex.read(e.getKey(), 0, expectedLength, TIMEOUT);
            int readLength = 0;
            while (readResult.hasNext()) {
                ReadResultEntryContents entry = readResult.next().getContent().join();
                int length = entry.getLength();
                readLength += length;
                int streamSegmentOffset = expectedLengths.getOrDefault(e.getKey(), 0);
                expectedLengths.put(e.getKey(), streamSegmentOffset + length);
                AssertExtensions.assertStreamEquals(String.format("Unexpected data returned from ReadIndex. StreamSegmentId = %d, Offset = %d.", e.getKey(), streamSegmentOffset), e.getValue(), entry.getData(), length);
            }

            Assert.assertEquals("Not enough bytes were read from the ReadIndex for StreamSegment " + e.getKey(), expectedLength, readLength);
        }
    }

    /**
     * Given a list of LogOperations, calculates the final lengths of the StreamSegments that are encountered, by inspecting
     * every StreamSegmentAppendOperation and MergeTransactionOperation. All other types of Log Operations are ignored.
     */
    private AbstractMap<Long, Integer> getExpectedLengths(Collection<OperationWithCompletion> operations) {
        HashMap<Long, Integer> result = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                result.put(
                        appendOperation.getStreamSegmentId(),
                        result.getOrDefault(appendOperation.getStreamSegmentId(), 0) + appendOperation.getData().length);
            } else if (o.operation instanceof MergeTransactionOperation) {
                MergeTransactionOperation mergeOperation = (MergeTransactionOperation) o.operation;

                result.put(
                        mergeOperation.getStreamSegmentId(),
                        result.getOrDefault(mergeOperation.getStreamSegmentId(), 0) + result.getOrDefault(mergeOperation.getTransactionSegmentId(), 0));
                result.remove(mergeOperation.getTransactionSegmentId());
            }
        }

        return result;
    }

    /**
     * Given a list of Log Operations, generates an InputStream for each encountered StreamSegment that contains the final
     * contents of that StreamSegment. Only considers operations of type StreamSegmentAppendOperation and MergeTransactionOperation.
     */
    private AbstractMap<Long, InputStream> getExpectedContents(Collection<OperationWithCompletion> operations) {
        HashMap<Long, List<ByteArrayInputStream>> partialContents = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                List<ByteArrayInputStream> segmentContents = partialContents.get(appendOperation.getStreamSegmentId());
                if (segmentContents == null) {
                    segmentContents = new ArrayList<>();
                    partialContents.put(appendOperation.getStreamSegmentId(), segmentContents);
                }

                segmentContents.add(new ByteArrayInputStream(appendOperation.getData()));
            } else if (o.operation instanceof MergeTransactionOperation) {
                MergeTransactionOperation mergeOperation = (MergeTransactionOperation) o.operation;
                List<ByteArrayInputStream> targetSegmentContents = partialContents.get(mergeOperation.getStreamSegmentId());
                if (targetSegmentContents == null) {
                    targetSegmentContents = new ArrayList<>();
                    partialContents.put(mergeOperation.getStreamSegmentId(), targetSegmentContents);
                }

                List<ByteArrayInputStream> sourceSegmentContents = partialContents.get(mergeOperation.getTransactionSegmentId());
                targetSegmentContents.addAll(sourceSegmentContents);
                partialContents.remove(mergeOperation.getTransactionSegmentId());
            }
        }

        // Construct final result.
        HashMap<Long, InputStream> result = new HashMap<>();
        for (Map.Entry<Long, List<ByteArrayInputStream>> e : partialContents.entrySet()) {
            result.put(e.getKey(), new SequenceInputStream(Iterators.asEnumeration(e.getValue().iterator())));
        }

        return result;
    }

    //endregion

    //region FailedStreamSegmentAppendOperation

    static class FailedStreamSegmentAppendOperation extends StreamSegmentAppendOperation {
        private final boolean failAtBeginning;

        FailedStreamSegmentAppendOperation(StreamSegmentAppendOperation base, boolean failAtBeginning) {
            super(base.getStreamSegmentId(), base.getData(), base.getAttributeUpdates());
            this.failAtBeginning = failAtBeginning;
        }

        @Override
        protected void serializeContent(DataOutputStream target) throws IOException {
            if (!this.failAtBeginning) {
                super.serializeContent(target);
            }

            throw new IOException("intentional failure");
        }
    }

    //endregion

    // region CorruptedMemoryOperationLog

    @RequiredArgsConstructor
    static class CorruptedMemoryOperationLog extends SequencedItemList<Operation> {
        private final long corruptAtIndex;
        private final AtomicLong addCount = new AtomicLong();

        @Override
        public boolean add(Operation item) {
            if (this.addCount.incrementAndGet() == this.corruptAtIndex) {
                // Still add the item, but report that we haven't added it.
                return false;
            }

            return super.add(item);
        }
    }

    //endregion

    //region OperationWithCompletion

    @RequiredArgsConstructor
    static class OperationWithCompletion {
        final Operation operation;
        final CompletableFuture<Long> completion;

        @Override
        public String toString() {
            return String.format(
                    "(%s) %s",
                    this.completion.isDone() ? (this.completion.isCompletedExceptionally() ? "Error" : "Complete") : "Not Completed",
                    this.operation);
        }

        static CompletableFuture<Void> allOf(Collection<OperationWithCompletion> operations) {
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            operations.forEach(oc -> futures.add(oc.completion));
            return FutureHelpers.allOf(futures);
        }
    }

    //endregion
}
