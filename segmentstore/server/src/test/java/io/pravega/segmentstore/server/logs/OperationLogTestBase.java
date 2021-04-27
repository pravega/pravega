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

import com.google.common.collect.Iterators;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;

/**
 * Base class for all Operation Log-based classes (i.e., DurableLog and OperationProcessor).
 */
abstract class OperationLogTestBase extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(30000);
    private static final int MAX_SEGMENT_COUNT = 1000 * 1000;

    @Override
    protected int getThreadPoolSize() {
        return 3;
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
            segmentMetadata.setLength(0);
            segmentMetadata.setStorageLength(0);
        }

        return result;
    }

    /**
     * Creates a number of StreamSegments in the given Metadata and OperationLog.
     */
    Set<Long> createStreamSegmentsWithOperations(int streamSegmentCount, OperationLog durableLog) {
        val operations = new ArrayList<StreamSegmentMapOperation>();
        val futures = new ArrayList<CompletableFuture<Void>>();
        for (int i = 0; i < streamSegmentCount; i++) {
            val op = new StreamSegmentMapOperation(StreamSegmentInformation.builder().name(getStreamSegmentName(i)).build());
            operations.add(op);
            futures.add(durableLog.add(op, OperationPriority.Normal, TIMEOUT));
        }

        Futures.allOf(futures).join();
        return operations.stream().map(StreamSegmentMapOperation::getStreamSegmentId).collect(Collectors.toSet());
    }

    /**
     * Updates the given Container Metadata to have a number of Transactions mapped to the given StreamSegment Ids.
     */
    AbstractMap<Long, Long> createTransactionsInMetadata(HashSet<Long> streamSegmentIds, int transactionsPerStreamSegment,
                                                         UpdateableContainerMetadata containerMetadata) {
        assert transactionsPerStreamSegment <= MAX_SEGMENT_COUNT : "cannot have more than " + MAX_SEGMENT_COUNT + " Transactions per StreamSegment for this test.";
        HashMap<Long, Long> result = new HashMap<>();
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < transactionsPerStreamSegment; i++) {
                long transactionId = getTransactionId(streamSegmentId, i);
                assert result.put(transactionId, streamSegmentId) == null : "duplicate TransactionId generated: " + transactionId;
                assert !streamSegmentIds.contains(transactionId) : "duplicate StreamSegmentId (Transaction) generated: " + transactionId;
                String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, UUID.randomUUID());
                UpdateableSegmentMetadata transactionMetadata = containerMetadata.mapStreamSegmentId(transactionName, transactionId);
                transactionMetadata.setLength(0);
                transactionMetadata.setStorageLength(0);
            }
        }

        return result;
    }

    /**
     * Creates a number of Transaction Segments in the given Metadata and OperationLog.
     */
    AbstractMap<Long, Long> createTransactionsWithOperations(Set<Long> streamSegmentIds, int transactionsPerStreamSegment,
                                                             ContainerMetadata containerMetadata, OperationLog durableLog) {
        val result = new HashMap<Long, Long>();
        for (long streamSegmentId : streamSegmentIds) {
            String streamSegmentName = containerMetadata.getStreamSegmentMetadata(streamSegmentId).getName();

            for (int i = 0; i < transactionsPerStreamSegment; i++) {
                String transactionName = NameUtils.getTransactionNameFromId(streamSegmentName, UUID.randomUUID());
                StreamSegmentMapOperation op = new StreamSegmentMapOperation(StreamSegmentInformation.builder().name(transactionName).build());
                durableLog.add(op, OperationPriority.Normal, TIMEOUT).join();
                result.put(op.getStreamSegmentId(), streamSegmentId);
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
    List<Operation> generateOperations(Collection<Long> streamSegmentIds, Map<Long, Long> transactionIds, int appendsPerStreamSegment,
                                       int metadataCheckpointsEvery, boolean mergeTransactions, boolean sealStreamSegments) {
        List<Operation> result = new ArrayList<>();

        // Add some appends.
        int appendId = 0;
        for (long streamSegmentId : streamSegmentIds) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                val attributes = AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Replace, i));
                result.add(new StreamSegmentAppendOperation(streamSegmentId, generateAppendData(appendId), attributes));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        for (long transactionId : transactionIds.keySet()) {
            for (int i = 0; i < appendsPerStreamSegment; i++) {
                val attributes = AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Replace, i));
                result.add(new StreamSegmentAppendOperation(transactionId, generateAppendData(appendId), attributes));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                appendId++;
            }
        }

        // Merge Transactions.
        if (mergeTransactions) {
            // Key = Source Segment Id, Value = Target Segment Id.
            transactionIds.entrySet().forEach(mapping -> {
                result.add(new StreamSegmentSealOperation(mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
                result.add(new MergeSegmentOperation(mapping.getValue(), mapping.getKey()));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
        }

        // Seal the StreamSegments.
        if (sealStreamSegments) {
            streamSegmentIds.forEach(streamSegmentId -> {
                result.add(new StreamSegmentSealOperation(streamSegmentId));
                addCheckpointIfNeeded(result, metadataCheckpointsEvery);
            });
        }

        return result;
    }

    protected ByteArraySegment generateAppendData(int appendId) {
        return new ByteArraySegment(String.format("Append_%d", appendId).getBytes());
    }

    private void addCheckpointIfNeeded(Collection<Operation> operations, int metadataCheckpointsEvery) {
        if (metadataCheckpointsEvery > 0 && operations.size() % metadataCheckpointsEvery == 0) {
            operations.add(new MetadataCheckpointOperation());
        }
    }

    //endregion

    //region Verification

    void performMetadataChecks(Collection<Long> streamSegmentIds, Collection<Long> invalidStreamSegmentIds,
                               Map<Long, Long> transactions, Collection<OperationWithCompletion> operations,
                               ContainerMetadata metadata, boolean expectTransactionsMerged, boolean expectSegmentsSealed) {
        // Verify that transactions are merged
        for (long transactionId : transactions.keySet()) {
            SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(transactionId);
            if (invalidStreamSegmentIds.contains(transactionId)) {
                Assert.assertTrue("Unexpected data for a Transaction that was invalid.",
                        transactionMetadata == null || transactionMetadata.getLength() == 0);
            } else {
                Assert.assertEquals("Unexpected Transaction seal state for Transaction " + transactionId,
                        expectTransactionsMerged, transactionMetadata.isSealed());
                Assert.assertEquals("Unexpected Transaction merge state for Transaction " + transactionId,
                        expectTransactionsMerged, transactionMetadata.isMerged());
            }
        }

        // Verify the end state of each stream segment (length, sealed).
        AbstractMap<Long, Integer> expectedLengths = getExpectedLengths(operations);
        for (long streamSegmentId : streamSegmentIds) {
            SegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(streamSegmentId);
            if (invalidStreamSegmentIds.contains(streamSegmentId)) {
                Assert.assertTrue("Unexpected data for a StreamSegment that was invalid.",
                        segmentMetadata == null || segmentMetadata.getLength() == 0);
            } else {
                Assert.assertEquals("Unexpected seal state for StreamSegment " + streamSegmentId,
                        expectSegmentsSealed, segmentMetadata.isSealed());
                Assert.assertEquals("Unexpected length for StreamSegment " + streamSegmentId,
                        (int) expectedLengths.getOrDefault(streamSegmentId, 0), segmentMetadata.getLength());
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
                BufferView entry = readResult.next().getContent().join();
                int length = entry.getLength();
                readLength += length;
                int streamSegmentOffset = expectedLengths.getOrDefault(e.getKey(), 0);
                expectedLengths.put(e.getKey(), streamSegmentOffset + length);
                AssertExtensions.assertStreamEquals(String.format("Unexpected data returned from ReadIndex. StreamSegmentId = %d, Offset = %d.",
                        e.getKey(), streamSegmentOffset), e.getValue(), entry.getReader(), length);
            }

            Assert.assertEquals("Not enough bytes were read from the ReadIndex for StreamSegment " + e.getKey(), expectedLength, readLength);
        }
    }

    boolean isExpectedExceptionForNonDataCorruption(Throwable ex) {
        return ex instanceof IOException
                || ex instanceof DurableDataLogException
                || ex instanceof ObjectClosedException;
    }

    boolean isExpectedExceptionForDataCorruption(Throwable ex) {
        return ex instanceof DataCorruptionException
                || ex instanceof IllegalContainerStateException
                || ex instanceof ObjectClosedException
                || ex instanceof CancellationException
                || (ex instanceof IOException && (ex.getCause() instanceof DataCorruptionException || ex.getCause() instanceof IllegalContainerStateException));
    }

    /**
     * Given a list of LogOperations, calculates the final lengths of the StreamSegments that are encountered, by inspecting
     * every StreamSegmentAppendOperation and MergeSegmentOperation. All other types of Log Operations are ignored.
     */
    private AbstractMap<Long, Integer> getExpectedLengths(Collection<OperationWithCompletion> operations) {
        HashMap<Long, Integer> result = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is a failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                result.put(
                        appendOperation.getStreamSegmentId(),
                        result.getOrDefault(appendOperation.getStreamSegmentId(), 0) + appendOperation.getData().getLength());
            } else if (o.operation instanceof MergeSegmentOperation) {
                MergeSegmentOperation mergeOperation = (MergeSegmentOperation) o.operation;

                result.put(
                        mergeOperation.getStreamSegmentId(),
                        result.getOrDefault(mergeOperation.getStreamSegmentId(), 0) + result.getOrDefault(mergeOperation.getSourceSegmentId(), 0));
                result.remove(mergeOperation.getSourceSegmentId());
            }
        }

        return result;
    }

    /**
     * Given a list of Log Operations, generates an InputStream for each encountered StreamSegment that contains the final
     * contents of that StreamSegment. Only considers operations of type StreamSegmentAppendOperation and MergeSegmentOperation.
     */
    private AbstractMap<Long, InputStream> getExpectedContents(Collection<OperationWithCompletion> operations) {
        HashMap<Long, List<InputStream>> partialContents = new HashMap<>();
        for (OperationWithCompletion o : operations) {
            Assert.assertTrue("Operation is not completed.", o.completion.isDone());
            if (o.completion.isCompletedExceptionally()) {
                // This is failed operation; ignore it.
                continue;
            }

            if (o.operation instanceof StreamSegmentAppendOperation) {
                StreamSegmentAppendOperation appendOperation = (StreamSegmentAppendOperation) o.operation;
                List<InputStream> segmentContents = partialContents.get(appendOperation.getStreamSegmentId());
                if (segmentContents == null) {
                    segmentContents = new ArrayList<>();
                    partialContents.put(appendOperation.getStreamSegmentId(), segmentContents);
                }

                segmentContents.add(appendOperation.getData().getReader());
            } else if (o.operation instanceof MergeSegmentOperation) {
                MergeSegmentOperation mergeOperation = (MergeSegmentOperation) o.operation;
                List<InputStream> targetSegmentContents = partialContents.get(mergeOperation.getStreamSegmentId());
                if (targetSegmentContents == null) {
                    targetSegmentContents = new ArrayList<>();
                    partialContents.put(mergeOperation.getStreamSegmentId(), targetSegmentContents);
                }

                List<InputStream> sourceSegmentContents = partialContents.get(mergeOperation.getSourceSegmentId());
                targetSegmentContents.addAll(sourceSegmentContents);
                partialContents.remove(mergeOperation.getSourceSegmentId());
            }
        }

        // Construct final result.
        HashMap<Long, InputStream> result = new HashMap<>();
        for (Map.Entry<Long, List<InputStream>> e : partialContents.entrySet()) {
            result.put(e.getKey(), new SequenceInputStream(Iterators.asEnumeration(e.getValue().iterator())));
        }

        return result;
    }

    //endregion

    //region FailedStreamSegmentAppendOperation

    static class FailedStreamSegmentAppendOperation extends StreamSegmentAppendOperation {

        FailedStreamSegmentAppendOperation(StreamSegmentAppendOperation base) {
            super(base.getStreamSegmentId(), base.getData(), base.getAttributeUpdates());
        }

        @Override
        public long getStreamSegmentId() {
            throw new IntentionalException("intentional failure");
        }
    }

    //endregion

    // region CorruptedMemoryOperationLog

    @RequiredArgsConstructor
    static class CorruptedMemoryOperationLog extends InMemoryLog {
        private final long corruptAtIndex;
        private final AtomicLong addCount = new AtomicLong();

        @Override
        public void add(Operation item) {
            if (this.addCount.incrementAndGet() == this.corruptAtIndex) {
                throw new InMemoryLog.OutOfOrderOperationException("Intentional");
            }

            super.add(item);
        }
    }

    //endregion

    //region OperationWithCompletion

    @RequiredArgsConstructor
    static class OperationWithCompletion {
        final Operation operation;
        final CompletableFuture<Void> completion;

        @Override
        public String toString() {
            return String.format(
                    "(%s) %s",
                    this.completion.isDone() ? (this.completion.isCompletedExceptionally() ? "Error" : "Complete") : "Not Completed",
                    this.operation);
        }

        static CompletableFuture<Void> allOf(Collection<OperationWithCompletion> operations) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            operations.forEach(oc -> futures.add(oc.completion));
            return Futures.allOf(futures);
        }
    }

    //endregion
}
