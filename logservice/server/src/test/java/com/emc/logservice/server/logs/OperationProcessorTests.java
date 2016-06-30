/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.logs;

import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.contracts.StreamSegmentException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.SegmentMetadata;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.ServiceHelpers;
import com.emc.logservice.server.TestDurableDataLog;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.OperationFactory;
import com.emc.logservice.server.logs.operations.OperationHelpers;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.reading.ReadIndex;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.ErrorInjector;
import com.google.common.util.concurrent.Service;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Unit tests for OperationProcessor class.
 */
public class OperationProcessorTests {
    private static final String CONTAINER_ID = "TestContainer";
    private static final int MAX_DATA_LOG_APPEND_SIZE = 8 * 1024;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Tests the ability of the OperationProcessor to process Operations in a failure-free environment.
     */
    @Test
    public void testWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int batchesPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeBatches = true;
        boolean sealStreamSegments = true;

        // Setup all the components for the OperationProcessor
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        TruncationMarkerCollection truncationMarkers = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkers);
        MemoryOperationLog memoryLog = new MemoryOperationLog();
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(memoryLog, readIndex);

        // Generate some test data.
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        AbstractMap<Long, Long> batches = LogTestHelpers.createBatches(streamSegmentIds, batchesPerStreamSegment, metadata);
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, batches, appendsPerStreamSegment, mergeBatches, sealStreamSegments);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE);
        dataLog.initialize(TIMEOUT).join();
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        LogTestHelpers.allOf(completionFutures).join();

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        performLogOperationChecks(completionFutures, memoryLog, dataLog, truncationMarkers);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), batches, completionFutures, metadata, mergeBatches, sealStreamSegments);
        performReadIndexChecks(completionFutures, readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when encountering invalid operations (such as
     * appends to StreamSegments that do not exist or to those that are sealed). This covers the following exceptions:
     * * StreamSegmentNotExistsException
     * * StreamSegmentSealedException
     * * General MetadataUpdateException.
     */
    @Test
    public void testWithInvalidOperations() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 40;
        long sealedStreamSegmentId = 6; // We are going to prematurely seal this StreamSegment.
        long deletedStreamSegmentId = 8; // We are going to prematurely mark this StreamSegment as deleted.
        long nonExistentStreamSegmentId; // This is a bogus StreamSegment, that does not exist.

        // Setup all the components for the OperationProcessor
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        TruncationMarkerCollection truncationMarkers = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkers);
        MemoryOperationLog memoryLog = new MemoryOperationLog();
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(memoryLog, readIndex);

        // Generate some test data (no need to complicate ourselves with batches here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        nonExistentStreamSegmentId = streamSegmentIds.size();
        streamSegmentIds.add(nonExistentStreamSegmentId);
        metadata.getStreamSegmentMetadata(sealedStreamSegmentId).markSealed();
        metadata.getStreamSegmentMetadata(deletedStreamSegmentId).markDeleted();
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE);
        dataLog.initialize(TIMEOUT).join();
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof MetadataUpdateException || ex instanceof StreamSegmentException);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        HashSet<Long> streamSegmentsWithNoContents = new HashSet<>();
        streamSegmentsWithNoContents.add(sealedStreamSegmentId);
        streamSegmentsWithNoContents.add(deletedStreamSegmentId);
        streamSegmentsWithNoContents.add(nonExistentStreamSegmentId);

        // Verify that the "right" operations failed, while the others succeeded.
        for (LogTestHelpers.OperationWithCompletion oc : completionFutures) {
            if (oc.operation instanceof StorageOperation) {
                long streamSegmentId = ((StorageOperation) oc.operation).getStreamSegmentId();
                if (streamSegmentsWithNoContents.contains(streamSegmentId)) {
                    Assert.assertTrue("Completion future for invalid StreamSegment " + streamSegmentId + " did not complete exceptionally.", oc.completion.isCompletedExceptionally());
                    Predicate<Throwable> errorValidator;
                    if (streamSegmentId == sealedStreamSegmentId) {
                        errorValidator = ex -> ex instanceof StreamSegmentSealedException;
                    } else if (streamSegmentId == deletedStreamSegmentId) {
                        errorValidator = ex -> ex instanceof StreamSegmentNotExistsException;
                    } else {
                        errorValidator = ex -> ex instanceof MetadataUpdateException;
                    }

                    AssertExtensions.assertThrows("Unexpected exception for failed Operation.", oc.completion::join, errorValidator);
                    continue;
                }
            }

            // If we get here, we must verify no exception was thrown.
            oc.completion.join();
        }

        performLogOperationChecks(completionFutures, memoryLog, dataLog, truncationMarkers);
        performMetadataChecks(streamSegmentIds, streamSegmentsWithNoContents, new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when Serialization errors happen.
     */
    @Test
    public void testWithOperationSerializationFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAppendFrequency = 7; // Fail every X appends encountered.

        // Setup all the components for the OperationProcessor
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        TruncationMarkerCollection truncationMarkers = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkers);
        MemoryOperationLog memoryLog = new MemoryOperationLog();
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(memoryLog, readIndex);

        // Generate some test data (no need to complicate ourselves with batches here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

        // Replace some of the Append Operations with a FailedAppendOperations. Some operations fail at the beginning,
        // some at the end of the serialization.
        int appendCount = 0;
        HashSet<Integer> failedOperationIndices = new HashSet<>();
        for (int i = 0; i < operations.size(); i++) {
            if (operations.get(i) instanceof StreamSegmentAppendOperation) {
                if ((appendCount++) % failAppendFrequency == 0) {
                    operations.set(i, new FailedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operations.get(i), i % 2 == 0));
                    failedOperationIndices.add(i);
                }
            }
        }

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE);
        dataLog.initialize(TIMEOUT).join();
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof IOException);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        // Verify that the "right" operations failed, while the others succeeded.
        for (int i = 0; i < completionFutures.size(); i++) {
            LogTestHelpers.OperationWithCompletion oc = completionFutures.get(i);
            if (failedOperationIndices.contains(i)) {
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof IOException);
            } else {
                // Verify no exception was thrown.
                oc.completion.join();
            }
        }

        performLogOperationChecks(completionFutures, memoryLog, dataLog, truncationMarkers);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when there are DataLog write failures.
     */
    @Test
    public void testWithDataLogFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failSyncCommitFrequency = 3; // Fail (synchronously) every X DataFrame commits (to DataLog).
        int failAsyncCommitFrequency = 5; // Fail (asynchronously) every X DataFrame commits (to DataLog).

        // Setup all the components for the OperationProcessor
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        TruncationMarkerCollection truncationMarkers = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkers);
        MemoryOperationLog memoryLog = new MemoryOperationLog();
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(memoryLog, readIndex);

        // Generate some test data (no need to complicate ourselves with batches here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE);
        dataLog.initialize(TIMEOUT).join();
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        operationProcessor.startAsync().awaitRunning();

        ErrorInjector<Exception> syncErrorInjector = new ErrorInjector<>(
                count -> count % failSyncCommitFrequency == 0,
                () -> new IOException("intentional"));
        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> count % failAsyncCommitFrequency == 0,
                () -> new DurableDataLogException("intentional"));
        dataLog.setAppendErrorInjectors(syncErrorInjector, aSyncErrorInjector);

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof IOException || ex instanceof DurableDataLogException);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        performLogOperationChecks(completionFutures, memoryLog, dataLog, truncationMarkers);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when a simulated DataCorruptionException
     * is generated.
     */
    @Test
    public void testWithDataCorruptionFailures() throws Exception {
        // If a DataCorruptionException is thrown for a particular Operation, the OperationQueueProcessor should
        // immediately shut down and stop accepting other ops.
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAtOperationIndex = 123; // Fail Operation at index X.

        // Setup all the components for the OperationProcessor
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        TruncationMarkerCollection truncationMarkers = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkers);
        CorruptedMemoryOperationLog corruptedMemoryLog = new CorruptedMemoryOperationLog(failAtOperationIndex);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(corruptedMemoryLog, readIndex);

        // Generate some test data (no need to complicate ourselves with batches here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE);
        dataLog.initialize(TIMEOUT).join();
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof DataCorruptionException);

        // Wait for the service to fail (and make sure it failed).
        CompletableFuture<Void> serviceShutDown = ServiceHelpers.awaitShutDown(operationProcessor);
        AssertExtensions.assertThrows(
                "Operation Processor did not shut down with failure.",
                serviceShutDown::join,
                ex -> true);
        Assert.assertEquals("Unexpected service state after encountering DataCorruptionException.", Service.State.FAILED, operationProcessor.state());

        // Verify that the "right" operations failed, while the others succeeded.
        int successCount = 0;
        boolean encounteredFirstFailure = false;
        for (int i = 0; i < completionFutures.size(); i++) {
            LogTestHelpers.OperationWithCompletion oc = completionFutures.get(i);

            // Once an operation failed (in our scenario), no other operation can succeed.
            if (encounteredFirstFailure) {
                Assert.assertTrue("Encountered successful operation after a failed operation.", oc.completion.isCompletedExceptionally());
            }
            if (i < failAtOperationIndex) {
                // The operation that failed may have inadvertently failed other operations that were aggregated together
                // with it, which is why it's hard to determine precisely what the first expected failed operation is.
                if (oc.completion.isCompletedExceptionally()) {
                    // If we do find a failed one in this area, make sure it is failed with DataCorruptionException.
                    AssertExtensions.assertThrows(
                            "Unexpected exception for failed Operation in the same DataFrame as intentionally failed operation.",
                            oc.completion::join,
                            ex -> ex instanceof DataCorruptionException);
                    encounteredFirstFailure = true;
                } else {
                    successCount++;
                }
            } else if (i == failAtOperationIndex) {
                AssertExtensions.assertThrows(
                        "Unexpected exception for intentionally failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof DataCorruptionException);
                encounteredFirstFailure = true;
            } else {
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof DataCorruptionException
                                || ex instanceof IllegalStateException
                                || (ex instanceof IOException && (ex.getCause() instanceof DataCorruptionException || ex.getCause() instanceof IllegalStateException)));
            }
        }

        AssertExtensions.assertGreaterThan("No operation succeeded.", 0, successCount);
        performLogOperationChecks(completionFutures, corruptedMemoryLog, dataLog, truncationMarkers);

        // There is no point in performing metadata checks. A DataCorruptionException means the Metadata (and the general
        // state of the Container) is in an undefined state.
    }

    private List<LogTestHelpers.OperationWithCompletion> processOperations(Collection<Operation> operations, OperationProcessor operationProcessor) {
        List<LogTestHelpers.OperationWithCompletion> completionFutures = new ArrayList<>();
        operations.forEach(op -> completionFutures.add(new LogTestHelpers.OperationWithCompletion(op, operationProcessor.process(op))));
        return completionFutures;
    }

    private void performLogOperationChecks(Collection<LogTestHelpers.OperationWithCompletion> operations, MemoryOperationLog memoryLog, DurableDataLog dataLog, TruncationMarkerCollection truncationMarkers) throws Exception {
        // Log Operation based checks
        @Cleanup
        DataFrameReader<Operation> dataFrameReader = new DataFrameReader<>(dataLog, new OperationFactory(), CONTAINER_ID);
        long lastSeqNo = -1;
        DataFrameReader.ReadResult lastReadResult = null;
        Iterator<Operation> memoryLogIterator = memoryLog.read(o -> true, operations.size() + 1);
        for (LogTestHelpers.OperationWithCompletion oc : operations) {
            if (oc.completion.isCompletedExceptionally()) {
                // We expect this operation to not have been processed.
                continue;
            }

            // Verify that the operations have been completed and assigned sequential Sequence Numbers.
            Operation expectedOp = oc.operation;
            long currentSeqNo = oc.completion.join();
            Assert.assertEquals("Operation and its corresponding Completion Future have different Sequence Numbers.", currentSeqNo, expectedOp.getSequenceNumber());
            AssertExtensions.assertGreaterThan("Operations were not assigned sequential Sequence Numbers.", lastSeqNo, currentSeqNo);
            lastSeqNo = currentSeqNo;

            // MemoryLog: verify that the operations match that of the expected list.
            Assert.assertTrue("No more items left to read from MemoryLog. Expected: " + expectedOp, memoryLogIterator.hasNext());
            Assert.assertEquals("Unexpected Operation in MemoryLog.", expectedOp, memoryLogIterator.next()); // Ok to use assertEquals because we are actually expecting the same object here.

            // DataLog: read back using DataFrameReader and verify the operations match that of the expected list.
            DataFrameReader.ReadResult<Operation> readResult = dataFrameReader.getNext();
            Assert.assertNotNull("No more items left to read from DataLog. Expected: " + expectedOp, readResult);
            OperationHelpers.assertEquals(expectedOp, readResult.getItem());

            // Check truncation markers if this is the last Operation to be written.
            if (lastReadResult != null && !lastReadResult.isLastFrameEntry() && readResult.getDataFrameSequence() != lastReadResult.getDataFrameSequence()) {
                long dataFrameSeq = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber());
                Assert.assertEquals("Unexpected truncation marker for Operation Sequence Number " + expectedOp.getSequenceNumber(), lastReadResult.getDataFrameSequence(), dataFrameSeq);
            } else if (readResult.isLastFrameEntry()) {
                // The current Log Operation was the last one in the frame. This is a Truncation Marker.
                long dataFrameSeq = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber());
                Assert.assertEquals("Unexpected truncation marker for Operation Sequence Number " + expectedOp.getSequenceNumber(), readResult.getDataFrameSequence(), dataFrameSeq);
            }

            lastReadResult = readResult;
        }
    }

    private void performMetadataChecks(Collection<Long> streamSegmentIds, Collection<Long> invalidStreamSegmentIds, AbstractMap<Long, Long> batches, Collection<LogTestHelpers.OperationWithCompletion> operations, SegmentMetadataCollection metadata, boolean expectBatchesMerged, boolean expectSegmentsSealed) {
        // Verify that batches are merged
        for (long batchId : batches.keySet()) {
            SegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(batchId);
            if (invalidStreamSegmentIds.contains(batchId)) {
                Assert.assertTrue("Unexpected data for a Batch that was invalid.", batchMetadata == null || batchMetadata.getDurableLogLength() == 0);
            } else {
                Assert.assertEquals("Unexpected batch seal state for batch " + batchId, expectBatchesMerged, batchMetadata.isSealed());
                Assert.assertEquals("Unexpected batch merge state for batch " + batchId, expectBatchesMerged, batchMetadata.isMerged());
            }
        }

        // Verify the end state of each stream segment (length, sealed).
        AbstractMap<Long, Integer> expectedLengths = LogTestHelpers.getExpectedLengths(operations);
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

    private void performReadIndexChecks(Collection<LogTestHelpers.OperationWithCompletion> operations, Cache readIndex) throws Exception {
        AbstractMap<Long, Integer> expectedLengths = LogTestHelpers.getExpectedLengths(operations);
        AbstractMap<Long, InputStream> expectedData = LogTestHelpers.getExpectedContents(operations);
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

    private static class FailedStreamSegmentAppendOperation extends StreamSegmentAppendOperation {
        private final boolean failAtBeginning;

        public FailedStreamSegmentAppendOperation(StreamSegmentAppendOperation base, boolean failAtBeginning) {
            super(base.getStreamSegmentId(), base.getData(), base.getAppendContext());
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

    private static class CorruptedMemoryOperationLog extends MemoryOperationLog {
        private final long corruptAtIndex;
        private final AtomicLong addCount;

        public CorruptedMemoryOperationLog(int corruptAtIndex) {
            this.corruptAtIndex = corruptAtIndex;
            this.addCount = new AtomicLong();
        }

        @Override
        public boolean addIf(Operation item, Predicate<Operation> lastItemChecker) {
            if (this.addCount.incrementAndGet() == this.corruptAtIndex) {
                // Still add the item, but report that we haven't added it.
                return false;
            }

            return super.addIf(item, lastItemChecker);
        }
    }
}
