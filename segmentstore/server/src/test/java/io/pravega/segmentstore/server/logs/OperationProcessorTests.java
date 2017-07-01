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

import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Service;
import io.pravega.common.concurrent.ServiceShutdownListener;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.ConfigHelpers;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.TruncationMarkerRepository;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationComparer;
import io.pravega.segmentstore.server.logs.operations.OperationFactory;
import io.pravega.segmentstore.server.logs.operations.ProbeOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.reading.CacheManager;
import io.pravega.segmentstore.server.reading.ContainerReadIndex;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for OperationProcessor class.
 */
public class OperationProcessorTests extends OperationLogTestBase {
    private static final int CONTAINER_ID = 1234567;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 8 * 1024;
    private static final int METADATA_CHECKPOINT_EVERY = 100;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests the ability of the OperationProcessor to process Operations in a failure-free environment.
     */
    @Test
    public void testWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int transactionsPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeTransactions = true;
        boolean sealStreamSegments = true;

        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data.
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        AbstractMap<Long, Long> transactions = createTransactionsInMetadata(streamSegmentIds, transactionsPerStreamSegment, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, transactions, appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, mergeTransactions, sealStreamSegments);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        OperationWithCompletion.allOf(completionFutures).join();

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), transactions, completionFutures, context.metadata, mergeTransactions, sealStreamSegments);
        performReadIndexChecks(completionFutures, context.readIndex);
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

        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data (no need to complicate ourselves with Transactions here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        nonExistentStreamSegmentId = streamSegmentIds.size();
        streamSegmentIds.add(nonExistentStreamSegmentId);
        context.metadata.getStreamSegmentMetadata(sealedStreamSegmentId).markSealed();
        context.metadata.getStreamSegmentMetadata(deletedStreamSegmentId).markDeleted();
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof MetadataUpdateException || ex instanceof StreamSegmentException);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        HashSet<Long> streamSegmentsWithNoContents = new HashSet<>();
        streamSegmentsWithNoContents.add(sealedStreamSegmentId);
        streamSegmentsWithNoContents.add(deletedStreamSegmentId);
        streamSegmentsWithNoContents.add(nonExistentStreamSegmentId);

        // Verify that the "right" operations failed, while the others succeeded.
        for (OperationWithCompletion oc : completionFutures) {
            if (oc.operation instanceof StorageOperation) {
                long streamSegmentId = ((StorageOperation) oc.operation).getStreamSegmentId();
                if (streamSegmentsWithNoContents.contains(streamSegmentId)) {
                    Assert.assertTrue("Completion future for invalid StreamSegment " + streamSegmentId + " did not complete exceptionally.",
                            oc.completion.isCompletedExceptionally());
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

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, streamSegmentsWithNoContents, new HashMap<>(), completionFutures, context.metadata, false, false);
        performReadIndexChecks(completionFutures, context.readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when Serialization errors happen.
     */
    @Test
    public void testWithOperationSerializationFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAppendFrequency = 7; // Fail every X appends encountered.

        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data (no need to complicate ourselves with Transactions here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, false, false);

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
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof IOException);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();

        // Verify that the "right" operations failed, while the others succeeded.
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);
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

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, context.metadata, false, false);
        performReadIndexChecks(completionFutures, context.readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when there are DataLog write failures. The expected
     * outcome is that the OperationProcessor will auto-shutdown when such errors are encountered.
     */
    @Test
    public void testWithDataLogFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAfterCommits = 5; // Fail (asynchronously) after X DataFrame commits (to DataLog).

        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data (no need to complicate ourselves with Transactions here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> count >= failAfterCommits,
                () -> new DurableDataLogException("intentional"));
        dataLog.setAppendErrorInjectors(null, aSyncErrorInjector);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                super::isExpectedExceptionForNonDataCorruption);

        // Wait for the OperationProcessor to shutdown with failure.
        ServiceShutdownListener.awaitShutdown(operationProcessor, TIMEOUT, false);
        Assert.assertEquals("Expected the OperationProcessor to fail after DurableDataLogException encountered.",
                Service.State.FAILED, operationProcessor.state());

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, context.metadata, false, false);
        performReadIndexChecks(completionFutures, context.readIndex);
    }

    /**
     * Tests the ability of the OperationProcessor handle a DataLogWriterNotPrimaryException.
     */
    @Test
    public void testWithDataLogNotPrimaryException() throws Exception {
        int streamSegmentCount = 1;
        int appendsPerStreamSegment = 1;

        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data (no need to complicate ourselves with Transactions here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> true,
                () -> new CompletionException(new DataLogWriterNotPrimaryException("intentional")));
        dataLog.setAppendErrorInjectors(null, aSyncErrorInjector);

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof IOException || ex instanceof DataLogWriterNotPrimaryException);

        // Verify that the OperationProcessor automatically shuts down and that it has the right failure cause.
        ServiceShutdownListener.awaitShutdown(operationProcessor, TIMEOUT, false);
        Assert.assertEquals("OperationProcessor is not in a failed state after fence-out detected.",
                Service.State.FAILED, operationProcessor.state());
        Assert.assertTrue("OperationProcessor did not fail with the correct exception.",
                operationProcessor.failureCause() instanceof DataLogWriterNotPrimaryException);
    }

    /**
     * Tests the ability of the OperationProcessor to process Operations when a simulated DataCorruptionException
     * is generated.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testWithDataCorruptionFailures() throws Exception {
        // If a DataCorruptionException is thrown for a particular Operation, the OperationQueueProcessor should
        // immediately shut down and stop accepting other ops.
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAtOperationIndex = 123; // Fail Operation at index X.

        @Cleanup
        TestContext context = new TestContext();

        // Create a different state updater and Memory log - and use these throughout this test.
        CorruptedMemoryOperationLog corruptedMemoryLog = new CorruptedMemoryOperationLog(failAtOperationIndex);
        MemoryStateUpdater stateUpdater = new MemoryStateUpdater(corruptedMemoryLog, context.readIndex);

        // Generate some test data (no need to complicate ourselves with Transactions here; that is tested in the no-failure test).
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, false, false);

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof DataCorruptionException);

        // Wait for the store to fail (and make sure it failed).
        AssertExtensions.assertThrows(
                "Operation Processor did not shut down with failure.",
                () -> ServiceShutdownListener.awaitShutdown(operationProcessor, true),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected service state after encountering DataCorruptionException.", Service.State.FAILED, operationProcessor.state());

        // Verify that the "right" operations failed, while the others succeeded.
        int successCount = 0;
        boolean encounteredFirstFailure = false;
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);
            if (!oc.operation.canSerialize()) {
                // Non-serializable operations (i.e., ProbeOperations always complete normally).
                continue;
            }

            // Once an operation failed (in our scenario), no other operation can succeed.
            if (encounteredFirstFailure) {
                Assert.assertTrue("Encountered successful operation after a failed operation: " + oc.operation, oc.completion.isCompletedExceptionally());
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
                        ex -> ex instanceof DataCorruptionException
                                || ex instanceof IllegalContainerStateException
                                || (ex instanceof IOException && (ex.getCause() instanceof DataCorruptionException)));
                encounteredFirstFailure = true;
            } else {
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        super::isExpectedExceptionForDataCorruption);
            }
        }

        AssertExtensions.assertGreaterThan("No operation succeeded.", 0, successCount);
        performLogOperationChecks(completionFutures, corruptedMemoryLog, dataLog, context.metadata);

        // There is no point in performing metadata checks. A DataCorruptionException means the Metadata (and the general
        // state of the Container) is in an undefined state.
    }

    /**
     * Tests the ability of the OperationProcessor to handle a single ProbeOperation (this is because it's a non-serializable
     * operation, so there is no commit to DurableDataLog - we need to verify the operation is properly completed in this
     * case).
     */
    @Test
    public void testWithSingleProbeOperation() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data.
        ProbeOperation operation = new ProbeOperation();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        OperationWithCompletion completionFuture = processOperations(Collections.singleton(operation), operationProcessor).get(0);

        // Wait for the ProbeOperation to complete (without exception). This is all we need to verify.
        completionFuture.completion.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Stop the processor.
        operationProcessor.stopAsync().awaitTerminated();
    }

    /**
     * Tests a scenario where the OperationProcessor is shut down while a DataFrame is being processed and will eventually
     * complete successfully - however its operation should be cancelled.
     */
    @Test
    public void testConcurrentStopAndCommit() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data.
        val segmentId = createStreamSegmentsInMetadata(1, context.metadata).stream().findFirst().orElse(-1L);
        List<Operation> operations = Collections.singletonList(new StreamSegmentAppendOperation(segmentId, new byte[1], null));

        CompletableFuture<LogAddress> appendCallback = new CompletableFuture<>();

        // Setup an OperationProcessor with a custom DurableDataLog and start it.
        @Cleanup
        DurableDataLog dataLog = new ManualAppendOnlyDurableDataLog(() -> appendCallback);
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        OperationWithCompletion completionFuture = processOperations(operations, operationProcessor).stream().findFirst().orElse(null);
        operationProcessor.stopAsync();
        appendCallback.complete(new TestLogAddress(1));

        // Stop the processor.
        operationProcessor.awaitTerminated();

        // Wait for the operation to complete. The operation should have been cancelled (due to the OperationProcessor
        // shutting down) - no other exception (or successful completion is accepted).
        AssertExtensions.assertThrows(
                "Operation did not fail with the right exception.",
                () -> completionFuture.completion,
                ex -> ex instanceof CancellationException);
    }

    private List<OperationWithCompletion> processOperations(Collection<Operation> operations, OperationProcessor operationProcessor) {
        List<OperationWithCompletion> completionFutures = new ArrayList<>();
        operations.forEach(op -> completionFutures.add(new OperationWithCompletion(op, operationProcessor.process(op))));
        return completionFutures;
    }

    private void performLogOperationChecks(Collection<OperationWithCompletion> operations, SequencedItemList<Operation> memoryLog,
                                           DurableDataLog dataLog, TruncationMarkerRepository truncationMarkers) throws Exception {
        // Log Operation based checks
        @Cleanup
        DataFrameReader<Operation> dataFrameReader = new DataFrameReader<>(dataLog, new OperationFactory(), CONTAINER_ID);
        long lastSeqNo = -1;
        Iterator<Operation> memoryLogIterator = memoryLog.read(-1, operations.size() + 1);
        OperationComparer memoryLogComparer = new OperationComparer(true);
        for (OperationWithCompletion oc : operations) {
            if (oc.completion.isCompletedExceptionally()) {
                // We expect this operation to not have been processed.
                continue;
            }

            if (!oc.operation.canSerialize()) {
                // We do not expect this operation in the log; skip it.
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

            // Use memoryLogComparer: we are actually expecting the same object here.
            memoryLogComparer.assertEquals("Unexpected Operation in MemoryLog.", expectedOp, memoryLogIterator.next());

            // DataLog: read back using DataFrameReader and verify the operations match that of the expected list.
            DataFrameReader.ReadResult<Operation> readResult = dataFrameReader.getNext();
            Assert.assertNotNull("No more items left to read from DataLog. Expected: " + expectedOp, readResult);

            // We are reading the raw operation from the DataFrame, so expect different objects (but same contents).
            OperationComparer.DEFAULT.assertEquals(expectedOp, readResult.getItem());

            // Check truncation markers if this is the last Operation to be written.
            LogAddress dataFrameAddress = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber());
            if (readResult.getLastFullDataFrameAddress() != null
                    && readResult.getLastFullDataFrameAddress().getSequence() != readResult.getLastUsedDataFrameAddress().getSequence()) {
                // This operation spans multiple DataFrames. The TruncationMarker should be set on the last DataFrame
                // that ends with a part of it.
                Assert.assertEquals("Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it spans multiple DataFrames.",
                        readResult.getLastFullDataFrameAddress(), dataFrameAddress);
            } else if (readResult.isLastFrameEntry()) {
                // The operation was the last one in the frame. This is a Truncation Marker.
                Assert.assertEquals("Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it is the last entry in a DataFrame.",
                        readResult.getLastUsedDataFrameAddress(), dataFrameAddress);
            } else {
                // The operation is not the last in the frame, and it doesn't span multiple frames either.
                // There could be data after it that is not safe to truncate. The correct Truncation Marker is the
                // same as the one for the previous operation.
                LogAddress expectedTruncationMarker = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber() - 1);
                Assert.assertEquals("Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it is in the middle of a DataFrame.",
                        expectedTruncationMarker, dataFrameAddress);
            }
        }
    }

    private MetadataCheckpointPolicy getNoOpCheckpointPolicy() {
        // Turn off any MetadataCheckpointing. In these tests, we are doing that manually.
        DurableLogConfig dlConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, Integer.MAX_VALUE)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, Long.MAX_VALUE)
                .build();

        return new MetadataCheckpointPolicy(dlConfig, Runnables.doNothing(), executorService());
    }

    private class TestContext implements AutoCloseable {
        final CacheManager cacheManager;
        final Storage storage;
        final SequencedItemList<Operation> memoryLog;
        final CacheFactory cacheFactory;
        final UpdateableContainerMetadata metadata;
        final ReadIndex readIndex;
        final MemoryStateUpdater stateUpdater;

        TestContext() {
            this.cacheFactory = new InMemoryCacheFactory();
            this.storage = new InMemoryStorage(executorService());
            this.storage.initialize(1);
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            ReadIndexConfig readIndexConfig = ConfigHelpers
                    .withInfiniteCachePolicy(ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024))
                    .build();
            this.cacheManager = new CacheManager(readIndexConfig.getCachePolicy(), executorService());
            this.readIndex = new ContainerReadIndex(readIndexConfig, this.metadata, this.cacheFactory, this.storage, this.cacheManager, executorService());
            this.memoryLog = new SequencedItemList<>();
            this.stateUpdater = new MemoryStateUpdater(this.memoryLog, this.readIndex);
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.storage.close();
            this.cacheFactory.close();
            this.cacheManager.close();
        }
    }


    //region ManualAppendOnlyDurableDataLog

    @RequiredArgsConstructor
    private static class ManualAppendOnlyDurableDataLog implements DurableDataLog {
        private final Supplier<CompletableFuture<LogAddress>> addImplementation;

        @Override
        public CompletableFuture<LogAddress> append(ArrayView data, Duration timeout) {
            return this.addImplementation.get();
        }

        //region Not Implemented methods.

        @Override
        public void initialize(Duration timeout) throws DurableDataLogException {

        }

        @Override
        public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
            return null;
        }

        @Override
        public int getMaxAppendLength() {
            return 1024 * 1024;
        }

        @Override
        public long getEpoch() {
            return 0;
        }

        @Override
        public QueueStats getQueueStatistics() {
            return QueueStats.DEFAULT;
        }

        @Override
        public void close() {

        }

        //endregion
    }

    //endregion
}
