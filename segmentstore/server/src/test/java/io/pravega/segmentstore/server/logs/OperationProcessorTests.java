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

import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Service;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.ServiceListeners;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.TruncationMarkerRepository;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CheckpointOperationBase;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationComparer;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.reading.ContainerReadIndex;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
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
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        OperationWithCompletion.allOf(completionFutures).join();

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), transactions, completionFutures, context.metadata, mergeTransactions, sealStreamSegments);
        performReadIndexChecks(completionFutures, context.readIndex);
        operationProcessor.stopAsync().awaitTerminated();
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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof MetadataUpdateException || ex instanceof StreamSegmentException);

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
        operationProcessor.stopAsync().awaitTerminated();
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

        // Replace some of the Append Operations with a FailedAppendOperation. Some operations fail at the beginning,
        // some at the end of the serialization.
        int appendCount = 0;
        HashSet<Integer> failedOperationIndices = new HashSet<>();
        for (int i = 0; i < operations.size(); i++) {
            if (operations.get(i) instanceof StreamSegmentAppendOperation) {
                if ((appendCount++) % failAppendFrequency == 0) {
                    operations.set(i, new FailedStreamSegmentAppendOperation((StreamSegmentAppendOperation) operations.get(i)));
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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof IntentionalException);

        // Verify that the "right" operations failed, while the others succeeded.
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);
            if (failedOperationIndices.contains(i)) {
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof IntentionalException);
            } else {
                // Verify no exception was thrown.
                oc.completion.join();
            }
        }

        performLogOperationChecks(completionFutures, context.memoryLog, dataLog, context.metadata);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, context.metadata, false, false);
        performReadIndexChecks(completionFutures, context.readIndex);
        operationProcessor.stopAsync().awaitTerminated();
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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
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
        ServiceListeners.awaitShutdown(operationProcessor, TIMEOUT, false);
        Assert.assertEquals("Expected the OperationProcessor to fail after DurableDataLogException encountered.",
                Service.State.FAILED, operationProcessor.state());

        performFailureRecoveryChecks(completionFutures, dataLog);
        context.close();
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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
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
        ServiceListeners.awaitShutdown(operationProcessor, TIMEOUT, false);
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
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for the store to fail (and make sure it failed).
        AssertExtensions.assertThrows(
                "Operation Processor did not shut down with failure.",
                () -> ServiceListeners.awaitShutdown(operationProcessor, true),
                ex -> ex instanceof IllegalStateException);
        Assert.assertEquals("Unexpected service state after encountering DataCorruptionException.", Service.State.FAILED, operationProcessor.state());

        // Verify that the "right" operations failed, while the others succeeded.
        int successCount = 0;
        boolean encounteredFirstFailure = false;
        for (int i = 0; i < completionFutures.size(); i++) {
            OperationWithCompletion oc = completionFutures.get(i);

            // Once an operation failed (in our scenario), no other operation can succeed.
            if (encounteredFirstFailure) {
                Assert.assertTrue("Encountered successful operation after a failed operation: " + oc.operation, oc.completion.isCompletedExceptionally());
            }
            // The operation that failed may have inadvertently failed other operations that were aggregated together
            // with it, which is why it's hard to determine precisely what the first expected failed operation is.
            if (oc.completion.isCompletedExceptionally()) {
                // If we do find a failed one in this area, make sure it is failed with DataCorruptionException.
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation in the same DataFrame as intentionally failed operation.",
                        oc.completion::join,
                        super::isExpectedExceptionForDataCorruption);
                encounteredFirstFailure = true;
            } else {
                successCount++;
            }
        }

        AssertExtensions.assertGreaterThan("No operation succeeded.", 0, successCount);
        performLogOperationChecks(completionFutures, corruptedMemoryLog, dataLog, context.metadata, failAtOperationIndex - 1);

        // There is no point in performing metadata checks. A DataCorruptionException means the Metadata (and the general
        // state of the Container) is in an undefined state.
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
        List<Operation> operations = Collections.singletonList(new StreamSegmentAppendOperation(segmentId, new ByteArraySegment(new byte[1]), null));

        CompletableFuture<LogAddress> appendCallback = new CompletableFuture<>();

        // Setup an OperationProcessor with a custom DurableDataLog and start it.
        @Cleanup
        DurableDataLog dataLog = new ManualAppendOnlyDurableDataLog(() -> appendCallback);
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Process all generated operations.
        OperationWithCompletion completionFuture = processOperations(operations, operationProcessor).stream().findFirst().orElse(null);
        operationProcessor.stopAsync();
        appendCallback.complete(new TestLogAddress(1));

        // Stop the processor.
        operationProcessor.awaitTerminated();

        // Wait for the operation to complete. The operation should have been cancelled (due to the OperationProcessor
        // shutting down) - no other exception (or successful completion is accepted).
        AssertExtensions.assertSuppliedFutureThrows(
                "Operation did not fail with the right exception.",
                () -> completionFuture.completion,
                ex -> ex instanceof CancellationException || ex instanceof ObjectClosedException);
    }

    /**
     * Tests throttling and Operation Priorities.
     */
    @Test
    public void testThrottlingAndPriorities() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Generate some test data.
        //val segmentId = createStreamSegmentsInMetadata(1, context.metadata).stream().findFirst().orElse(-1L);
        val op1 = new MetadataCheckpointOperation();
        val op2 = new MetadataCheckpointOperation();

        // Setup a ThrottledOperationProcessor with a ManualThrottler and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);

        val interrupted = new AtomicBoolean(false);
        @Cleanup
        val throttler = new ManualThrottler(() -> interrupted.set(true), new NoOpCalculator(), () -> false, executorService());
        @Cleanup
        val operationProcessor = new ThrottledOperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService(), throttler);
        operationProcessor.startAsync().awaitRunning();

        // Block processing of operations.
        throttler.setThrottleEnabled(true);

        // Queue up OP1 with Normal priority (should be subject to throttling).
        val op1Future = operationProcessor.process(op1, OperationPriority.Normal);

        // Check if we are currently throttling. This is sometimes set in a background thread so we need to use assertEventuallyEquals.
        AssertExtensions.assertEventuallyEquals(true, throttler::isCurrentlyThrottling, TIMEOUT.toMillis());
        Assert.assertTrue("Expected to be throttling at this point.", throttler.isCurrentlyThrottling());
        Assert.assertFalse("Not expected OP1 to be complete yet.", op1Future.isDone());
        Assert.assertFalse("Not expected an interruption yet.", interrupted.get());

        // Queue up OP2 with Critical priority (should interrupt throttling and execute first).
        val op2Future = operationProcessor.process(op2, OperationPriority.Critical);
        Assert.assertTrue("Still expecting throttling to be happening.", throttler.isCurrentlyThrottling());
        Assert.assertFalse("Not expecting OP2 to be complete yet.", op2Future.isDone());
        Assert.assertTrue("Expected interruption due to Operation Priority.", interrupted.get());

        // Terminate the throttling delay and do not throttle anymore.
        throttler.setThrottleEnabled(false);
        throttler.completeDelayFuture();

        // Wait for our operations to have completed.
        CompletableFuture.allOf(op1Future, op2Future).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify that OP2 has executed prior to OP1.
        AssertExtensions.assertGreaterThan("Expected OP2 (Critical) to have been executed prior to OP1 (Normal),",
                op2.getSequenceNumber(), op1.getSequenceNumber());

        operationProcessor.stopAsync().awaitTerminated();
    }

    /**
     * Tests the behavior of the OperationProcessor when handling a {@link CancellationException} and {@link ObjectClosedException}.
     */
    @Test
    public void testWithCancellationExceptionAndObjectClosedException() throws Exception {
        int streamSegmentCount = 1;
        int appendsPerStreamSegment = 1;
        int transactionsPerStreamSegment = 2;
        boolean mergeTransactions = true;
        boolean sealStreamSegments = true;

        @Cleanup
        TestContext context = new TestContext();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService());
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Generate some test data.
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        AbstractMap<Long, Long> transactions = createTransactionsInMetadata(streamSegmentIds, transactionsPerStreamSegment, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, transactions, appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, mergeTransactions, sealStreamSegments);

        // The first operation will be a CancellationException, which is supposed to do not shut down the OperationProcessor.
        Operation cancellationException = spy(operations.get(0));
        Mockito.doThrow(new CancellationException("Intentional")).when(cancellationException).setSequenceNumber(anyLong());
        operations.add(0, cancellationException);
        List<OperationWithCompletion> completionFutures = processOperations(operations.subList(0, 1), operationProcessor);
        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof CancellationException);
        operations.remove(0);
        // Even though the exception, the OperationProcessor is not closed.
        Assert.assertTrue("OperationProcessor not running when it should.", operationProcessor.isRunning());

        // The second operation will be a ObjectCloseException, which is supposed to shut down the OperationProcessor.
        Operation objectClosedException = spy(operations.get(0));
        Mockito.doThrow(new ObjectClosedException("Intentional")).when(objectClosedException).setSequenceNumber(anyLong());
        operations.add(0, objectClosedException);
        completionFutures = processOperations(operations, operationProcessor);
        AssertExtensions.assertThrows(
                "No operations failed.",
                OperationWithCompletion.allOf(completionFutures)::join,
                ex -> ex instanceof ObjectClosedException);

        // Verify that the OperationProcessor automatically shuts down and that it has the right failure cause.
        ServiceListeners.awaitShutdown(operationProcessor, TIMEOUT, false);
        Assert.assertEquals("OperationProcessor is not in a failed state after ObjectClosedException detected.",
                Service.State.FAILED, operationProcessor.state());
        Assert.assertTrue("OperationProcessor did not fail with the correct exception.",
                operationProcessor.failureCause() instanceof ObjectClosedException);
        Assert.assertFalse("OperationProcessor running when it should not.", operationProcessor.isRunning());
    }

    /**
     * Tests the behavior of the OperationProcessor when handling failed futures with {@link ObjectClosedException}.
     */
    @Test
    public void testStartWithFailedFuturesInDurableLog() throws Exception {
        int streamSegmentCount = 100;
        int appendsPerStreamSegment = 100;
        int transactionsPerStreamSegment = 2;
        boolean mergeTransactions = true;
        boolean sealStreamSegments = true;

        @Cleanup
        TestContext context = new TestContext();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = spy(TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService()));
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Generate some test data.
        HashSet<Long> streamSegmentIds = createStreamSegmentsInMetadata(streamSegmentCount, context.metadata);
        AbstractMap<Long, Long> transactions = createTransactionsInMetadata(streamSegmentIds, transactionsPerStreamSegment, context.metadata);
        List<Operation> operations = generateOperations(streamSegmentIds, transactions, appendsPerStreamSegment,
                METADATA_CHECKPOINT_EVERY, mergeTransactions, sealStreamSegments);

        CompletableFuture<LogAddress> failedFuture = CompletableFuture.failedFuture(new ObjectClosedException("Intentional"));
        doReturn(failedFuture).when(dataLog).append(any(CompositeArrayView.class), any(Duration.class));
        List<OperationWithCompletion> completionFutures = processOperations(operations, operationProcessor);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertFutureThrows("No operations failed or failed with wrong exception.",
                OperationWithCompletion.allOf(completionFutures),
                ex -> ex instanceof ObjectClosedException);

        // Verify that the OperationProcessor automatically shuts down and that it has the right failure cause.
        ServiceListeners.awaitShutdown(operationProcessor, TIMEOUT, false);
        Assert.assertEquals("OperationProcessor is not in a failed state after ObjectClosedException detected.",
                Service.State.FAILED, operationProcessor.state());
        Assert.assertFalse("OperationProcessor running when it should not.", operationProcessor.isRunning());
    }

    /**
     * Tests the behavior of the OperationProcessor when the operationQueue is unexpectedly closed.
     */
    @Test
    public void testOperationProcessorShutsDownIfQueueClosed() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = spy(TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService()));
        dataLog.initialize(TIMEOUT);
        @Cleanup
        OperationProcessor operationProcessor = new OperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService());
        operationProcessor.startAsync().awaitRunning();

        // Close the queue, simulating that some error unexpectedly close it.
        operationProcessor.closeQueue(new CompletionException(new IntentionalException("")));

        // Make sure that we get an ObjectClosedException when attempting to add an operation to process.
        AssertExtensions.assertFutureThrows("Expected ObjectClosedException when adding new operation.",
                operationProcessor.process(mock(Operation.class), OperationPriority.Critical),
                ex -> ex instanceof ObjectClosedException || ex instanceof IllegalContainerStateException);

        // Check that eventually the OperationProcessor is unblocked and shuts down.
        AssertExtensions.assertEventuallyEquals(false, operationProcessor::isRunning, 6000);
    }

    /**
     * Verifies that the OperationProcessor can shut down when the operationQueue is closed and the Throttler is inducing
     * MAX_DELAYs and there are incoming operations.
     *
     * @throws Exception
     */
    @Test
    public void testOperationProcessorClosedQueueWhileMaxThrottlingAndOperations() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = spy(TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService()));
        dataLog.initialize(TIMEOUT);

        // There is some entanglement between Throttler and OperationProcessor which makes testing the shutdown of OperationProcessor
        // when MAX_DELAY is being induced a bit tricky. The isSuspended() supplier in Throttler actually depends on
        // OperationProcessor.hasToSuspendThrottlingDelay(). And, at the same time, if we want to control the Throttler
        // to retrieve MAX_DELAY, we need to instantiate it outside OperationProcessor, despite it depends on it. To solve this,
        // we use the isSuspendedReference to build the Throttler initialized to "false", and once the OperationProcessot is
        // instantiated. we then set the supplier to actually use the OperationProcessor.hasToSuspendThrottlingDelay() method.
        AtomicReference<Supplier<Boolean>> isSuspendedReference = new AtomicReference<>(() -> false);
        int maxDelay = DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue();
        @Cleanup
        val throttler = new TestThrottler(new MaxDelayThrottler(maxDelay), () -> isSuspendedReference.get().get(), maxDelay, executorService());
        @Cleanup
        val operationProcessor = new ThrottledOperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService(), throttler);
        // Here we set the isSuspendedReference with the right supplier.
        isSuspendedReference.set(operationProcessor::shouldSuspendThrottlingDelay);
        operationProcessor.startAsync().awaitRunning();

        // Close the queue, simulating that some error unexpectedly close it.
        operationProcessor.closeQueue(new CompletionException(new IntentionalException("")));

        // Make sure that we get an ObjectClosedException when attempting to add a Normal operation to process.
        AssertExtensions.assertFutureThrows("Expected ObjectClosedException when adding new Normal operation.",
                operationProcessor.process(mock(Operation.class), OperationPriority.Normal),
                ex -> ex instanceof ObjectClosedException || ex instanceof IllegalContainerStateException);

        // Make sure that we get an ObjectClosedException when attempting to add a Critical operation to process, but we
        // should expect the OperationProcessor to shut down.
        AssertExtensions.assertFutureThrows("Expected ObjectClosedException when adding new Critical operation.",
                operationProcessor.process(mock(Operation.class), OperationPriority.Critical),
                ex -> ex instanceof ObjectClosedException || ex instanceof IllegalContainerStateException);

        // Check that eventually the OperationProcessor is unblocked and shuts down.
        AssertExtensions.assertEventuallyEquals(false, operationProcessor::isRunning, 6000);
    }

    /**
     * Verifies that the OperationProcessor can shut down when the operationQueue is closed and the Throttler is inducing
     * MAX_DELAYs and there are no incoming operations.
     *
     * @throws Exception
     */
    @Test
    public void testOperationProcessorClosedQueueWhileMaxThrottlingAndNoOperations() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Setup an OperationProcessor and start it.
        @Cleanup
        TestDurableDataLog dataLog = spy(TestDurableDataLog.create(CONTAINER_ID, MAX_DATA_LOG_APPEND_SIZE, executorService()));
        dataLog.initialize(TIMEOUT);

        // There is some entanglement between Throttler and OperationProcessor which makes testing the shutdown of OperationProcessor
        // when MAX_DELAY is being induced a bit tricky. The isSuspended() supplier in Throttler actually depends on
        // OperationProcessor.hasToSuspendThrottlingDelay(). And, at the same time, if we want to control the Throttler
        // to retrieve MAX_DELAY, we need to instantiate it outside OperationProcessor, despite it depends on it. To solve this,
        // we use the isSuspendedReference to build the Throttler initialized to "false", and once the OperationProcessot is
        // instantiated. we then set the supplier to actually use the OperationProcessor.hasToSuspendThrottlingDelay() method.
        AtomicReference<Supplier<Boolean>> isSuspendedReference = new AtomicReference<>(() -> false);
        int maxDelay = 3000;
        @Cleanup
        val throttler = new TestThrottler(new MaxDelayThrottler(maxDelay), () -> isSuspendedReference.get().get(), maxDelay, executorService());
        @Cleanup
        val operationProcessor = new ThrottledOperationProcessor(context.metadata, context.stateUpdater,
                dataLog, getNoOpCheckpointPolicy(), getDefaultThrottlerSettings(), executorService(), throttler);
        // Here we set the isSuspendedReference with the right supplier.
        isSuspendedReference.set(operationProcessor::shouldSuspendThrottlingDelay);
        operationProcessor.startAsync().awaitRunning();

        // Close the queue, simulating that some error unexpectedly close it.
        operationProcessor.closeQueue(new CompletionException(new IntentionalException("")));

        // Check that eventually the OperationProcessor is unblocked and shuts down.
        AssertExtensions.assertEventuallyEquals(false, operationProcessor::isRunning, 6000);
    }

    private List<OperationWithCompletion> processOperations(Collection<Operation> operations, OperationProcessor operationProcessor) {
        List<OperationWithCompletion> completionFutures = new ArrayList<>();
        operations.forEach(op -> completionFutures.add(new OperationWithCompletion(op, operationProcessor.process(op, OperationPriority.Normal))));
        return completionFutures;
    }

    private void performLogOperationChecks(Collection<OperationWithCompletion> operations, InMemoryLog memoryLog,
                                           DurableDataLog dataLog, TruncationMarkerRepository truncationMarkers) throws Exception {
        performLogOperationChecks(operations, memoryLog, dataLog, truncationMarkers, Integer.MAX_VALUE);
    }

    private void performLogOperationChecks(Collection<OperationWithCompletion> operations, InMemoryLog memoryLog,
                                           DurableDataLog dataLog, TruncationMarkerRepository truncationMarkers, int maxCount) throws Exception {
        // Log Operation based checks
        val successfulOps = operations.stream()
                                      .filter(oc -> !oc.completion.isCompletedExceptionally())
                                      .map(oc -> oc.operation)
                                      .limit(maxCount)
                                      .collect(Collectors.toList());

        @Cleanup
        DataFrameReader<Operation> dataFrameReader = new DataFrameReader<>(dataLog, new OperationSerializer(), CONTAINER_ID);
        long lastSeqNo = -1;

        Iterator<Operation> memoryLogIterator;
        if (successfulOps.isEmpty()) {
            memoryLogIterator = Collections.emptyIterator();
        } else {
            val memoryLogOps = readUpToSequenceNumber(memoryLog, successfulOps.get(successfulOps.size() - 1).getSequenceNumber());
            memoryLogIterator = memoryLogOps.iterator();
        }

        OperationComparer memoryLogComparer = new OperationComparer(true);
        for (Operation expectedOp : successfulOps) {
            // Verify that the operations have been completed and assigned sequential Sequence Numbers.
            AssertExtensions.assertGreaterThan("Operations were not assigned sequential Sequence Numbers.", lastSeqNo, expectedOp.getSequenceNumber());
            lastSeqNo = expectedOp.getSequenceNumber();

            // MemoryLog: verify that the operations match that of the expected list.
            Assert.assertTrue("No more items left to read from MemoryLog. Expected: " + expectedOp, memoryLogIterator.hasNext());

            // Use memoryLogComparer: we are actually expecting the same object here.
            Operation actual = memoryLogIterator.next();
            memoryLogComparer.assertEquals("Unexpected Operation in MemoryLog.", expectedOp, actual);

            // DataLog: read back using DataFrameReader and verify the operations match that of the expected list.
            DataFrameRecord<Operation> dataFrameRecord = dataFrameReader.getNext();
            Assert.assertNotNull("No more items left to read from DataLog. Expected: " + expectedOp, dataFrameRecord);

            // We are reading the raw operation from the DataFrame, so expect different objects (but same contents).
            if (expectedOp instanceof CheckpointOperationBase) {
                // Checkpoint operations are different. While they do serialize their contents, we do not hold on to that
                // since they may be pretty big and serve no purpose after serialization. There are other tests in this suite
                // and in ContainerMetadataUpdateTransactionTests and DurableLogTests that verify we can properly read
                // their contents during recovery.
                val actualEntry = (CheckpointOperationBase) dataFrameRecord.getItem();
                Assert.assertNull("Expected in-memory checkpoint operation to not have contents set.", ((CheckpointOperationBase) expectedOp).getContents());
                Assert.assertNotNull("Expected serialized checkpoint operation to have contents set.", actualEntry.getContents());
                Assert.assertEquals(" Unexpected Sequence Number", expectedOp.getSequenceNumber(), actualEntry.getSequenceNumber());
            } else {
                // All other operations.
                OperationComparer.DEFAULT.assertEquals(expectedOp, dataFrameRecord.getItem());
            }

            // Check truncation markers if this is the last Operation to be written.
            if (dataFrameRecord.getLastFullDataFrameAddress() != null
                    && dataFrameRecord.getLastFullDataFrameAddress().getSequence() != dataFrameRecord.getLastUsedDataFrameAddress().getSequence()) {
                // This operation spans multiple DataFrames. The TruncationMarker should be set on the last DataFrame
                // that ends with a part of it.
                AssertExtensions.assertEventuallyEquals(
                        "Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it spans multiple DataFrames.",
                        dataFrameRecord.getLastFullDataFrameAddress(),
                        () -> truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber()), 100, TIMEOUT.toMillis());
            } else if (dataFrameRecord.isLastFrameEntry()) {
                // The operation was the last one in the frame. This is a Truncation Marker.
                AssertExtensions.assertEventuallyEquals(
                        "Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it is the last entry in a DataFrame.",
                        dataFrameRecord.getLastUsedDataFrameAddress(),
                        () -> truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber()), 100, TIMEOUT.toMillis());
            } else {
                // The operation is not the last in the frame, and it doesn't span multiple frames either.
                // There could be data after it that is not safe to truncate. The correct Truncation Marker is the
                // same as the one for the previous operation.
                LogAddress expectedTruncationMarker = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber() - 1);
                LogAddress dataFrameAddress = truncationMarkers.getClosestTruncationMarker(expectedOp.getSequenceNumber());
                Assert.assertEquals("Unexpected truncation marker for Operation SeqNo " + expectedOp.getSequenceNumber() + " when it is in the middle of a DataFrame.",
                        expectedTruncationMarker, dataFrameAddress);
            }
        }
    }

    private void performFailureRecoveryChecks(List<OperationWithCompletion> operations, TestDurableDataLog dataLog) throws Exception {
        boolean encounteredFailure = false;
        val successfulOps = new ArrayList<Operation>();
        for (val oc : operations) {
            encounteredFailure |= oc.completion.isCompletedExceptionally();
            if (encounteredFailure) {
                Assert.assertTrue("Found a non-errorred operation after an errorred operation.", oc.completion.isCompletedExceptionally());
            } else {
                Assert.assertTrue(oc.completion.isDone());
                successfulOps.add(oc.operation);
            }
        }

        @Cleanup
        DataFrameReader<Operation> dataFrameReader = new DataFrameReader<>(dataLog, new OperationSerializer(), CONTAINER_ID);
        long lastSeqNo = -1;
        for (Operation expectedOp : successfulOps) {
            // Verify that the operations have been completed and assigned sequential Sequence Numbers.
            AssertExtensions.assertGreaterThan("Operations were not assigned sequential Sequence Numbers.", lastSeqNo, expectedOp.getSequenceNumber());
            lastSeqNo = expectedOp.getSequenceNumber();

            // DataLog: read back using DataFrameReader and verify the operations match that of the expected list.
            DataFrameRecord<Operation> dataFrameRecord = dataFrameReader.getNext();
            Assert.assertNotNull("No more items left to read from DataLog. Expected: " + expectedOp, dataFrameRecord);

            // We are reading the raw operation from the DataFrame, so expect different objects (but same contents).
            if (expectedOp instanceof CheckpointOperationBase) {
                // Checkpoint operations are different. While they do serialize their contents, we do not hold on to that
                // since they may be pretty big and serve no purpose after serialization. There are other tests in this suite
                // and in ContainerMetadataUpdateTransactionTests and DurableLogTests that verify we can properly read
                // their contents during recovery.
                val actualEntry = (CheckpointOperationBase) dataFrameRecord.getItem();
                Assert.assertNull("Expected in-memory checkpoint operation to not have contents set.", ((CheckpointOperationBase) expectedOp).getContents());
                Assert.assertNotNull("Expected serialized checkpoint operation to have contents set.", actualEntry.getContents());
                Assert.assertEquals(" Unexpected Sequence Number", expectedOp.getSequenceNumber(), actualEntry.getSequenceNumber());
            } else {
                // All other operations.
                OperationComparer.DEFAULT.assertEquals(expectedOp, dataFrameRecord.getItem());
            }
        }
    }

    @SneakyThrows
    private List<Operation> readUpToSequenceNumber(InMemoryLog log, long seqNo) {
        ArrayList<Operation> result = new ArrayList<>();
        while (true) {
            // Figure out if we've already reached our limit.
            long afterSequence = result.size() == 0 ? -1 : result.get(result.size() - 1).getSequenceNumber();
            if (afterSequence >= seqNo) {
                break;
            }

            // Figure out how much to read. If we don't know, read at least one item so we see what's the first SeqNo
            // in the Log.
            int maxCount = result.size() == 0 ? 1 : (int) (seqNo - result.get(result.size() - 1).getSequenceNumber());
            Queue<Operation> logIterator = log.take(maxCount, TIMEOUT, executorService()).get(5000, TimeUnit.MILLISECONDS);
            while (!logIterator.isEmpty()) {
                result.add(logIterator.poll());
            }
        }
        return result;
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

    private ThrottlerPolicy getDefaultThrottlerSettings() {
        DurableLogConfig dlConfig = DurableLogConfig.builder().build();
        return new ThrottlerPolicy(dlConfig);
    }

    private class TestContext implements AutoCloseable {
        final CacheManager cacheManager;
        final Storage storage;
        final InMemoryLog memoryLog;
        final CacheStorage cacheStorage;
        final UpdateableContainerMetadata metadata;
        final ReadIndex readIndex;
        final MemoryStateUpdater stateUpdater;

        TestContext() {
            this.storage = InMemoryStorageFactory.newStorage(executorService());
            this.storage.initialize(1);
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            ReadIndexConfig readIndexConfig = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService());
            this.readIndex = new ContainerReadIndex(readIndexConfig, this.metadata, this.storage, this.cacheManager, executorService());
            this.memoryLog = new InMemoryLog();
            this.stateUpdater = new MemoryStateUpdater(this.memoryLog, this.readIndex);
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.storage.close();
            this.cacheManager.close();
            this.cacheStorage.close();
            this.memoryLog.close();
        }
    }

    //region ThrottledOperationProcessor

    private static class ThrottledOperationProcessor extends OperationProcessor {
        @Getter
        private final Throttler throttler;

        ThrottledOperationProcessor(UpdateableContainerMetadata metadata, MemoryStateUpdater stateUpdater,
                                    DurableDataLog durableDataLog, MetadataCheckpointPolicy checkpointPolicy,
                                    ThrottlerPolicy throttlerPolicy, ScheduledExecutorService executor,
                                    Throttler throttler) {
            super(metadata, stateUpdater, durableDataLog, checkpointPolicy, throttlerPolicy, executor);
            this.throttler = throttler;
        }
    }

    private static class ManualThrottler extends Throttler {
        private final AtomicBoolean throttleEnabled = new AtomicBoolean(true);
        private final AtomicReference<CompletableFuture<Void>> lastDelayFuture = new AtomicReference<>();
        private final Runnable onNotifyThrottleSourceChanged;

        ManualThrottler(Runnable onNotifyThrottleSourceChanged, ThrottlerCalculator.Throttler throttler, Supplier<Boolean> isSuspended, ScheduledExecutorService executor) {
            super(CONTAINER_ID, ThrottlerCalculator.builder().maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue()).throttler(throttler).build(),
                    isSuspended, executor, new SegmentStoreMetrics.OperationProcessor(CONTAINER_ID));
            this.onNotifyThrottleSourceChanged = onNotifyThrottleSourceChanged;
        }

        void setThrottleEnabled(boolean enabled) {
            this.throttleEnabled.set(enabled);
        }

        @Override
        public boolean isThrottlingRequired() {
            return this.throttleEnabled.get();
        }

        @Override
        public CompletableFuture<Void> throttle() {
            if (!this.throttleEnabled.get()) {
                return CompletableFuture.completedFuture(null);
            }

            val oldDelay = this.lastDelayFuture.getAndSet(null);
            Assert.assertTrue(oldDelay == null || oldDelay.isDone());
            val result = new CompletableFuture<Void>();
            this.lastDelayFuture.set(result);
            return result;
        }

        @Override
        public void notifyThrottleSourceChanged() {
            this.onNotifyThrottleSourceChanged.run();
        }

        void completeDelayFuture() {
            val delayFuture = this.lastDelayFuture.getAndSet(null);
            Assert.assertNotNull(delayFuture);
            delayFuture.complete(null);
        }

        boolean isCurrentlyThrottling() {
            val d = this.lastDelayFuture.get();
            return d != null && !d.isDone();
        }
    }

    private static class TestThrottler extends Throttler {
        TestThrottler(ThrottlerCalculator.Throttler throttler, Supplier<Boolean> isSuspended, int maxDelay, ScheduledExecutorService executor) {
            super(CONTAINER_ID, ThrottlerCalculator.builder().maxDelayMillis(maxDelay).throttler(throttler).build(),
                    isSuspended, executor, new SegmentStoreMetrics.OperationProcessor(CONTAINER_ID));
        }
    }

    static class MaxDelayThrottler extends ThrottlerCalculator.Throttler {

        private final int maxDelay;

        MaxDelayThrottler(int maxDelay) {
            this.maxDelay = maxDelay;
        }

        @Override
        boolean isThrottlingRequired() {
            return true;
        }

        @Override
        int getDelayMillis() {
            return this.maxDelay;
        }

        @Override
        ThrottlerCalculator.ThrottlerName getName() {
            return ThrottlerCalculator.ThrottlerName.DurableDataLog;
        }
    }

    static class NoOpCalculator extends ThrottlerCalculator.Throttler {
        @Override
        boolean isThrottlingRequired() {
            return false;
        }

        @Override
        int getDelayMillis() {
            return 0;
        }

        @Override
        ThrottlerCalculator.ThrottlerName getName() {
            return ThrottlerCalculator.ThrottlerName.Batching;
        }
    }

    //endregion

    //endregion

    //region ManualAppendOnlyDurableDataLog

    @RequiredArgsConstructor
    private static class ManualAppendOnlyDurableDataLog implements DurableDataLog {
        private final Supplier<CompletableFuture<LogAddress>> addImplementation;

        @Override
        public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
            return this.addImplementation.get();
        }

        //region Not Implemented methods.

        @Override
        public void initialize(Duration timeout) throws DurableDataLogException {

        }

        @Override
        public void enable() {

        }

        @Override
        public void disable() throws DurableDataLogException {

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
        public WriteSettings getWriteSettings() {
            return new WriteSettings(1024 * 1024, Duration.ofMinutes(1), Integer.MAX_VALUE);
        }

        @Override
        public ReadOnlyLogMetadata loadMetadata() throws DataLogInitializationException {
            throw new DataLogInitializationException("Unsupported operation");
        }

        @Override
        public long getEpoch() {
            return 0;
        }

        @Override
        public void overrideEpoch(long epoch) throws DurableDataLogException {
            throw new DataLogInitializationException("Unsupported operation");
        }

        @Override
        public QueueStats getQueueStatistics() {
            return QueueStats.DEFAULT;
        }

        @Override
        public void registerQueueStateChangeListener(ThrottleSourceListener listener) {

        }

        @Override
        public void close() {

        }

        //endregion
    }

    //endregion
}
