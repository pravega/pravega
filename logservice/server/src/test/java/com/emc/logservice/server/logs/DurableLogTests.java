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

import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.StreamSegmentException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.CloseableExecutorService;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.ExceptionHelpers;
import com.emc.logservice.server.IllegalContainerStateException;
import com.emc.logservice.server.ServiceShutdownListener;
import com.emc.logservice.server.TestDurableDataLog;
import com.emc.logservice.server.TestDurableDataLogFactory;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.OperationHelpers;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.reading.ReadIndex;
import com.emc.logservice.storageabstraction.DataLogNotAvailableException;
import com.emc.logservice.storageabstraction.DurableDataLogException;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.mocks.InMemoryDurableDataLogFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryStorage;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.ErrorInjector;
import com.google.common.util.concurrent.Service;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Unit tests for DurableLog class.
 * Note: Some of the tests within this class are very similar to those of OperationProcessorTests. This is because the
 * DurableLog makes heavy use of that class and has the same semantics for the add/append methods. The main difference
 * is that the DurableLog sets up many of the components that the OperationProcessor requires, so we still need to do all
 * these tests.
 */
public class DurableLogTests extends OperationLogTestBase {
    private static final String CONTAINER_ID = "TestContainer";
    private static final int MAX_DATA_LOG_APPEND_SIZE = 8 * 1024;

    //region add() method

    /**
     * Tests the ability of the DurableLog to process Operations in a failure-free environment.
     */
    @Test
    public void testAddWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int batchesPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeBatches = true;
        boolean sealStreamSegments = true;

        // Setup a DurableLog and start it.
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE));
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        @Cleanup
        DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get());
        durableLog.startAsync().awaitRunning();

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        AbstractMap<Long, Long> batches = LogTestHelpers.createBatches(streamSegmentIds, batchesPerStreamSegment, metadata);
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, batches, appendsPerStreamSegment, mergeBatches, sealStreamSegments);

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. If any of them failed, this will fail too and report the exception.
        LogTestHelpers.allOf(completionFutures).join();

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), batches, completionFutures, metadata, mergeBatches, sealStreamSegments);
        performReadIndexChecks(completionFutures, readIndex);

        // Stop the processor.
        System.out.println("waiting to shut down");
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when encountering invalid operations (such as
     * appends to StreamSegments that do not exist or to those that are sealed). This covers the following exceptions:
     * * StreamSegmentNotExistsException
     * * StreamSegmentSealedException
     * * General MetadataUpdateException.
     */
    @Test
    public void testAddWithInvalidOperations() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 40;
        long sealedStreamSegmentId = 6; // We are going to prematurely seal this StreamSegment.
        long deletedStreamSegmentId = 8; // We are going to prematurely mark this StreamSegment as deleted.
        long nonExistentStreamSegmentId; // This is a bogus StreamSegment, that does not exist.

        // Setup a DurableLog and start it.
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE));
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        @Cleanup
        DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get());
        durableLog.startAsync().awaitRunning();

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);
        nonExistentStreamSegmentId = streamSegmentIds.size();
        streamSegmentIds.add(nonExistentStreamSegmentId);
        metadata.getStreamSegmentMetadata(sealedStreamSegmentId).markSealed();
        metadata.getStreamSegmentMetadata(deletedStreamSegmentId).markDeleted();
        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof MetadataUpdateException || ex instanceof StreamSegmentException);

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

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, streamSegmentsWithNoContents, new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when Serialization errors happen.
     */
    @Test
    public void testAddWithOperationSerializationFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAppendFrequency = 7; // Fail every X appends encountered.

        // Setup a DurableLog and start it.
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE));
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        @Cleanup
        DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get());
        durableLog.startAsync().awaitRunning();

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
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

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof IOException);

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

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when there are DataLog write failures.
     */
    @Test
    public void testAddWithDataLogFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failSyncCommitFrequency = 3; // Fail (synchronously) every X DataFrame commits (to DataLog).
        int failAsyncCommitFrequency = 5; // Fail (asynchronously) every X DataFrame commits (to DataLog).

        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();

        // Setup a DurableLog and start it.
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE), dataLog::set);
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        @Cleanup
        DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get());
        durableLog.startAsync().awaitRunning();

        Assert.assertNotNull("Internal error: could not grab a pointer to the created TestDurableDataLog.", dataLog.get());

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);

        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);
        ErrorInjector<Exception> syncErrorInjector = new ErrorInjector<>(
                count -> count % failSyncCommitFrequency == 0,
                () -> new IOException("intentional"));
        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> count % failAsyncCommitFrequency == 0,
                () -> new DurableDataLogException("intentional"));
        dataLog.get().setAppendErrorInjectors(syncErrorInjector, aSyncErrorInjector);

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof IOException || ex instanceof DurableDataLogException);

        performLogOperationChecks(completionFutures, durableLog);
        performMetadataChecks(streamSegmentIds, new HashSet<>(), new HashMap<>(), completionFutures, metadata, false, false);
        performReadIndexChecks(completionFutures, readIndex);

        // Stop the processor.
        durableLog.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability of the DurableLog to process Operations when a simulated DataCorruptionException
     * is generated.
     */
    @Test
    public void testAddWithDataCorruptionFailures() throws Exception {
        int streamSegmentCount = 10;
        int appendsPerStreamSegment = 80;
        int failAfterCommit = 5; // Fail after X DataFrame commits

        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();

        // Setup a DurableLog and start it.
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE), dataLog::set);
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
        @Cleanup
        DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get());
        durableLog.startAsync().awaitRunning();

        Assert.assertNotNull("Internal error: could not grab a pointer to the created TestDurableDataLog.", dataLog.get());

        // Generate some test data (we need to do this after we started the DurableLog because in the process of
        // recovery, it wipes away all existing metadata).
        HashSet<Long> streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata);

        List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);
        ErrorInjector<Exception> aSyncErrorInjector = new ErrorInjector<>(
                count -> count >= failAfterCommit,
                () -> new DataCorruptionException("intentional"));
        dataLog.get().setAppendErrorInjectors(null, aSyncErrorInjector);

        // Process all generated operations.
        List<LogTestHelpers.OperationWithCompletion> completionFutures = processOperations(operations, durableLog);

        // Wait for all such operations to complete. We are expecting exceptions, so verify that we do.
        AssertExtensions.assertThrows(
                "No operations failed.",
                LogTestHelpers.allOf(completionFutures)::join,
                ex -> ex instanceof DataCorruptionException);

        // Wait for the service to fail (and make sure it failed).
        AssertExtensions.assertThrows(
                "DurableLog did not shut down with failure.",
                () -> ServiceShutdownListener.awaitShutdown(durableLog, true),
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("Unexpected service state after encountering DataCorruptionException.", Service.State.FAILED, durableLog.state());

        // Verify that the "right" operations failed, while the others succeeded.
        int successCount = 0;
        boolean encounteredFirstFailure = false;
        for (int i = 0; i < completionFutures.size(); i++) {
            LogTestHelpers.OperationWithCompletion oc = completionFutures.get(i);

            // Once an operation failed (in our scenario), no other operation can succeed.
            if (encounteredFirstFailure) {
                Assert.assertTrue("Encountered successful operation after a failed operation.", oc.completion.isCompletedExceptionally());
            }

            // The operation that failed may have inadvertently failed other operations that were aggregated together
            // with it, which is why it's hard to determine precisely what the first expected failed operation is.
            if (oc.completion.isCompletedExceptionally()) {
                // If we do find a failed one in this area, make sure it is failed with DataCorruptionException.
                AssertExtensions.assertThrows(
                        "Unexpected exception for failed Operation.",
                        oc.completion::join,
                        ex -> ex instanceof DataCorruptionException
                                || ex instanceof IllegalContainerStateException
                                || (ex instanceof IOException && (ex.getCause() instanceof DataCorruptionException || ex.getCause() instanceof IllegalContainerStateException)));
                encounteredFirstFailure = true;
            } else {
                successCount++;
            }
        }

        AssertExtensions.assertGreaterThan("No operation succeeded.", 0, successCount);

        // There is no point in performing any other checks. A DataCorruptionException means the Metadata (and the general
        // state of the Container) is in an undefined state.
    }

    //endregion

    //region Recovery

    /**
     * Tests the DurableLog recovery process in a scenario when there are no failures during the process.
     */
    @Test
    public void testRecoveryWithNoFailures() throws Exception {
        int streamSegmentCount = 50;
        int batchesPerStreamSegment = 2;
        int appendsPerStreamSegment = 20;
        boolean mergeBatches = true;
        boolean sealStreamSegments = true;

        // Setup a DurableLog and start it.
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE));
        @Cleanup
        Storage storage = new InMemoryStorage();
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);

        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));

        HashSet<Long> streamSegmentIds;
        AbstractMap<Long, Long> batches;
        List<LogTestHelpers.OperationWithCompletion> completionFutures;
        List<Operation> originalOperations;

        // First DurableLog. We use this for generating data.
        try (
                Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
                DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata, durableLog, storage);
            batches = LogTestHelpers.createBatches(streamSegmentIds, batchesPerStreamSegment, metadata, durableLog, storage);
            List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, batches, appendsPerStreamSegment, mergeBatches, sealStreamSegments);

            // Process all generated operations and wait for them to complete
            completionFutures = processOperations(operations, durableLog);
            LogTestHelpers.allOf(completionFutures).join();

            // Get a list of all the operations, before recovery.
            originalOperations = readAllDurableLog(durableLog);

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        // Second DurableLog. We use this for recovery.
        metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        try (
                Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
                DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get())) {
            durableLog.startAsync().awaitRunning();

            List<Operation> recoveredOperations = readAllDurableLog(durableLog);
            AssertExtensions.assertListEquals("Recovered operations do not match original ones.", originalOperations, recoveredOperations, OperationHelpers::assertEquals);
            performMetadataChecks(streamSegmentIds, new HashSet<>(), batches, completionFutures, metadata, mergeBatches, sealStreamSegments);
            performReadIndexChecks(completionFutures, readIndex);

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }
    }

    /**
     * Tests the DurableLog recovery process in a scenario when there are failures during the process
     * (these may or may not be DataCorruptionExceptions).
     */
    @Test
    public void testRecoveryFailures() throws Exception {
        int streamSegmentCount = 50;
        int appendsPerStreamSegment = 20;
        int failReadAfter = 2;// Fail DataLog reads after X reads.

        // Setup a DurableLog and start it.
        AtomicReference<TestDurableDataLog> dataLog = new AtomicReference<>();
        @Cleanup
        TestDurableDataLogFactory dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE), dataLog::set);
        @Cleanup
        Storage storage = new InMemoryStorage();
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        @Cleanup
        CloseableExecutorService executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(10));

        HashSet<Long> streamSegmentIds;
        List<LogTestHelpers.OperationWithCompletion> completionFutures;

        // First DurableLog. We use this for generating data.
        try (
                Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
                DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get())) {
            durableLog.startAsync().awaitRunning();

            // Generate some test data (we need to do this after we started the DurableLog because in the process of
            // recovery, it wipes away all existing metadata).
            streamSegmentIds = LogTestHelpers.createStreamSegments(streamSegmentCount, metadata, durableLog, storage);
            List<Operation> operations = LogTestHelpers.generateOperations(streamSegmentIds, new HashMap<>(), appendsPerStreamSegment, false, false);

            // Process all generated operations and wait for them to complete
            completionFutures = processOperations(operations, durableLog);
            LogTestHelpers.allOf(completionFutures).join();

            // Stop the processor.
            durableLog.stopAsync().awaitTerminated();
        }

        //Recovery failure due to DataLog Failures.
        metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        dataLog.set(null);
        try (
                Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
                DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get())) {

            // Inject some artificial error into the DataLogRead after a few reads.
            ErrorInjector<Exception> readNextInjector = new ErrorInjector<>(
                    count -> count > failReadAfter,
                    () -> new DataLogNotAvailableException("intentional"));
            dataLog.get().setReadErrorInjectors(null, readNextInjector);

            // Verify the exception thrown from startAsync() is of the right kind. This exception will be wrapped in
            // multiple layers, so we need to dig deep into it.
            AssertExtensions.assertThrows(
                    "Recovery did not fail properly when expecting DurableDataLogException.",
                    () -> durableLog.startAsync().awaitRunning(),
                    ex -> {
                        if (ex instanceof IllegalStateException) {
                            ex = ex.getCause();
                        }

                        ex = ExceptionHelpers.getRealException(ex);
                        return ex instanceof DataLogNotAvailableException && ex.getMessage().equals("intentional");
                    });
        }

        // Recovery failure due to DataCorruption
        metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        dataLog.set(null);
        try (
                Cache readIndex = new ReadIndex(metadata, CONTAINER_ID);
                DurableLog durableLog = new DurableLog(metadata, dataLogFactory, readIndex, executorService.get())) {

            // Reset error injectors to nothing.
            dataLog.get().setReadErrorInjectors(null, null);
            AtomicInteger readCounter = new AtomicInteger();
            dataLog.get().setReadInterceptor(
                    readItem -> {
                        byte[] payload = readItem.getPayload();
                        if (readCounter.incrementAndGet() > failReadAfter && payload.length > 14) { // 14 == DataFrame.Header.Length
                            // Mangle with the payload and overwrite its contents with a DataFrame having a bogus
                            // previous sequence number.
                            DataFrame df = new DataFrame(Integer.MAX_VALUE, payload.length);
                            df.seal();
                            try {
                                StreamHelpers.readAll(df.getData(), payload, 0, payload.length);
                            } catch (Exception ex) {
                                Assert.fail(ex.toString());
                            }
                        }
                    }
            );

            // Verify the exception thrown from startAsync() is of the right kind. This exception will be wrapped in
            // multiple layers, so we need to dig deep into it.
            AssertExtensions.assertThrows(
                    "Recovery did not fail properly when expecting DataCorruptionException.",
                    () -> durableLog.startAsync().awaitRunning(),
                    ex -> {
                        if (ex instanceof IllegalStateException) {
                            ex = ex.getCause();
                        }

                        return ExceptionHelpers.getRealException(ex) instanceof DataCorruptionException;
                    });
        }
    }

    //endregion

    /**
     * Tests the truncate() method prior to recovering the DurableLog.
     */
    @Test
    public void testTruncatePreRecovery() {

    }

    /**
     * Tests the truncate() method after recovering the DurableLog.
     */
    @Test
    public void testTruncatePostRecovery() {

    }

    private void performLogOperationChecks(Collection<LogTestHelpers.OperationWithCompletion> operations, DurableLog durableLog) throws Exception {
        // Log Operation based checks
        long lastSeqNo = -1;
        Iterator<Operation> logIterator = durableLog.read(-1L, operations.size() + 1, TIMEOUT).join();
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
            Assert.assertTrue("No more items left to read from DurableLog. Expected: " + expectedOp, logIterator.hasNext());
            Assert.assertEquals("Unexpected Operation in MemoryLog.", expectedOp, logIterator.next()); // Ok to use assertEquals because we are actually expecting the same object here.
        }
    }

    private List<Operation> readAllDurableLog(OperationLog durableLog) {
        ArrayList<Operation> result = new ArrayList<>();
        Iterator<Operation> logIterator = durableLog.read(-1L, Integer.MAX_VALUE, TIMEOUT).join();
        while (logIterator.hasNext()) {
            result.add(logIterator.next());
        }

        return result;
    }

    private List<LogTestHelpers.OperationWithCompletion> processOperations(Collection<Operation> operations, DurableLog durableLog) {
        List<LogTestHelpers.OperationWithCompletion> completionFutures = new ArrayList<>();
        for (Operation o : operations) {
            CompletableFuture<Long> completionFuture;
            try {
                completionFuture = durableLog.add(o, TIMEOUT);
            } catch (Exception ex) {
                completionFuture = FutureHelpers.failedFuture(ex);
            }

            completionFutures.add(new LogTestHelpers.OperationWithCompletion(o, completionFuture));
        }

        return completionFutures;
    }
}
