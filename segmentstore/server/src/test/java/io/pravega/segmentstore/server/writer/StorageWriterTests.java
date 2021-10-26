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
package io.pravega.segmentstore.server.writer;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.EvictableMetadata;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.ServiceListeners;
import io.pravega.segmentstore.server.TestStorage;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the StorageWriter class.
 */
public class StorageWriterTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1;
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 1000;
    private static final int UPDATE_ATTRIBUTES_PER_SEGMENT = 50;
    private static final int APPENDS_PER_SEGMENT_RECOVERY = 500; // We use depth-first, which has slower performance.
    private static final int METADATA_CHECKPOINT_FREQUENCY = 50;
    private static final AttributeId CORE_ATTRIBUTE_ID = Attributes.EVENT_COUNT;
    private static final List<AttributeId> EXTENDED_ATTRIBUTE_IDS = Arrays.asList(AttributeId.randomUUID(), AttributeId.randomUUID(), AttributeId.randomUUID());
    private static final WriterConfig DEFAULT_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1000)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .with(WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE, 100)
            .with(WriterConfig.ERROR_SLEEP_MILLIS, 0L)
            .build();

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests a normal, happy case, when the Writer needs to process operations in the "correct" order, from a DataSource
     * that does not produce any errors (i.e., Timeouts) and to a Storage that works perfectly.
     * See testWriter() for more details about testing flow.
     */
    @Test
    public void testNormalFlow() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a scenario where the DataSource throws random exceptions. Simulated errors are for
     * the following operations:
     * * Read (sync + async)
     * * Acknowledge (sync + async)
     * * GetAppendData (sync)
     */
    @Test
    public void testWithDataSourceTransientErrors() throws Exception {
        final int failReadSyncEvery = 2;
        final int failReadAsyncEvery = 3;
        final int failAckSyncEvery = 2;
        final int failAckAsyncEvery = 3;
        final int failGetAppendDataEvery = 99;
        final int failPersistAttributeEvery = 11;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        Supplier<Exception> exceptionSupplier = IntentionalException::new;

        // Simulated Read errors.
        context.dataSource.setReadSyncErrorInjector(new ErrorInjector<>(count -> count % failReadSyncEvery == 0, exceptionSupplier));
        context.dataSource.setReadAsyncErrorInjector(new ErrorInjector<>(count -> count % failReadAsyncEvery == 0, exceptionSupplier));

        // Simulated ack/truncate errors.
        context.dataSource.setAckSyncErrorInjector(new ErrorInjector<>(count -> count % failAckSyncEvery == 0, exceptionSupplier));
        context.dataSource.setAckAsyncErrorInjector(new ErrorInjector<>(count -> count % failAckAsyncEvery == 0, exceptionSupplier));

        // Simulated data retrieval errors.
        context.dataSource.setGetAppendDataErrorInjector(new ErrorInjector<>(count -> count % failGetAppendDataEvery == 0, exceptionSupplier));

        // Simulated attribute persistence errors
        context.dataSource.setPersistAttributesErrorInjector(new ErrorInjector<>(count -> count % failPersistAttributeEvery == 0, exceptionSupplier));

        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a scenario where some sort of fatal error occurs from the DataSource. In this case,
     * the cache lookup for a CachedStreamSegmentAppendOperation returns null.
     */
    @Test
    public void testWithDataSourceFatalErrors() {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);

        // We clear up the cache after we have added the operations in the data source - this will cause the writer
        // to pick them up and end up failing when attempting to fetch the cache contents.
        context.dataSource.clearAppendData();

        AssertExtensions.assertThrows(
                "StorageWriter did not fail when a fatal data retrieval error occurred.",
                () -> {
                    // The Corruption may happen early enough so the "awaitRunning" isn't complete yet. In that case,
                    // the writer will never reach its 'Running' state. As such, we need to make sure at least one of these
                    // will throw (either start or, if the failure happened after start, make sure it eventually fails and shuts down).
                    context.writer.startAsync().awaitRunning();
                    ServiceListeners.awaitShutdown(context.writer, TIMEOUT, true);
                },
                ex -> ex instanceof IllegalStateException);

        // The previous error could be thrown either while starting or while shutting down. In case of the former,
        // we should wait until the writer is properly terminated.
        AssertExtensions.assertThrows(
                "StorageWriter did not fail when a fatal data retrieval error occurred.",
                () -> ServiceListeners.awaitShutdown(context.writer, TIMEOUT, true),
                ex -> ex instanceof IllegalStateException);

        Assert.assertTrue("Unexpected failure cause for StorageWriter: " + context.writer.failureCause(), Exceptions.unwrap(context.writer.failureCause()) instanceof DataCorruptionException);
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws non-corruption exceptions (i.e., not badOffset).
     */
    @Test
    public void testWithStorageTransientErrors() throws Exception {
        final int failWriteSyncEvery = 4;
        final int failWriteAsyncEvery = 6;
        final int failSealSyncEvery = 4;
        final int failSealAsyncEvery = 6;
        final int failConcatAsyncEvery = 6;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        Supplier<Exception> exceptionSupplier = IntentionalException::new;

        // Simulated Write errors.
        context.storage.setWriteSyncErrorInjector(new ErrorInjector<>(count -> count % failWriteSyncEvery == 0, exceptionSupplier));
        context.storage.setWriteAsyncErrorInjector(new ErrorInjector<>(count -> count % failWriteAsyncEvery == 0, exceptionSupplier));

        // Simulated Seal errors.
        context.storage.setSealSyncErrorInjector(new ErrorInjector<>(count -> count % failSealSyncEvery == 0, exceptionSupplier));
        context.storage.setSealAsyncErrorInjector(new ErrorInjector<>(count -> count % failSealAsyncEvery == 0, exceptionSupplier));

        // Simulated Concat errors.
        context.storage.setConcatAsyncErrorInjector(new ErrorInjector<>(count -> count % failConcatAsyncEvery == 0, exceptionSupplier));

        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a scenario where the DurableLog is left throwing DataLogWriterNotPrimaryException. In
     * this case, we need to restart the StorageWriter and expect the container recovery to resolve the original issue.
     */
    @Test
    public void testWithDataSourceNotPrimaryErrors() throws Exception {
        final int failWriteSyncEvery = 4;
        Predicate<Throwable> errorPredicate = ex -> ex instanceof DataLogWriterNotPrimaryException;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        Supplier<Exception> exceptionSupplier = () -> new DataLogWriterNotPrimaryException("Problem interacting with Zookeeper");

        // Simulate DataLogWriterNotPrimaryException in DurableLog from a failed truncation (e.g., KeeperException.BadVersionException).
        context.dataSource.setAckAsyncErrorInjector(new ErrorInjector<>(count -> count % failWriteSyncEvery == 0, exceptionSupplier));

        // Start the writer.
        context.writer.startAsync();

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        ArrayList<Long> transactionIds = new ArrayList<>();
        transactionsBySegment.values().forEach(transactionIds::addAll);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);
        appendDataBreadthFirst(transactionIds, segmentContents, context);

        // Wait for the Storage Writer to be shutdown.
        ServiceListeners.awaitShutdown(context.writer, TIMEOUT, false);

        // Ensure that the failure case is DataLogWriterNotPrimaryException.
        Assert.assertTrue("Unexpected failure cause for DurableLog.",
                errorPredicate.test(Exceptions.unwrap(context.writer.failureCause())));
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws data corruption exceptions (i.e., badOffset,
     * and after reconciliation, the data is still corrupt).
     */
    @Test
    public void testWithStorageCorruptionErrors() throws Exception {
        AtomicBoolean corruptionHappened = new AtomicBoolean();
        Function<TestContext, ErrorInjector<Exception>> createErrorInjector = context -> {
            byte[] corruptionData = "foo".getBytes();
            SegmentHandle corruptedSegmentHandle = InMemoryStorage.newHandle(context.metadata.getStreamSegmentMetadata(0).getName(), false);
            Supplier<Exception> exceptionSupplier = () -> {
                // Corrupt data. We use an internal method (append) to atomically write data at the end of the segment.
                // GetLength+Write would not work well because there may be concurrent writes that modify the data between
                // requesting the length and attempting to write, thus causing the corruption to fail.
                // NOTE: this is a synchronous call, but append() is also a sync method. If append() would become async,
                // care must be taken not to block a thread while waiting for it.
                context.storage.append(corruptedSegmentHandle, new ByteArrayInputStream(corruptionData), corruptionData.length);

                // Return some other kind of exception.
                return new TimeoutException("Intentional");
            };
            return new ErrorInjector<>(c -> !corruptionHappened.getAndSet(true), exceptionSupplier);
        };

        testWithStorageCriticalErrors(createErrorInjector, ex -> ex instanceof ReconciliationFailureException);
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component reports that it is no longer the primary owner
     * of a particular segment (it was fenced out).
     */
    @Test
    public void testWithStorageNotPrimaryErrors() throws Exception {
        testWithStorageCriticalErrors(
                context -> new ErrorInjector<>(c -> true, () -> new StorageNotPrimaryException("intentional")),
                ex -> ex instanceof StorageNotPrimaryException);
    }

    /**
     * Tests the StorageWriter in a configurable scenario where the Storage component throws a critical (container-stopper)
     * exception and verifies its handling of the situation.
     *
     * @param createErrorInjector          Creates an ErrorInjector that will cause the Storage component to enter an
     *                                     errored state and throw an exception back at the StorageWriter.
     * @param validatePostFailureException Validates that the {@link StorageWriter#failureCause()} is set correctly after
     *                                     the StorageWriter terminates with failure.
     */
    private void testWithStorageCriticalErrors(Function<TestContext, ErrorInjector<Exception>> createErrorInjector,
                                               Predicate<Throwable> validatePostFailureException) throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);

        // We only try to corrupt data once.
        context.storage.setWriteAsyncErrorInjector(createErrorInjector.apply(context));
        AssertExtensions.assertThrows(
                "StorageWriter did not fail when critical error occurred.",
                () -> {
                    // The critical error may happen early enough so the "awaitRunning" isn't complete yet. In that case,
                    // the writer will never reach its 'Running' state. As such, we need to make sure at least one of these
                    // will throw (either start or, if the failure happened after start, make sure it eventually fails and shuts down).
                    context.writer.startAsync().awaitRunning();
                    ServiceListeners.awaitShutdown(context.writer, TIMEOUT, true);
                },
                ex -> ex instanceof IllegalStateException);

        ServiceListeners.awaitShutdown(context.writer, TIMEOUT, false);
        Assert.assertTrue("Unexpected failure cause for StorageWriter.",
                validatePostFailureException.test(Exceptions.unwrap(context.writer.failureCause())));
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws data corruption exceptions (i.e., badOffset),
     * but after reconciliation, the data is not corrupt.
     */
    @Test
    public void testWithStorageRecoverableCorruptionErrors() throws Exception {
        final int failWriteEvery = 3;
        final int failSealEvery = 7;
        final int failMergeEvery = 5;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Inject write errors every now and then.
        AtomicInteger writeCount = new AtomicInteger();
        AtomicInteger writeFailCount = new AtomicInteger();
        context.storage.setWriteInterceptor((segmentName, offset, data, length, storage) -> {
            if (writeCount.incrementAndGet() % failWriteEvery == 0) {
                return storage.write(InMemoryStorage.newHandle(segmentName, false), offset, data, length, TIMEOUT)
                              .thenRun(() -> {
                                  writeFailCount.incrementAndGet();
                                  throw new IntentionalException(String.format("S=%s,O=%d,L=%d", segmentName, offset, length));
                              });
            }

            return null;
        });

        // Inject Seal errors every now and then.
        AtomicInteger sealCount = new AtomicInteger();
        AtomicInteger sealFailCount = new AtomicInteger();
        context.storage.setSealInterceptor((segmentName, storage) -> {
            if (sealCount.incrementAndGet() % failSealEvery == 0) {
                return storage.seal(InMemoryStorage.newHandle(segmentName, false), TIMEOUT)
                              .thenRun(() -> {
                                  sealFailCount.incrementAndGet();
                                  throw new IntentionalException(String.format("S=%s", segmentName));
                              });
            }

            return null;
        });

        // Inject Merge/Concat errors every now and then.
        AtomicInteger mergeCount = new AtomicInteger();
        AtomicInteger mergeFailCount = new AtomicInteger();
        context.storage.setConcatInterceptor((targetSegment, offset, sourceSegment, storage) -> {
            if (mergeCount.incrementAndGet() % failMergeEvery == 0) {
                return storage.concat(InMemoryStorage.newHandle(targetSegment, false), offset, sourceSegment, TIMEOUT)
                              .thenRun(() -> {
                                  mergeFailCount.incrementAndGet();
                                  throw new IntentionalException(String.format("T=%s,O=%d,S=%s", targetSegment, offset, sourceSegment));
                              });
            }

            return null;
        });

        testWriter(context);

        AssertExtensions.assertGreaterThan("Not enough writes were made for this test.", 0, writeCount.get());
        AssertExtensions.assertGreaterThan("Not enough write failures happened for this test.", 0, writeFailCount.get());

        AssertExtensions.assertGreaterThan("Not enough seals were made for this test.", 0, sealCount.get());
        AssertExtensions.assertGreaterThan("Not enough seal failures happened for this test.", 0, sealFailCount.get());

        AssertExtensions.assertGreaterThan("Not enough mergers were made for this test.", 0, mergeCount.get());
        AssertExtensions.assertGreaterThan("Not enough merge failures happened for this test.", 0, mergeFailCount.get());
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws arbitrary exceptions when requesting
     * information about a segment. This simulates the Segment Aggregators failing to initialize and we ensure that the
     * StorageWriter is resilient enough to handle this situation.
     */
    @Test
    public void testWithSegmentAggregatorInitErrors() throws Exception {
        final int failGetEvery = 2; // Fail very frequently - we want this tested well.

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.writer.startAsync();

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);

        Supplier<Exception> exceptionSupplier = IntentionalException::new;
        context.storage.setGetErrorInjector(new ErrorInjector<>(count -> count % failGetEvery == 0, exceptionSupplier));

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);

        // Truncate half of the remaining segments, then seal all, then truncate all segments.
        metadataCheckpoint(context);

        // Wait for the writer to complete its job.
        val writerStopped = new CompletableFuture<Void>();
        Services.onStop(context.writer, () -> writerStopped.complete(null), writerStopped::completeExceptionally, executorService());
        val fullyAck = context.dataSource.waitFullyAcked();

        // Stop quickly if the writer fails. If not wait until fully acked.
        CompletableFuture.anyOf(writerStopped, fullyAck).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // If the writer stopped prematurely, it should only be because it failed, so we should never get in here.
        // However, just for sanity reasons, ensure that we have fully acked to the data source, as that is a prerequisite
        // for verifying final output.
        fullyAck.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify final output (and clear out any error injectors since we are done using the writer at this point).
        context.storage.setGetErrorInjector(null);
        verifyFinalOutput(segmentContents, Collections.emptyList(), context);
    }

    /**
     * Tests the StorageWriter in a Scenario where it needs to gracefully recover from a Container failure, and not all
     * previously written data has been acknowledged to the DataSource.
     * General test flow:
     * 1. Starts a writer, and adds a subset of all operations. All acks to the DataSource are not processed
     * 2. Stops the writer, adds the remaining data, and restarts the writer (with acks now processing)
     * 3. Restarts the writer, and waits for it to finish, after which the final data is inspected.
     */
    @Test
    public void testRecovery() throws Exception {
        // Start a writer, and add all operations, both overlapping the already committed data, and for the new data.
        // At the end, verify everything is ack-ed and in Storage as it should be.

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.dataSource.setAckEffective(false); // Disable ack-ing.
        context.writer.startAsync();

        // Create a bunch of segments and Transaction.
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        ArrayList<Long> transactionIds = new ArrayList<>();
        transactionsBySegment.values().forEach(transactionIds::addAll);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Parent segments have 50% of data written
        appendDataDepthFirst(segmentIds, segmentId -> APPENDS_PER_SEGMENT_RECOVERY / 2, segmentContents, context);

        List<Long> firstThirdTransactions = transactionIds.subList(0, transactionIds.size() / 3);
        List<Long> secondThirdTransactions = transactionIds.subList(transactionIds.size() / 3, transactionIds.size() * 2 / 3);
        List<Long> lastThirdTransactions = transactionIds.subList(transactionIds.size() * 2 / 3, transactionIds.size());

        // First and second 1/3 of Transactions have full data.
        appendDataDepthFirst(firstThirdTransactions, segmentId -> APPENDS_PER_SEGMENT_RECOVERY, segmentContents, context);
        appendDataDepthFirst(secondThirdTransactions, segmentId -> APPENDS_PER_SEGMENT_RECOVERY, segmentContents, context);

        // Third 1/3 of Transactions have 50% of data.
        appendDataDepthFirst(lastThirdTransactions, segmentId -> APPENDS_PER_SEGMENT_RECOVERY / 2, segmentContents, context);

        // First 1/3 of Transactions are merged into parent.
        sealSegments(firstThirdTransactions, context);
        mergeTransactions(firstThirdTransactions, segmentContents, context);

        // Second 1/3 of Transactions are sealed, but not merged into parent.
        sealSegments(secondThirdTransactions, context);

        // Wait for the writer to complete its job.
        metadataCheckpoint(context);
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // At this point the storage should mimic the setup we created above, yet the WriterDataSource still has all the
        // original operations in place. Stop the writer, add the rest of the operations to the DataSource, then restart the writer.
        context.writer.stopAsync().awaitTerminated();
        context.dataSource.setAckEffective(true);

        // Add the last 50% of data to the parent segments.
        appendDataDepthFirst(segmentIds, segmentId -> APPENDS_PER_SEGMENT_RECOVERY / 2, segmentContents, context);

        // Seal & merge second 1/3 of Transactions.
        sealSegments(secondThirdTransactions, context);
        mergeTransactions(secondThirdTransactions, segmentContents, context);

        // Add remaining data, seal & merge last 1/3 of Transactions.
        appendDataDepthFirst(lastThirdTransactions, segmentId -> APPENDS_PER_SEGMENT_RECOVERY / 2, segmentContents, context);
        sealSegments(lastThirdTransactions, context);
        mergeTransactions(lastThirdTransactions, segmentContents, context);

        // Seal the parents.
        sealSegments(segmentIds, context);
        metadataCheckpoint(context);

        // Restart the writer (restart a new one, to clear out any in-memory state).
        // Note that this also changes the storage owner, which verifies that the writer correctly reacquires ownership.
        context.resetWriter();
        context.writer.startAsync().awaitRunning();
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify final output.
        verifyFinalOutput(segmentContents, transactionIds, context);
    }

    /**
     * Tests the ability of the StorageWriter to cleanup SegmentAggregators that have been deleted in Storage or are
     * gone from the Metadata.
     * 1. Creates 3 segments, and adds an append for each of them.
     * 2. Marks segment 2 as deleted (in metadata) and evicts segment 3 from metadata (no deletion).
     * 3. Runs one more Writer cycle (to clean up).
     * 4. Reinstates the missing segment metadatas and adds appends for each of them, verifying that the Writer re-requests
     * the metadata for those two.
     */
    @Test
    public void testCleanup() throws Exception {
        final WriterConfig config = WriterConfig.builder()
                                                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1) // This differs from DEFAULT_CONFIG.
                                                .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 1)
                                                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                                                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                                                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                                                .with(WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE, 100)
                                                .with(WriterConfig.ERROR_SLEEP_MILLIS, 0L)
                                                .build();
        @Cleanup
        TestContext context = new TestContext(config);
        context.writer.startAsync();

        // Create a bunch of segments and Transaction.
        final ArrayList<Long> segmentIds = createSegments(context);
        final UpdateableSegmentMetadata segment1 = context.metadata.getStreamSegmentMetadata(segmentIds.get(0));
        final UpdateableSegmentMetadata segment2 = context.metadata.getStreamSegmentMetadata(segmentIds.get(1));
        final UpdateableSegmentMetadata segment3 = context.metadata.getStreamSegmentMetadata(segmentIds.get(2));
        final byte[] data = new byte[1];

        Function<UpdateableSegmentMetadata, Operation> createAppend = segment -> {
            StreamSegmentAppendOperation append = new StreamSegmentAppendOperation(segment.getId(), new ByteArraySegment(data), null);
            append.setStreamSegmentOffset(segment.getLength());
            context.dataSource.recordAppend(append);
            segment.setLength(segment.getLength() + data.length);
            return new CachedStreamSegmentAppendOperation(append);
        };

        // Process an append for each segment, to make sure the writer has knowledge of those segments.
        context.dataSource.add(createAppend.apply(segment1));
        context.dataSource.add(createAppend.apply(segment2));
        context.dataSource.add(createAppend.apply(segment3));
        metadataCheckpoint(context);
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Delete segment2 (markDeleted) and evict segment3 (by forcing the metadata to forget about it).
        long evictionCutoff = context.metadata.nextOperationSequenceNumber() + 1;
        context.metadata.getStreamSegmentId(segment1.getName(), true);
        context.metadata.getStreamSegmentId(segment2.getName(), true);
        segment2.markDeleted();
        Collection<Long> evictedSegments = evictSegments(evictionCutoff, context);

        // Make sure the right segment is evicted, and not the other two ones (there are other segments in this system which we don't care about).
        Assert.assertTrue("Expected segment was not evicted.", evictedSegments.contains(segment3.getId()));
        Assert.assertFalse("Unexpected segments were not evicted.", evictedSegments.contains(segment1.getId()) && evictedSegments.contains(segment3.getId()));

        // Add one more append to Segment1 - this will force the writer to go on a full iteration and thus invoke cleanup.
        context.dataSource.add(createAppend.apply(segment1));
        metadataCheckpoint(context);
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Get rid of Segment2 from the metadata.
        evictionCutoff = context.metadata.nextOperationSequenceNumber() + 1;
        context.metadata.getStreamSegmentId(segment1.getName(), true);
        evictedSegments = evictSegments(evictionCutoff, context);
        Assert.assertTrue("Expected segment was not evicted.", evictedSegments.contains(segment2.getId()));

        // Repopulate the metadata.
        val segment2Take2 = context.metadata.mapStreamSegmentId(segment2.getName(), segment2.getId());
        val segment3Take2 = context.metadata.mapStreamSegmentId(segment3.getName(), segment3.getId());
        segment2Take2.copyFrom(segment2);
        segment3Take2.copyFrom(segment3);

        // Add an append for each of the re-added segments and verify that the Writer re-requested the metadata, which
        // indicates it had to recreate their SegmentAggregators.
        HashSet<Long> requestedSegmentIds = new HashSet<>();
        context.dataSource.setSegmentMetadataRequested(requestedSegmentIds::add);
        context.dataSource.add(createAppend.apply(segment2Take2));
        context.dataSource.add(createAppend.apply(segment3Take2));
        metadataCheckpoint(context);
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        Assert.assertTrue("The deleted segments did not have their metadata requested.",
                requestedSegmentIds.contains(segment2.getId()) && requestedSegmentIds.contains(segment3.getId()));
    }

    /**
     * Tests the StorageWriter with multiple Segment Processors.
     */
    @Test
    public void testMultipleProcessors() throws Exception {
        final int processorCount = 2;
        val processors = new HashMap<Long, ArrayList<WriterSegmentProcessor>>();

        WriterFactory.CreateProcessors createProcessors = sm -> {
            Assert.assertFalse("Processors already created for segment " + sm, processors.containsKey(sm.getId()));
            val result = new ArrayList<WriterSegmentProcessor>();
            for (int i = 0; i < processorCount; i++) {
                val p = new TestWriterProcessor();
                result.add(p);
            }

            processors.put(sm.getId(), result);
            return result;
        };
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, createProcessors);

        context.writer.startAsync();

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        ArrayList<Long> transactionIds = new ArrayList<>();
        transactionsBySegment.values().forEach(transactionIds::addAll);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);
        appendDataBreadthFirst(transactionIds, segmentContents, context);
        sealSegments(transactionIds, context);
        mergeTransactions(transactionIds, segmentContents, context);
        sealSegments(segmentIds, context);

        // Wait for the writer to complete its job.
        metadataCheckpoint(context);
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify the processors.
        for (val e : processors.entrySet()) {
            // Verify they received the same operations.
            val firstProcessor = (TestWriterProcessor) e.getValue().get(0);
            AssertExtensions.assertGreaterThan("Expected at least one operation.", 0, firstProcessor.operations.size());
            for (int i = 1; i < e.getValue().size(); i++) {
                AssertExtensions.assertListEquals("Not all processors for the same segment received the same operations.",
                        firstProcessor.operations, ((TestWriterProcessor) e.getValue().get(i)).operations, Object::equals);
            }

            // Verify they have all been flushed.
            for (val p : e.getValue()) {
                Assert.assertEquals("Not all processors were flushed.", -1, p.getLowestUncommittedSequenceNumber());
            }
        }

        // Verify that the main Segment Aggregators were still able to do their jobs.
        verifyFinalOutput(segmentContents, transactionIds, context);
        context.writer.close();
        for (val e : processors.entrySet()) {
            for (val p : e.getValue()) {
                Assert.assertTrue("Not all processors were closed.", p.isClosed());
            }
        }
    }

    /**
     * Tests the {@link StorageWriter#forceFlush} method.
     */
    @Test
    public void testForceFlush() throws Exception {
        // Special config that increases the flush thresholds and keeps the read timeout to something manageable (for empty reads).
        val config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1024 * 1024)
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 5000L)
                .with(WriterConfig.MAX_ITEMS_TO_READ_AT_ONCE, 100)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                .build();
        @Cleanup
        TestContext context = new TestContext(config);
        context.writer.startAsync();

        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);

        // Do not queue up a checkpoint - make it impossible for the Writer to acknowledge anything on its own path.
        // Instead, force-flush and verify the output when done.
        val ff1 = context.writer.forceFlush(context.metadata.getOperationSequenceNumber(), TIMEOUT);
        val result1 = ff1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Expected something to be flushed.", result1);

        // Verify result.
        verifyFinalOutput(segmentContents, Collections.emptyList(), context);

        // Do a second force-flush. At this point there should be nothing left to flush, so ensure the StorageWriter
        // cannot write to Storage or update attributes.
        context.storage.close();
        context.dataSource.setPersistAttributesErrorInjector(new ErrorInjector<>(i -> true, Exception::new));
        val ff2 = context.writer.forceFlush(context.metadata.getOperationSequenceNumber(), TIMEOUT);
        val result2 = ff2.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertFalse("Not expected anything to be flushed the second time.", result2);
    }

    /**
     * Tests the writer as it is setup in the given context.
     * General test flow:
     * 1. Add Appends (Cached/non-cached) to both Parent and Transaction segments
     * 2. Seal and merge the Transactions
     * 3. Seal the parent segments.
     * 4. Wait for everything to be ack-ed and check the result.
     *
     * @param context The TestContext to use.
     */
    private void testWriter(TestContext context) throws Exception {
        context.writer.startAsync();

        // Create a bunch of segments and Transactions.
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        ArrayList<Long> transactionIds = new ArrayList<>();
        transactionsBySegment.values().forEach(transactionIds::addAll);

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendDataBreadthFirst(segmentIds, segmentContents, context);
        appendDataBreadthFirst(transactionIds, segmentContents, context);

        // Merge Transaction.
        sealSegments(transactionIds, context);
        mergeTransactions(transactionIds, segmentContents, context);

        // Truncate half of the remaining segments, then seal all, then truncate all segments.
        truncateSegments(segmentIds.subList(0, segmentIds.size() / 2), context);
        sealSegments(segmentIds, context);
        truncateSegments(segmentIds, context);
        metadataCheckpoint(context);

        // Wait for the writer to complete its job.
        context.dataSource.waitFullyAcked().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify final output (and clear out any error injectors since we are done using the writer at this point).
        context.dataSource.setAckAsyncErrorInjector(null);
        context.dataSource.setAckSyncErrorInjector(null);
        context.dataSource.setGetAppendDataErrorInjector(null);
        context.dataSource.setPersistAttributesErrorInjector(null);
        context.dataSource.setReadAsyncErrorInjector(null);
        context.dataSource.setReadSyncErrorInjector(null);
        verifyFinalOutput(segmentContents, transactionIds, context);
    }

    private void truncateSegments(Collection<Long> segmentIds, TestContext context) {
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
            long truncationOffset = (segmentMetadata.getStartOffset() + segmentMetadata.getLength()) / 2;
            segmentMetadata.setStartOffset(truncationOffset);
            StreamSegmentTruncateOperation truncateOp = new StreamSegmentTruncateOperation(segmentId, truncationOffset);
            context.dataSource.add(truncateOp);
        }
    }

    //region Helpers

    private void verifyFinalOutput(HashMap<Long, ByteArrayOutputStream> segmentContents, Collection<Long> transactionIds, TestContext context) {
        // Verify all Transactions are deleted.
        for (long transactionId : transactionIds) {
            SegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(transactionId);
            Assert.assertTrue("Transaction not marked as deleted in metadata: " + transactionId, metadata.isDeleted());
            Assert.assertFalse("Transaction was not deleted from storage after being merged: " + transactionId, context.storage.exists(metadata.getName(), TIMEOUT).join());
            verifyAttributes(metadata, context);
        }

        for (long segmentId : segmentContents.keySet()) {
            SegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
            Assert.assertNotNull("Setup error: No metadata for segment " + segmentId, metadata);
            Assert.assertFalse("Setup error: Not expecting a Transaction segment in the final list: " + segmentId,
                    context.transactionIds.containsKey(metadata.getId()));

            Assert.assertEquals("Metadata does not indicate that all bytes were copied to Storage for segment " + segmentId, metadata.getLength(), metadata.getStorageLength());
            Assert.assertEquals("Metadata.Sealed disagrees with Metadata.SealedInStorage for segment " + segmentId, metadata.isSealed(), metadata.isSealedInStorage());

            SegmentProperties sp = context.storage.getStreamSegmentInfo(metadata.getName(), TIMEOUT).join();
            Assert.assertEquals("Metadata.StorageLength disagrees with Storage.Length for segment " + segmentId, metadata.getStorageLength(), sp.getLength());
            Assert.assertEquals("Metadata.Sealed/SealedInStorage disagrees with Storage.Sealed for segment " + segmentId, metadata.isSealedInStorage(), sp.isSealed());

            byte[] expected = segmentContents.get(segmentId).toByteArray();
            byte[] actual = new byte[expected.length];
            int actualLength = context.storage.read(InMemoryStorage.newHandle(metadata.getName(), true), 0, actual, 0, actual.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentId, metadata.getStorageLength(), actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentId, expected, actual);
            Assert.assertEquals("Unexpected truncation offset for segment " + segmentId,
                    metadata.getStartOffset(), context.storage.getTruncationOffset(metadata.getName()));
            verifyAttributes(metadata, context);
        }
    }

    private void verifyAttributes(SegmentMetadata metadata, TestContext context) {
        val persistedAttributes = context.dataSource.getPersistedAttributes(metadata.getId());
        int extendedAttributeCount = 0;
        if (context.transactionIds.containsKey(metadata.getId()) && metadata.isMerged()) {
            Assert.assertEquals("Unexpected number of attributes in attribute index for merged transaction " + metadata.getId(),
                    0, persistedAttributes.size());
            AssertExtensions.assertFutureThrows(
                    "Merged transaction attribute index still exists.",
                    context.dataSource.persistAttributes(metadata.getId(), Collections.singletonMap(AttributeId.randomUUID(), 0L), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        } else {
            for (val e : metadata.getAttributes().entrySet()) {
                if (Attributes.isCoreAttribute(e.getKey())) {
                    Assert.assertFalse("Not expecting Core Attribute in Attribute Index for Segment " + metadata.getId(), persistedAttributes.containsKey(e.getKey()));
                } else {
                    extendedAttributeCount++;
                    Assert.assertEquals("Unexpected attribute value for Segment " + metadata.getId(), e.getValue(), persistedAttributes.get(e.getKey()));
                }
            }

            Assert.assertEquals("Unexpected number of attributes in attribute index for Segment " + metadata.getId(), extendedAttributeCount, persistedAttributes.size());
            if (metadata.isSealedInStorage()) {
                AssertExtensions.assertFutureThrows(
                        "Sealed segment attribute index accepted new values.",
                        context.dataSource.persistAttributes(metadata.getId(), Collections.singletonMap(AttributeId.randomUUID(), 0L), TIMEOUT),
                        ex -> ex instanceof StreamSegmentSealedException);
            }
        }
    }


    private void mergeTransactions(Iterable<Long> transactionIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        for (long transactionId : transactionIds) {
            UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
            UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(context.transactionIds.get(transactionMetadata.getId()));
            Assert.assertFalse("Transaction already merged", transactionMetadata.isMerged());
            Assert.assertTrue("Transaction not sealed prior to merger", transactionMetadata.isSealed());
            Assert.assertFalse("Parent is sealed already merged", parentMetadata.isSealed());

            // Create the Merge Op
            MergeSegmentOperation op = new MergeSegmentOperation(parentMetadata.getId(), transactionMetadata.getId());
            op.setLength(transactionMetadata.getLength());
            op.setStreamSegmentOffset(parentMetadata.getLength());

            // Update metadata
            parentMetadata.setLength(parentMetadata.getLength() + transactionMetadata.getLength());
            transactionMetadata.markMerged();

            // Process the merge op
            context.dataSource.add(op);

            try {
                segmentContents.get(parentMetadata.getId()).write(segmentContents.get(transactionMetadata.getId()).toByteArray());
            } catch (IOException ex) {
                throw new AssertionError(ex);
            }

            segmentContents.remove(transactionId);
        }
    }

    private void metadataCheckpoint(TestContext context) {
        context.dataSource.add(new MetadataCheckpointOperation());
    }

    private void sealSegments(Collection<Long> segmentIds, TestContext context) {
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
            segmentMetadata.markSealed();
            StreamSegmentSealOperation sealOp = new StreamSegmentSealOperation(segmentId);
            sealOp.setStreamSegmentOffset(segmentMetadata.getLength());
            context.dataSource.add(sealOp);
        }
    }

    /**
     * Appends data, depth-first, by filling up one segment before moving on to another.
     */
    private void appendDataDepthFirst(Collection<Long> segmentIds, Function<Long, Integer> getAppendsPerSegment, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        int writeId = 0;
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
            int appendCount = getAppendsPerSegment.apply(segmentId);
            for (int i = 0; i < appendCount; i++) {
                appendData(segmentMetadata, i, writeId, segmentContents, context);
                writeId++;
            }

            for (int i = 0; i < UPDATE_ATTRIBUTES_PER_SEGMENT; i++) {
                updateAttributes(segmentMetadata, context);
            }
        }
    }

    /**
     * Appends data, round-robin style, one append per segment (for each segment), then back to the beginning.
     */
    private void appendDataBreadthFirst(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        int writeId = 0;

        // Put some attributes first.
        int halfAttributes = UPDATE_ATTRIBUTES_PER_SEGMENT / 2;
        for (int i = 0; i < halfAttributes; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                updateAttributes(segmentMetadata, context);
            }
        }

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                appendData(segmentMetadata, i, writeId, segmentContents, context);
                writeId++;
            }
        }

        // Put the rest of the attributes.
        for (int i = 0; i < halfAttributes; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                updateAttributes(segmentMetadata, context);
            }
        }
    }

    private void appendData(UpdateableSegmentMetadata segmentMetadata, int appendId, int writeId, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        byte[] data = getAppendData(segmentMetadata.getName(), segmentMetadata.getId(), appendId, writeId);

        // Make sure we increase the Length prior to appending; the Writer checks for this.
        long offset = segmentMetadata.getLength();
        segmentMetadata.setLength(offset + data.length);
        AttributeUpdateCollection attributeUpdates = generateAttributeUpdates(segmentMetadata);
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentMetadata.getId(), new ByteArraySegment(data), attributeUpdates);
        op.setStreamSegmentOffset(offset);
        context.dataSource.recordAppend(op);
        context.dataSource.add(new CachedStreamSegmentAppendOperation(op));
        recordAppend(segmentMetadata.getId(), data, segmentContents);
    }

    private void updateAttributes(UpdateableSegmentMetadata segmentMetadata, TestContext context) {
        AttributeUpdateCollection attributeUpdates = generateAttributeUpdates(segmentMetadata);
        context.dataSource.add(new UpdateAttributesOperation(segmentMetadata.getId(), attributeUpdates));
    }

    private AttributeUpdateCollection generateAttributeUpdates(UpdateableSegmentMetadata segmentMetadata) {
        long coreAttributeValue = segmentMetadata.getAttributes().getOrDefault(CORE_ATTRIBUTE_ID, 0L) + 1;
        val attributeUpdates = new AttributeUpdateCollection();
        attributeUpdates.add(new AttributeUpdate(CORE_ATTRIBUTE_ID, AttributeUpdateType.Accumulate, coreAttributeValue));
        for (int i = 0; i < EXTENDED_ATTRIBUTE_IDS.size(); i++) {
            AttributeId id = EXTENDED_ATTRIBUTE_IDS.get(i);
            long extendedAttributeValue = segmentMetadata.getAttributes().getOrDefault(id, 0L) + 13 + i;
            attributeUpdates.add(new AttributeUpdate(id, AttributeUpdateType.Replace, extendedAttributeValue));
        }
        segmentMetadata.updateAttributes(
                attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue)));
        return attributeUpdates;
    }

    private <T> void recordAppend(T segmentIdentifier, byte[] data, HashMap<T, ByteArrayOutputStream> segmentContents) {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentIdentifier, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentIdentifier, contents);
        }
        try {
            contents.write(data);
        } catch (IOException ex) {
            Assert.fail(ex.toString());
        }
    }

    private ArrayList<Long> createSegments(TestContext context) {
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String name = getSegmentName(i);
            context.metadata.mapStreamSegmentId(name, i);
            initializeSegment(i, context);
            segmentIds.add((long) i);

            // Add the operation to the log.
            StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(context.storage.getStreamSegmentInfo(name, TIMEOUT).join());
            mapOp.setStreamSegmentId(i);
            context.dataSource.add(mapOp);
        }

        return segmentIds;
    }

    private HashMap<Long, ArrayList<Long>> createTransactions(Collection<Long> segmentIds, TestContext context) {
        // Create the Transactions.
        HashMap<Long, ArrayList<Long>> transactions = new HashMap<>();
        long transactionId = Integer.MAX_VALUE;
        for (long parentId : segmentIds) {
            ArrayList<Long> segmentTransactions = new ArrayList<>();
            transactions.put(parentId, segmentTransactions);
            SegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                String transactionName = NameUtils.getTransactionNameFromId(parentMetadata.getName(), UUID.randomUUID());
                context.transactionIds.put(transactionId, parentId);
                context.metadata.mapStreamSegmentId(transactionName, transactionId);
                initializeSegment(transactionId, context);
                segmentTransactions.add(transactionId);

                // Add the operation to the log.
                StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(context.storage.getStreamSegmentInfo(transactionName, TIMEOUT).join());
                mapOp.setStreamSegmentId(transactionId);
                context.dataSource.add(mapOp);

                transactionId++;
            }
        }

        return transactions;
    }

    private Collection<Long> evictSegments(long cutoffSeqNo, TestContext context) {
        EvictableMetadata metadata = (EvictableMetadata) context.metadata;
        Collection<SegmentMetadata> evictionCandidates = metadata.getEvictionCandidates(cutoffSeqNo, Integer.MAX_VALUE);
        metadata.cleanup(evictionCandidates, cutoffSeqNo);
        return evictionCandidates
                .stream()
                .map(SegmentMetadata::getId)
                .collect(Collectors.toSet());
    }

    private void initializeSegment(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setLength(0);
        metadata.setStorageLength(0);
        context.storage.create(metadata.getName(), TIMEOUT).join();
    }

    private byte[] getAppendData(String segmentName, long segmentId, int segmentAppendSeq, int writeId) {
        // NOTE: the data returned by this should be deterministic (not random) since the recovery test relies on it being that way.
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d%n", segmentName, segmentId, segmentAppendSeq, writeId).getBytes();
    }

    private String getSegmentName(int i) {
        return "Segment_" + i;
    }

    //endregion

    //region TestWriterProcessor

    @RequiredArgsConstructor
    private class TestWriterProcessor implements WriterSegmentProcessor {
        final AtomicBoolean closed = new AtomicBoolean();
        final ArrayList<SegmentOperation> operations = new ArrayList<>();
        final AtomicInteger firstUnAcked = new AtomicInteger();

        //region WriterSegmentProcessor Implementation

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public boolean isClosed() {
            return this.closed.get();
        }

        @Override
        public long getLowestUncommittedSequenceNumber() {
            if (this.firstUnAcked.get() >= this.operations.size()) {
                return -1;
            }

            return this.operations.get(this.firstUnAcked.get()).getSequenceNumber();
        }

        @Override
        public boolean mustFlush() {
            return false;
        }

        @Override
        public void add(SegmentOperation operation) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            this.operations.add(operation);
        }

        @Override
        public CompletableFuture<WriterFlushResult> flush(boolean force, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            this.firstUnAcked.set(this.operations.size());
            return CompletableFuture.completedFuture(new WriterFlushResult());
        }

        //endregion
    }

    //endregion

    // region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final TestWriterDataSource dataSource;
        final InMemoryStorage baseStorage;
        final TestStorage storage;
        final WriterConfig config;
        final Map<Long, Long> transactionIds;
        final WriterFactory.CreateProcessors createProcessors;
        StorageWriter writer;

        TestContext(WriterConfig config) {
            this(config, m -> Collections.emptyList());
        }

        TestContext(WriterConfig config, WriterFactory.CreateProcessors createProcessors) {
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            this.baseStorage = new InMemoryStorage();
            this.storage = new TestStorage(this.baseStorage, executorService());
            this.storage.initialize(1);
            this.config = config;
            this.createProcessors = createProcessors;

            this.transactionIds = new HashMap<>();
            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = METADATA_CHECKPOINT_FREQUENCY;
            this.dataSource = new TestWriterDataSource(this.metadata, executorService(), dataSourceConfig);
            this.writer = new StorageWriter(this.config, this.dataSource, this.storage, this.createProcessors, executorService());
        }

        void resetWriter() {
            this.writer.close();
            this.baseStorage.changeOwner();
            this.writer = new StorageWriter(this.config, this.dataSource, this.storage, this.createProcessors, executorService());
        }

        @Override
        public void close() {
            this.dataSource.close();
            this.writer.close();
            this.storage.close(); // This also closes the baseStorage.
        }
    }

    // endregion
}
