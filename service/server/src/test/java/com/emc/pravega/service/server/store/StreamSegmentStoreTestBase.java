/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.store;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.containers.ContainerConfig;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.writer.WriterConfig;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for any test that verifies the functionality of a StreamSegmentStore class.
 */
public abstract class StreamSegmentStoreTestBase extends ThreadPooledTestSuite {
    //region Test Configuration

    private static final int THREADPOOL_SIZE_SEGMENT_STORE = 20;
    private static final int THREADPOOL_SIZE_TEST = 3;
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 1;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    protected final ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig.builder()
                                  .with(ServiceConfig.CONTAINER_COUNT, 4)
                                  .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE))
            .include(ContainerConfig
                    .builder()
                    .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS))
            .include(DurableLogConfig
                    .builder()
                    .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                    .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                    .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L))
            .include(ReadIndexConfig.builder()
                                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024)
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 64 * 1024 * 1024L)
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 30 * 1000))
            .include(WriterConfig
                    .builder()
                    .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                    .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                    .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                    .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L));

    @Override
    protected int getThreadPoolSize() {
        return THREADPOOL_SIZE_TEST;
    }

    //endregion

    /**
     * Tests an end-to-end scenario using real adapters for Cache (RocksDB) and Storage (HDFS).
     * Currently this does not use a real adapter for DurableDataLog due to difficulties in getting DistributedLog
     * to run in-process.
     *
     * @throws Exception If an exception occurred.
     */
    @Test
    public void testEndToEnd() throws Exception {
        AtomicReference<Storage> storage = new AtomicReference<>();

        // Phase 1: Create segments and add some appends.
        ArrayList<String> segmentNames;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        try (val builder = createBuilder(storage)) {
            val segmentStore = builder.createStreamSegmentService();

            // Create the StreamSegments.
            segmentNames = createSegments(segmentStore);
            transactionsBySegment = createTransactions(segmentNames, segmentStore);

            // Add some appends.
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            appendData(segmentsAndTransactions, segmentContents, lengths, segmentStore).join();
            checkSegmentStatus(lengths, false, false, segmentStore);
        }

        // Phase 2: Force a recovery and merge all transactions.
        try (val builder = createBuilder(storage)) {
            val segmentStore = builder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);

            // Merge all transactions.
            mergeTransactions(transactionsBySegment, lengths, segmentContents, segmentStore);
            checkSegmentStatus(lengths, false, false, segmentStore);
        }

        // Phase 3: Force a recovery and check final reads.
        try (val builder = createBuilder(storage)) {
            val segmentStore = builder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);

            // Wait for all the data to move to Storage.
            waitForSegmentsInStorage(segmentNames, segmentStore, storage.get()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkStorage(segmentContents, segmentStore, storage.get());
        }

        // Phase 4: Force a recovery, seal segments and then delete them..
        try (val builder = createBuilder(storage)) {
            val segmentStore = builder.createStreamSegmentService();

            // Seals.
            sealSegments(segmentNames, segmentStore).join();
            checkSegmentStatus(lengths, true, false, segmentStore);

            waitForSegmentsInStorage(segmentNames, segmentStore, storage.get()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Deletes.
            deleteSegments(segmentNames, segmentStore).join();
            checkSegmentStatus(lengths, true, true, segmentStore);
        }
    }

    //region Helpers

    private ServiceBuilder createBuilder(AtomicReference<Storage> storage) {
        val builderConfig = this.configBuilder.build();
        val builder = createBuilder(builderConfig, storage);
        builder.initialize().join();
        return builder;
    }

    /**
     * When overridden in a derived class, creates a ServiceBuilder using the given configuration.
     *
     * @param builderConfig The configuration to use.
     * @param storage       After the completion of this method, this will contain a reference to the Storage used by
     *                      this builder.
     * @return The ServiceBuilder.
     */
    protected abstract ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig, AtomicReference<Storage> storage);

    private CompletableFuture<Void> appendData(Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents,
                                               HashMap<String, Long> lengths, StreamSegmentStore store) {
        val segmentFutures = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            AtomicInteger count = new AtomicInteger();
            segmentFutures.add(FutureHelpers.loop(
                    () -> count.incrementAndGet() < APPENDS_PER_SEGMENT,
                    () -> {
                        byte[] appendData = getAppendData(segmentName, count.get());
                        synchronized (lengths) {
                            lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                            recordAppend(segmentName, appendData, segmentContents);
                        }

                        return store.append(segmentName, appendData, null, TIMEOUT);
                    },
                    executorService()));
        }

        return FutureHelpers.allOf(segmentFutures);
    }

    private void mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
                                   HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        ArrayList<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                store.sealStreamSegment(transactionName, TIMEOUT)
                     .thenCompose(v -> store.mergeTransaction(transactionName, TIMEOUT)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        FutureHelpers.allOf(mergeFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Void> sealSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Long>>();
        for (String segmentName : segmentNames) {
            result.add(store.sealStreamSegment(segmentName, TIMEOUT));
        }

        return FutureHelpers.allOf(result);
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            result.add(store.deleteStreamSegment(segmentName, TIMEOUT));
        }

        return FutureHelpers.allOf(result);
    }

    private ArrayList<String> createSegments(StreamSegmentStore store) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createStreamSegment(segmentName, null, TIMEOUT));
        }

        FutureHelpers.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, StreamSegmentStore store) {
        // Create the Transaction.
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                futures.add(store.createTransaction(segmentName, UUID.randomUUID(), null, TIMEOUT));
            }
        }

        FutureHelpers.allOf(futures).join();

        // Get the Transaction names and index them by parent segment names.
        HashMap<String, ArrayList<String>> transactions = new HashMap<>();
        for (CompletableFuture<String> transactionFuture : futures) {
            String transactionName = transactionFuture.join();
            String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
            assert parentName != null : "Transaction created with invalid parent";
            ArrayList<String> segmentTransactions = transactions.get(parentName);
            if (segmentTransactions == null) {
                segmentTransactions = new ArrayList<>();
                transactions.put(parentName, segmentTransactions);
            }

            segmentTransactions.add(transactionName);
        }

        return transactions;
    }

    private byte[] getAppendData(String segmentName, int appendId) {
        return String.format("%s_%d", segmentName, appendId).getBytes();
    }

    @SneakyThrows(IOException.class)
    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private void checkSegmentStatus(HashMap<String, Long> segmentLengths, boolean expectSealed, boolean expectDeleted, StreamSegmentStore store) {
        for (Map.Entry<String, Long> e : segmentLengths.entrySet()) {
            String segmentName = e.getKey();
            if (expectDeleted) {
                AssertExtensions.assertThrows(
                        "Segment '" + segmentName + "' was not deleted.",
                        () -> store.getStreamSegmentInfo(segmentName, false, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            } else {
                SegmentProperties sp = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
                long expectedLength = e.getValue();
                Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
                Assert.assertEquals("Unexpected value for isSealed for segment " + segmentName, expectSealed, sp.isSealed());
                Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            }
        }
    }

    private void checkReads(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();
            long segmentLength = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();
            Assert.assertEquals("Unexpected Read Index length for segment " + segmentName, expectedData.length, segmentLength);

            long expectedCurrentOffset = 0;
            @Cleanup
            ReadResult readResult = store.read(segmentName, expectedCurrentOffset, (int) segmentLength, TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in testSegmentRegularOperations; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName, expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                if (!readEntry.getContent().isDone()) {
                    readEntry.requestContent(TIMEOUT);
                }
                readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset, expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store, Storage storage) {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();

            // 1. Deletion status
            SegmentProperties sp = null;
            try {
                sp = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                Assert.assertFalse(
                        "Segment is marked as deleted in SegmentStore but was not deleted in Storage " + segmentName,
                        storage.exists(segmentName, TIMEOUT).join());

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            SegmentProperties storageProps = storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Store and Storage for segment " + segmentName, sp.isSealed(), storageProps.isSealed());

            // 3. Contents.
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedData.length, storageProps.getLength());
            byte[] actualData = new byte[expectedData.length];
            val readHandle = storage.openRead(segmentName).join();
            int actualLength = storage.read(readHandle, 0, actualData, 0, actualData.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentName, expectedData.length, actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentName, expectedData, actualData);
        }
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore store, Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, storage));
        }

        return FutureHelpers.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage storage) {
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return FutureHelpers.loop(
                tryAgain::get,
                () -> storage.getStreamSegmentInfo(sp.getName(), TIMEOUT)
                             .thenCompose(storageProps -> {
                                 if (sp.isSealed()) {
                                     tryAgain.set(!storageProps.isSealed());
                                 } else {
                                     tryAgain.set(sp.getLength() != storageProps.getLength());
                                 }

                                 if (tryAgain.get() && !timer.hasRemaining()) {
                                     return FutureHelpers.<Void>failedFuture(new TimeoutException(
                                             String.format("Segment %s did not complete in Storage in the allotted time.", sp.getName())));
                                 } else {
                                     return FutureHelpers.delayedFuture(Duration.ofMillis(100), executorService());
                                 }
                             }), executorService());
    }

    //endregion

    //region ListenableStorageFactory

    @RequiredArgsConstructor
    protected static class ListenableStorageFactory implements StorageFactory {
        private final StorageFactory wrappedFactory;
        private final Consumer<Storage> storageCreated;

        @Override
        public Storage createStorageAdapter() {
            Storage storage = this.wrappedFactory.createStorageAdapter();
            val callback = this.storageCreated;
            if (callback != null) {
                callback.accept(storage);
            }

            return storage;
        }
    }

    //endregion
}
