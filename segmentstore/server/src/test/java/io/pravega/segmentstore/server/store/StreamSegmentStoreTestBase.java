/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Base class for any test that verifies the functionality of a StreamSegmentStore class.
 */
public abstract class StreamSegmentStoreTestBase extends ThreadPooledTestSuite {
    //region Test Configuration

    // Even though this should work with just 1-2 threads, doing so would cause this test to run for a long time. Choosing
    // a decent size so that the tests do finish up within a few seconds.
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int THREADPOOL_SIZE_SEGMENT_STORE = 20;
    private static final int THREADPOOL_SIZE_TEST = 3;
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 1;
    private static final int APPENDS_PER_SEGMENT = 100;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds() * 10);

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
                                    .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 512) // Need this for truncation testing.
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
     * Tests an end-to-end scenario for the SegmentStore, utilizing a read-write SegmentStore for making modifications
     * (writes, seals, creates, etc.) and a ReadOnlySegmentStore to verify the changes being persisted into Storage.
     * * Appends
     * * Reads
     * * Segment and transaction creation
     * * Transaction mergers
     * * Recovery
     *
     * @throws Exception If an exception occurred.
     */
    @Test(timeout = 300000)
    public void testEndToEnd() throws Exception {

        // Phase 1: Create segments and add some appends.
        ArrayList<String> segmentNames;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, Long> startOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        try (val builder = createBuilder()) {
            val segmentStore = builder.createStreamSegmentService();

            // Create the StreamSegments.
            segmentNames = createSegments(segmentStore);
            transactionsBySegment = createTransactions(segmentNames, segmentStore);

            // Add some appends.
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            appendData(segmentsAndTransactions, segmentContents, lengths, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkSegmentStatus(lengths, startOffsets, false, false, segmentStore);
        }

        // Phase 2: Force a recovery and merge all transactions.
        try (val builder = createBuilder()) {
            val segmentStore = builder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);

            // Merge all transactions.
            mergeTransactions(transactionsBySegment, lengths, segmentContents, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkSegmentStatus(lengths, startOffsets, false, false, segmentStore);
        }

        // Phase 3: Force a recovery, immediately check reads, then truncate and read at the same time.
        try (val builder = createBuilder();
             val readOnlyBuilder = createReadOnlyBuilder()) {
            val segmentStore = builder.createStreamSegmentService();
            val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);

            // Wait for all the data to move to Storage.
            waitForSegmentsInStorage(segmentNames, segmentStore, readOnlySegmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkStorage(segmentContents, segmentStore, readOnlySegmentStore);
            checkReadsWhileTruncating(segmentContents, startOffsets, segmentStore);
            checkStorage(segmentContents, segmentStore, readOnlySegmentStore);
        }

        // Phase 4: Force a recovery, seal segments and then delete them.
        try (val builder = createBuilder();
             val readOnlyBuilder = createReadOnlyBuilder()) {
            val segmentStore = builder.createStreamSegmentService();
            val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            // Seals.
            sealSegments(segmentNames, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkSegmentStatus(lengths, startOffsets, true, false, segmentStore);

            waitForSegmentsInStorage(segmentNames, segmentStore, readOnlySegmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Deletes.
            deleteSegments(segmentNames, segmentStore).join();
            checkSegmentStatus(lengths, startOffsets, true, true, segmentStore);
        }
    }

    //region Helpers

    protected ServiceBuilder createBuilder() throws Exception {
        val builderConfig = this.configBuilder.build();
        val builder = createBuilder(builderConfig);
        builder.initialize();
        return builder;
    }

    /**
     * When overridden in a derived class, creates a ServiceBuilder using the given configuration.
     *
     * @param builderConfig The configuration to use.
     * @return The ServiceBuilder.
     */
    protected abstract ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig);

    public ServiceBuilder createReadOnlyBuilder() throws Exception {
        // Copy base config properties to a new object.
        val props = new Properties();
        this.configBuilder.build().forEach(props::put);

        // Create a new config (so we don't alter the base one) and set the ReadOnlySegmentStore to true).
        val builderConfig = ServiceBuilderConfig.builder()
                                                .include(props)
                                                .include(ServiceConfig.builder()
                                                                      .with(ServiceConfig.READONLY_SEGMENT_STORE, true))
                                                .build();

        val builder = createBuilder(builderConfig);
        builder.initialize();
        return builder;
    }

    protected CompletableFuture<Void> appendData(Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents,
                                                 HashMap<String, Long> lengths, StreamSegmentStore store) {
        val segmentFutures = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
                byte[] appendData = getAppendData(segmentName, i);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                segmentFutures.add(store.append(segmentName, appendData, null, TIMEOUT));
            }
        }

        return Futures.allOf(segmentFutures);
    }

    protected CompletableFuture<Void> mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
                                                        HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        ArrayList<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                mergeFutures.add(store.sealStreamSegment(transactionName, TIMEOUT)
                                      .thenCompose(v -> store.mergeTransaction(transactionName, TIMEOUT)));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        return Futures.allOf(mergeFutures);
    }

    protected CompletableFuture<Void> sealSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Long>>();
        for (String segmentName : segmentNames) {
            result.add(store.sealStreamSegment(segmentName, TIMEOUT));
        }

        return Futures.allOf(result);
    }

    protected CompletableFuture<Void> deleteSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            result.add(store.deleteStreamSegment(segmentName, TIMEOUT));
        }

        return Futures.allOf(result);
    }

    protected ArrayList<String> createSegments(StreamSegmentStore store) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createStreamSegment(segmentName, null, TIMEOUT));
        }

        Futures.allOf(futures).join();
        return segmentNames;
    }

    protected HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, StreamSegmentStore store) {
        // Create the Transaction.
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                futures.add(store.createTransaction(segmentName, UUID.randomUUID(), null, TIMEOUT));
            }
        }

        Futures.allOf(futures).join();

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

    protected void checkSegmentStatus(HashMap<String, Long> segmentLengths, HashMap<String, Long> startOffsets,
                                      boolean expectSealed, boolean expectDeleted, StreamSegmentStore store) {
        for (Map.Entry<String, Long> e : segmentLengths.entrySet()) {
            String segmentName = e.getKey();
            if (expectDeleted) {
                AssertExtensions.assertThrows(
                        "Segment '" + segmentName + "' was not deleted.",
                        () -> store.getStreamSegmentInfo(segmentName, false, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            } else {
                SegmentProperties sp = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
                long expectedStartOffset = startOffsets.getOrDefault(segmentName, 0L);
                long expectedLength = e.getValue();
                Assert.assertEquals("Unexpected Start Offset for segment " + segmentName, expectedStartOffset, sp.getStartOffset());
                Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
                Assert.assertEquals("Unexpected value for isSealed for segment " + segmentName, expectSealed, sp.isSealed());
                Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            }
        }
    }

    protected void checkReads(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();
            long segmentLength = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();
            Assert.assertEquals("Unexpected Read Index length for segment " + segmentName, expectedData.length, segmentLength);

            long expectedCurrentOffset = 0;
            @Cleanup
            ReadResult readResult = store.read(segmentName, expectedCurrentOffset, (int) segmentLength, TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in StreamSegmentContainerTests; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName,
                        0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName,
                        expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                if (!readEntry.getContent().isDone()) {
                    readEntry.requestContent(TIMEOUT);
                }
                readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName,
                        ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset,
                        expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

    protected void checkReadsWhileTruncating(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> startOffsets,
                                             StreamSegmentStore store) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();
            long segmentLength = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();
            long expectedCurrentOffset = 0;
            boolean truncate = false;

            while (expectedCurrentOffset < segmentLength) {
                @Cleanup
                ReadResult readResult = store.read(segmentName, expectedCurrentOffset, (int) (segmentLength - expectedCurrentOffset), TIMEOUT).join();
                Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

                // We only test the truncation-related pieces here; other read-related checks are done in checkReads.
                while (readResult.hasNext()) {
                    ReadResultEntry readEntry = readResult.next();
                    Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName,
                            expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                    if (!readEntry.getContent().isDone()) {
                        readEntry.requestContent(TIMEOUT);
                    }

                    if (readEntry.getType() == ReadResultEntryType.Truncated) {
                        long startOffset = startOffsets.getOrDefault(segmentName, 0L);
                        // Verify that the Segment actually is truncated beyond this offset.
                        AssertExtensions.assertLessThan("Found Truncated ReadResultEntry but current offset not truncated.",
                                startOffset, readEntry.getStreamSegmentOffset());

                        // Verify the ReadResultEntry cannot be used and throws an appropriate exception.
                        AssertExtensions.assertThrows(
                                "ReadEntry.getContent() did not throw for a Truncated entry.",
                                readEntry::getContent,
                                ex -> ex instanceof StreamSegmentTruncatedException);

                        // Verify ReadResult is done.
                        Assert.assertFalse("Unexpected result from ReadResult.hasNext when encountering truncated entry.",
                                readResult.hasNext());

                        // Verify attempting to read at the current offset will return the appropriate entry (and not throw).
                        @Cleanup
                        ReadResult truncatedResult = store.read(segmentName, readEntry.getStreamSegmentOffset(), 1, TIMEOUT).join();
                        val first = truncatedResult.next();
                        Assert.assertEquals("Read request for a truncated offset did not start with a Truncated ReadResultEntryType.",
                                ReadResultEntryType.Truncated, first.getType());

                        // Skip over until the first non-truncated offset.
                        expectedCurrentOffset = Math.max(expectedCurrentOffset, startOffset);
                        continue;
                    }

                    // Non-truncated entry; do the usual verifications.
                    readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName,
                            ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                    ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                    byte[] actualData = new byte[readEntryContents.getLength()];
                    StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                    AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset,
                            expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                    expectedCurrentOffset += readEntryContents.getLength();

                    // Every other read, determine if we should truncate or not.
                    if (truncate) {
                        long truncateOffset;
                        if (segmentName.hashCode() % 2 == 0) {
                            // Truncate just beyond the current read offset.
                            truncateOffset = Math.min(segmentLength, expectedCurrentOffset + 1);
                        } else {
                            // Truncate half of what we read so far.
                            truncateOffset = Math.min(segmentLength, expectedCurrentOffset / 2 + 1);
                        }

                        startOffsets.put(segmentName, truncateOffset);
                        store.truncateStreamSegment(segmentName, truncateOffset, TIMEOUT).join();
                    }

                    truncate = !truncate;
                }

                Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
            }
        }
    }

    protected static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore baseStore,
                                       StreamSegmentStore readOnlySegmentStore) throws Exception {
        for (Map.Entry<String, ByteArrayOutputStream> e : segmentContents.entrySet()) {
            String segmentName = e.getKey();
            byte[] expectedData = e.getValue().toByteArray();

            // 1. Deletion status
            SegmentProperties sp = null;
            try {
                sp = baseStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                AssertExtensions.assertThrows(
                        "Segment is marked as deleted in SegmentStore but was not deleted in Storage " + segmentName,
                        () -> readOnlySegmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            SegmentProperties storageProps = readOnlySegmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Store and Storage for segment " + segmentName,
                    sp.isSealed(), storageProps.isSealed());

            // 3. Contents.
            SegmentProperties metadataProps = baseStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedData.length,
                    storageProps.getLength());
            byte[] actualData = new byte[expectedData.length];
            int actualLength = 0;
            int expectedLength = actualData.length;

            try {
                @Cleanup
                ReadResult readResult = readOnlySegmentStore.read(segmentName, 0, actualData.length, TIMEOUT).join();
                actualLength = readResult.readRemaining(actualData, TIMEOUT);
            } catch (Exception ex) {
                ex = (Exception) Exceptions.unwrap(ex);
                if (!(ex instanceof StreamSegmentTruncatedException) || metadataProps.getStartOffset() == 0) {
                    // We encountered an unexpected Exception, or a Truncated Segment which was not expected to be truncated.
                    throw ex;
                }

                // Read from the truncated point, except if the whole segment got truncated.
                expectedLength = (int) (storageProps.getLength() - metadataProps.getStartOffset());
                if (metadataProps.getStartOffset() < storageProps.getLength()) {
                    @Cleanup
                    ReadResult readResult = readOnlySegmentStore.read(segmentName, metadataProps.getStartOffset(),
                            expectedLength, TIMEOUT).join();
                    actualLength = readResult.readRemaining(actualData, TIMEOUT);
                }
            }

            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentName,
                    expectedLength, actualLength);
            AssertExtensions.assertArrayEquals("Unexpected data written to storage for segment " + segmentName,
                    expectedData, expectedData.length - expectedLength, actualData, 0, expectedLength);
        }
    }

    protected CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
                                                               StreamSegmentStore readOnlyStore) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = baseStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, readOnlyStore));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, StreamSegmentStore readOnlyStore) {
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return Futures.loop(
                tryAgain::get,
                () -> readOnlyStore.getStreamSegmentInfo(sp.getName(), false, TIMEOUT)
                                   .thenCompose(storageProps -> {
                                       if (sp.isSealed()) {
                                           tryAgain.set(!storageProps.isSealed());
                                       } else {
                                           tryAgain.set(sp.getLength() != storageProps.getLength());
                                       }

                                       if (tryAgain.get() && !timer.hasRemaining()) {
                                           return Futures.<Void>failedFuture(new TimeoutException(
                                                   String.format("Segment %s did not complete in Storage in the allotted time.", sp.getName())));
                                       } else {
                                           return Futures.delayedFuture(Duration.ofMillis(100), executorService());
                                       }
                                   }), executorService());
    }

    //endregion
}
