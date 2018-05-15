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

import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Base class for any test that verifies the functionality of a StreamSegmentStore class.
 */
@Slf4j
public abstract class StreamSegmentStoreTestBase extends ThreadPooledTestSuite {
    //region Test Configuration

    // Even though this should work with just 1-2 threads, doing so would cause this test to run for a long time. Choosing
    // a decent size so that the tests do finish up within a few seconds.
    private static final int THREADPOOL_SIZE_SEGMENT_STORE = 20;
    private static final int THREADPOOL_SIZE_SEGMENT_STORE_STORAGE = 10;
    private static final int THREADPOOL_SIZE_TEST = 3;
    private static final int MAX_FENCING_ITERATIONS = 100;
    private static final String EMPTY_SEGMENT_NAME = "Empty_Segment";
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 1;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int ATTRIBUTE_UPDATES_PER_SEGMENT = 100;
    private static final List<UUID> ATTRIBUTES = Arrays.asList(Attributes.EVENT_COUNT, UUID.randomUUID(), UUID.randomUUID());
    private static final int EXPECTED_ATTRIBUTE_VALUE = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds() * 10);

    protected final ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig.builder()
                                  .with(ServiceConfig.CONTAINER_COUNT, 4)
                                  .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE)
                                  .with(ServiceConfig.STORAGE_THREAD_POOL_SIZE, THREADPOOL_SIZE_SEGMENT_STORE_STORAGE))
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

    /**
     * When overridden in a derived class, this method will return the number of milliseconds to wait between creating
     * new instances of the Segment Store, for the fencing test.
     */
    protected int getFencingIterationDelayMillis() {
        return 100;
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
    @Test
    public void testEndToEnd() throws Exception {
        ArrayList<String> segmentNames;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, Long> startOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        int instanceId = 0;

        // Phase 1: Create segments and add some appends.
        log.info("Starting Phase 1.");
        try (val builder = createBuilder(++instanceId)) {
            val segmentStore = builder.createStreamSegmentService();

            // Create the StreamSegments.
            segmentNames = createSegments(segmentStore);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));
            transactionsBySegment = createTransactions(segmentNames, segmentStore);
            log.info("Created Transactions: {}.", transactionsBySegment.values().stream().flatMap(Collection::stream).collect(Collectors.joining(", ")));

            // Add some appends.
            ArrayList<String> segmentsAndTransactions = new ArrayList<>(segmentNames);
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            appendData(segmentsAndTransactions, segmentContents, lengths, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished appending data.");

            checkSegmentStatus(lengths, startOffsets, false, false, segmentStore);
            log.info("Finished Phase 1");
        }

        // Phase 2: Force a recovery and merge all transactions.
        log.info("Starting Phase 2.");
        try (val builder = createBuilder(++instanceId)) {
            val segmentStore = builder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);
            log.info("Finished checking reads.");

            // Merge all transactions.
            mergeTransactions(transactionsBySegment, lengths, segmentContents, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished merging transactions.");

            checkSegmentStatus(lengths, startOffsets, false, false, segmentStore);
            log.info("Finished Phase 2.");
        }

        // Phase 3: Force a recovery, immediately check reads, then truncate and read at the same time.
        log.info("Starting Phase 3.");
        try (val builder = createBuilder(++instanceId);
             val readOnlyBuilder = createReadOnlyBuilder(instanceId)) {
            val segmentStore = builder.createStreamSegmentService();
            val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);
            log.info("Finished checking reads.");

            // Wait for all the data to move to Storage.
            waitForSegmentsInStorage(segmentNames, segmentStore, readOnlySegmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished waiting for segments in Storage.");

            checkStorage(segmentContents, segmentStore, readOnlySegmentStore);
            log.info("Finished Storage check.");

            checkReadsWhileTruncating(segmentContents, startOffsets, segmentStore);
            log.info("Finished checking reads while truncating.");

            checkStorage(segmentContents, segmentStore, readOnlySegmentStore);
            log.info("Finished Phase 3.");
        }

        // Phase 4: Force a recovery, seal segments and then delete them.
        log.info("Starting Phase 4.");
        try (val builder = createBuilder(++instanceId);
             val readOnlyBuilder = createReadOnlyBuilder(instanceId)) {
            val segmentStore = builder.createStreamSegmentService();
            val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            // Seals.
            sealSegments(segmentNames, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished sealing.");

            checkSegmentStatus(lengths, startOffsets, true, false, segmentStore);

            waitForSegmentsInStorage(segmentNames, segmentStore, readOnlySegmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Finished waiting for segments in Storage.");

            // Deletes.
            deleteSegments(segmentNames, segmentStore).join();
            log.info("Finished deleting segments.");

            checkSegmentStatus(lengths, startOffsets, true, true, segmentStore);
            log.info("Finished Phase 4.");
        }

        log.info("Finished.");
    }

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
    @Test
    public void testEndToEndWithFencing() throws Exception {
        ArrayList<ServiceBuilder> builders = new ArrayList<>();
        ArrayList<String> segmentNames = null;
        HashMap<String, ArrayList<String>> transactionsBySegment;
        ArrayList<String> segmentsAndTransactions;
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, Long> startOffsets = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Retry for Segment Store (Container) initialization. Normally we have the Controller coordinating which instances
        // are the rightful survivors, however in this case we need to simulate some of this behavior ourselves, by being
        // insistent. It is possible that previous instances meddle with the BKLog ZK metadata during the new instance's
        // initialization, causing the new instance to wrongfully assume it's not the rightful survivor. A quick retry solves
        // this problem, as there is no other kind of information available to disambiguate this.
        val initRetry = Retry.withExpBackoff(10, 2, 10, TIMEOUT.toMillis() / 10)
                .retryWhen(ex -> Exceptions.unwrap(ex) instanceof DataLogWriterNotPrimaryException);

        // Regular operation retry.
        val operationRetry = Retry.withExpBackoff(50, 2, 10, TIMEOUT.toMillis());

        log.info("Starting.");
        try {
            AtomicReference<StreamSegmentStore> currentStore = new AtomicReference<>();
            AtomicInteger fencingIteration = new AtomicInteger();
            CompletableFuture<Void> done = null;
            while (fencingIteration.get() < MAX_FENCING_ITERATIONS && (done == null || !done.isDone())) {
                // Start a new Segment Store instance and fence out the previous one.
                log.info("Starting Instance {}", fencingIteration.get() + 1);
                initRetry.run(() -> {
                    ServiceBuilder b = createBuilder(fencingIteration.get());
                    builders.add(b);
                    currentStore.set(b.createStreamSegmentService());
                    fencingIteration.incrementAndGet();
                    log.info("Instance {} Started", fencingIteration);
                    return null;
                });

                // The following blob will only get executed once, and be done so asynchronously.
                if (done == null) {
                    // Create the StreamSegments and their transactions.
                    segmentNames = createSegments(currentStore.get());
                    segmentsAndTransactions = new ArrayList<>(segmentNames);
                    log.info("Created Segments: {}.", String.join(", ", segmentNames));
                    transactionsBySegment = createTransactions(segmentNames, currentStore.get());
                    transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
                    log.info("Created Transactions: {}.", transactionsBySegment.values().stream().flatMap(Collection::stream).collect(Collectors.joining(", ")));

                    // Generate all the requests.
                    AtomicInteger index = new AtomicInteger();
                    val requests = Iterators.concat(
                            createAppendDataRequests(segmentsAndTransactions, segmentContents, lengths).iterator(),
                            createMergeTransactionsRequests(transactionsBySegment, lengths, segmentContents).iterator(),
                            createSealSegmentsRequests(segmentNames).iterator());

                    // Execute all the requests asynchronously, one by one. We retry all expected exceptions, and when we do,
                    // we make sure to execute them on the current Segment Store instance (since the previous one may be unusable).
                    done = Futures.loop(
                            requests::hasNext,
                            () -> {
                                int opIndex = index.getAndIncrement();
                                log.debug("Initiating Operation #{} on iteration {}.", opIndex, fencingIteration);
                                Function<StreamSegmentStore, CompletableFuture<Void>> request = requests.next();
                                AtomicReference<StreamSegmentStore> requestStore = new AtomicReference<>(currentStore.get());
                                return operationRetry
                                        .retryWhen(ex -> {
                                            requestStore.getAndSet(currentStore.get());
                                            ex = Exceptions.unwrap(ex);
                                            log.info("Operation #{} (Iteration = {}) failed due to {}.", opIndex, fencingIteration, ex.toString());
                                            return isExpectedFencingException(ex);
                                        })
                                        .runAsync(() -> request.apply(requestStore.get()), executorService());
                            },
                            executorService());
                }

                // Wait a while between fencing iterations.
                Thread.sleep(getFencingIterationDelayMillis());
            }

            done.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Check reads.
            checkReads(segmentContents, currentStore.get());
            log.info("Finished checking reads.");

            // Delete everything.
            deleteSegments(segmentNames, currentStore.get()).join();
            log.info("Finished deleting segments.");
            checkSegmentStatus(lengths, startOffsets, true, true, currentStore.get());
        } finally {
            log.info("Stopping all instances.");
            builders.forEach(ServiceBuilder::close);
        }

        log.info("Finished.");
    }

    //region Helpers

    private ServiceBuilder createBuilder(int instanceId) throws Exception {
        val builder = createBuilder(this.configBuilder, instanceId);
        builder.initialize();
        return builder;
    }

    /**
     * When overridden in a derived class, creates a ServiceBuilder using the given configuration.
     *
     * @param builderConfig The configuration to use.
     * @return The ServiceBuilder.
     */
    protected abstract ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId);

    private ServiceBuilder createReadOnlyBuilder(int instanceId) throws Exception {
        // Copy base config properties to a new object.
        val props = new Properties();
        this.configBuilder.build().forEach(props::put);

        // Create a new config (so we don't alter the base one) and set the ReadOnlySegmentStore to true).
        val configBuilder = ServiceBuilderConfig.builder()
                .include(props)
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.READONLY_SEGMENT_STORE, true));

        val builder = createBuilder(configBuilder, instanceId);
        builder.initialize();
        return builder;
    }

    private ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>> createAppendDataRequests(
            Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths) {
        val result = new ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>>();
        val halfAttributeCount = ATTRIBUTE_UPDATES_PER_SEGMENT / 2;
        for (String segmentName : segmentNames) {
            if (isEmptySegment(segmentName)) {
                continue;
            }

            // Add half the attribute updates now.
            for (int i = 0; i < halfAttributeCount; i++) {
                result.add(store -> store.updateAttributes(segmentName, createAttributeUpdates(), TIMEOUT));
            }

            // Add some appends.
            for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
                byte[] appendData = getAppendData(segmentName, i);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                result.add(store -> store.append(segmentName, appendData, createAttributeUpdates(), TIMEOUT));
            }

            // Add the rest of the attribute updates.
            for (int i = 0; i < halfAttributeCount; i++) {
                result.add(store -> store.updateAttributes(segmentName, createAttributeUpdates(), TIMEOUT));
            }
        }

        return result;
    }

    private CompletableFuture<Void> appendData(Collection<String> segmentNames, HashMap<String, ByteArrayOutputStream> segmentContents,
                                               HashMap<String, Long> lengths, StreamSegmentStore store) {
        return execute(createAppendDataRequests(segmentNames, segmentContents, lengths), store);
    }

    private Collection<AttributeUpdate> createAttributeUpdates() {
        return ATTRIBUTES.stream()
                .map(id -> new AttributeUpdate(id, AttributeUpdateType.Accumulate, 1))
                .collect(Collectors.toList());
    }

    private ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>> createMergeTransactionsRequests(
            HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
            HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {

        val result = new ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                result.add(store -> store.sealStreamSegment(transactionName, TIMEOUT)
                        .thenCompose(v -> store.mergeTransaction(transactionName, TIMEOUT)));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        return result;
    }

    private CompletableFuture<Void> mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths,
                                                      HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore store) throws Exception {
        return execute(createMergeTransactionsRequests(transactionsBySegment, lengths, segmentContents), store);
    }

    private ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>> createSealSegmentsRequests(Collection<String> segmentNames) {
        val result = new ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>>();
        for (String segmentName : segmentNames) {
            result.add(store -> Futures.toVoid(store.sealStreamSegment(segmentName, TIMEOUT)));
        }
        return result;
    }

    private CompletableFuture<Void> sealSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        return execute(createSealSegmentsRequests(segmentNames), store);
    }

    private CompletableFuture<Void> execute(ArrayList<Function<StreamSegmentStore, CompletableFuture<Void>>> requests, StreamSegmentStore store) {
        return Futures.allOf(requests.stream().map(r -> r.apply(store)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames, StreamSegmentStore store) {
        val result = new ArrayList<CompletableFuture<Void>>();
        for (String segmentName : segmentNames) {
            result.add(store.deleteStreamSegment(segmentName, TIMEOUT));
        }

        return Futures.allOf(result);
    }

    private ArrayList<String> createSegments(StreamSegmentStore store) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createStreamSegment(segmentName, null, TIMEOUT));
        }

        futures.add(store.createStreamSegment(EMPTY_SEGMENT_NAME, null, TIMEOUT));
        Futures.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, StreamSegmentStore store) {
        // Create the Transaction.
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            if (isEmptySegment(segmentName)) {
                continue;
            }

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

    private boolean isExpectedFencingException(Throwable ex) {
        return ex instanceof DataLogWriterNotPrimaryException
                || ex instanceof IllegalContainerStateException
                || ex instanceof ContainerNotFoundException
                || ex instanceof ObjectClosedException;
    }

    private boolean isEmptySegment(String segmentName) {
        return segmentName.equals(EMPTY_SEGMENT_NAME);
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

    private void checkSegmentStatus(HashMap<String, Long> segmentLengths, HashMap<String, Long> startOffsets,
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

                // Check attributes.
                val allAttributes = store.getAttributes(segmentName, ATTRIBUTES, true, TIMEOUT).join();
                for (UUID attributeId : ATTRIBUTES) {
                    Assert.assertEquals("Unexpected attribute value from getAttributes().",
                            EXPECTED_ATTRIBUTE_VALUE, (long) allAttributes.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));

                    if (Attributes.isCoreAttribute(attributeId)) {
                        // Core attributes must always be available from getInfo
                        Assert.assertEquals("Unexpected core attribute value from getInfo().",
                                EXPECTED_ATTRIBUTE_VALUE, (long) sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE));
                    } else {
                        val extAttrValue = sp.getAttributes().getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE);
                        Assert.assertTrue("Unexpected extended attribute value from getInfo()",
                                extAttrValue == Attributes.NULL_ATTRIBUTE_VALUE || extAttrValue == EXPECTED_ATTRIBUTE_VALUE);
                    }
                }
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

    private void checkReadsWhileTruncating(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> startOffsets,
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

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, StreamSegmentStore baseStore,
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

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
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
