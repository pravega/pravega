/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import com.google.common.collect.Iterators;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
@Slf4j
public class BookKeeperStorageIntegrationTest extends BookKeeperIntegrationTestBase {

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.configBuilder.include(BookKeeperStorageConfig
                .builder()
                .with(BookKeeperStorageConfig.ZK_ADDRESS, bookkeeper.getZkClient().getZookeeperClient().getCurrentConnectionString())
                .with(BookKeeperStorageConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperStorageConfig.BK_LEDGER_PATH, "/ledgers")
                .with(BookKeeperStorageConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                .with(BookKeeperStorageConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
        );
    }

    /**
     * Shuts down BookKeeper.
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
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
    @Override
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
             val readOnlyBuilder = builder) {
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
             val readOnlyBuilder = builder) {
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
     * Tests an end-to-end scenario for the SegmentStore where operations are continuously executed while the SegmentStore
     * itself is being fenced out by new instances. The difference between this and testEndToEnd() is that this does not
     * do a graceful shutdown of the Segment Store, instead it creates a new instance while the previous one is still running.
     *
     * @throws Exception If an exception occurred.
     */
    @Override
    @Test
    public void testEndToEndWithFencing() throws Exception {
        log.info("Starting.");
        try (val context = new FencingTestContext()) {
            // Create first instance (this is a one-off so we can bootstrap the test).
            context.createNewInstance();

            // Create the StreamSegments and their transactions.
            val segmentNames = createSegments(context.getActiveStore());
            val segmentsAndTransactions = new ArrayList<String>(segmentNames);
            log.info("Created Segments: {}.", String.join(", ", segmentNames));
            val transactionsBySegment = createTransactions(segmentNames, context.getActiveStore());
            transactionsBySegment.values().forEach(segmentsAndTransactions::addAll);
            log.info("Created Transactions: {}.", transactionsBySegment.values().stream().flatMap(Collection::stream).collect(Collectors.joining(", ")));

            // Generate all the requests.
            HashMap<String, Long> lengths = new HashMap<>();
            HashMap<String, Long> startOffsets = new HashMap<>();
            HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
            val appends = createAppendDataRequests(segmentsAndTransactions, segmentContents, lengths,
                    applyFencingMultiplier(ATTRIBUTE_UPDATES_PER_SEGMENT), applyFencingMultiplier(APPENDS_PER_SEGMENT));
            val mergers = createMergeTransactionsRequests(transactionsBySegment, lengths, segmentContents);
            val seals = createSealSegmentsRequests(segmentNames);
            Iterator<StreamSegmentStoreTestBase.StoreRequest> requests = Iterators.concat(appends.iterator(), mergers.iterator(), seals.iterator());

            // Calculate how frequently to create a new instance of the Segment Store.
            int newInstanceFrequency = (appends.size() + mergers.size() + seals.size()) / applyFencingMultiplier(MAX_INSTANCE_COUNT);
            log.info("Creating a new Segment Store instance every {} operations.", newInstanceFrequency);

            // Execute all the requests.
            CompletableFuture<Void> operationCompletions = executeWithFencing(requests, newInstanceFrequency, context);

            // Wait for our operations to complete.
            operationCompletions.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Wait for the instance creations to be done (this will help surface any exceptions coming from this).
            context.awaitAllInitializations().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Check reads.
            checkReads(segmentContents, context.getActiveStore());
            log.info("Finished checking reads.");

            try (val readOnlyBuilder = createBuilder(Integer.MAX_VALUE - 1)) {
                waitForSegmentsInStorage(segmentNames, context.getActiveStore(), readOnlyBuilder.createStreamSegmentService())
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                log.info("Finished waiting for segments in Storage.");
            }

            // Delete everything.
            deleteSegments(segmentNames, context.getActiveStore()).join();
            log.info("Finished deleting segments.");
            checkSegmentStatus(lengths, startOffsets, true, true, context.getActiveStore());
        }

        log.info("Finished.");
    }
    //region StreamSegmentStoreTestBase Implementation
    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder configBuilder, int instanceId) {
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> new BookKeeperStorageFactory(setup.getConfig(BookKeeperStorageConfig::builder), bookkeeper.getZkClient(), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), bookkeeper.getZkClient(), setup.getCoreExecutor()));

    }

    //endregion
}