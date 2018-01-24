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
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class BookKeeperStorageIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 1;
    private BookKeeperRunner bookkeeper = null;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

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
        bookkeeper.close();
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
            //val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            checkReads(segmentContents, segmentStore);

            // Wait for all the data to move to Storage.
            waitForSegmentsInStorage(segmentNames, segmentStore, segmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkStorage(segmentContents, segmentStore, segmentStore);
            checkReadsWhileTruncating(segmentContents, startOffsets, segmentStore);
            checkStorage(segmentContents, segmentStore, segmentStore);
        }

        // Phase 4: Force a recovery, seal segments and then delete them.
        try (val builder = createBuilder();
             val readOnlyBuilder = createReadOnlyBuilder()) {
            val segmentStore = builder.createStreamSegmentService();
            //val readOnlySegmentStore = readOnlyBuilder.createStreamSegmentService();

            // Seals.
            sealSegments(segmentNames, segmentStore).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            checkSegmentStatus(lengths, startOffsets, true, false, segmentStore);

            waitForSegmentsInStorage(segmentNames, segmentStore, segmentStore)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Deletes.
            deleteSegments(segmentNames, segmentStore).join();
            checkSegmentStatus(lengths, startOffsets, true, true, segmentStore);
        }
    }
    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> new BookKeeperStorageFactory(setup.getConfig(BookKeeperStorageConfig::builder), bookkeeper.getZkClient(), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), bookkeeper.getZkClient(), setup.getCoreExecutor()));
    }

    //endregion
}