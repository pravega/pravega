/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeperstorage;

import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class BookKeeperStorageTest extends StorageTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    private final int bookieCount = 1;
    private final int threadPoolSize = 20;
    private final AtomicInteger bkPort = new AtomicInteger();

    private BookKeeperServiceRunner bookKeeperServiceRunner = null;
    private CuratorFramework zkClient = null;
    private BookKeeperStorageConfig bookKeeperStorageConfig = null;


    @Before
    public void setUp() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        bkPort.set(TestUtils.getAvailableListenPort());
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < bookieCount; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        val runner = BookKeeperServiceRunner.builder()
                                            .startZk(true)
                                            .zkPort(bkPort.get())
                                            .ledgersPath("/pravega/bookkeeper/ledgers")
                                            .bookiePorts(bookiePorts)
                                            .build();
        runner.startAll();
        bookKeeperServiceRunner = runner;

        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + bkPort.get())
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        zkClient.start();

        // Setup config to use the port and namespace.
        bookKeeperStorageConfig = BookKeeperStorageConfig
                .builder()
                .with(BookKeeperStorageConfig.ZK_ADDRESS, "localhost:" + bkPort.get())
                .with(BookKeeperStorageConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperStorageConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .with(BookKeeperStorageConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperStorageConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperStorageConfig.BK_ACK_QUORUM_SIZE, 1)
                .with(BookKeeperStorageConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                .build();
    }

    @After
    public void tearDownBookKeeper() throws Exception {
        val process = bookKeeperServiceRunner;
        if (process != null) {
            process.close();
        }
    }

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Test
    @Override
    public void testFencing() throws Exception {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute only read-only operations.
            verifyWriteOperationsFail(handle1, storage1);
            //verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsFail(handle1, storage1);
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    @Override
    protected void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength(), concatHandle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    @Test
    public void getLedgerDataForWriteAt() throws StreamSegmentException {
        LogStorageManager manager = new LogStorageManager(bookKeeperStorageConfig, zkClient);
        manager.initialize(0);
        manager.create("name");

        LogStorage storage = new LogStorage("name", 0, 1, true);

        AssertExtensions.assertThrows("LogStorage should throw exception at wrong offset",
                () -> manager.getLedgerDataForWriteAt(storage, 10),
                exc -> exc instanceof BadOffsetException);
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new BookKeeperStorage(bookKeeperStorageConfig, zkClient), executorService());
    }

    @Test
    public void unSupportedOps() {
        final String segmentName = "segment";
        try (val storage1 = new BookKeeperStorage(bookKeeperStorageConfig, zkClient)) {
            AssertExtensions.assertThrows("",
                    () -> storage1.unseal(BookKeeperSegmentHandle.readHandle("temp")),
                    exc -> exc instanceof UnsupportedOperationException);
            AssertExtensions.assertThrows("",
                    () -> storage1.truncate(BookKeeperSegmentHandle.readHandle("temp"), 0),
                    exc -> exc instanceof UnsupportedOperationException);
        }
    }
}