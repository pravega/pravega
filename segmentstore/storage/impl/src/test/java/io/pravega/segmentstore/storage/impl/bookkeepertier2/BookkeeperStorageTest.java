/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.test.common.TestUtils;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BookkeeperStorageTest extends StorageTestBase {
    private static final int BOOKIE_COUNT = 1;
    private static final int THREAD_POOL_SIZE = 20;

    private static final AtomicReference<BookKeeperServiceRunner> BK_SERVICE = new AtomicReference<>();
    private static final AtomicInteger BK_PORT = new AtomicInteger();
    private static final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private static final AtomicReference<BookkeeperStorageConfig> config = new AtomicReference<>();

    @BeforeClass
    public static void Setup() throws Exception {
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        BK_PORT.set(TestUtils.getAvailableListenPort());
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        val runner = BookKeeperServiceRunner.builder()
                                            .startZk(true)
                                            .zkPort(BK_PORT.get())
                                            .ledgersPath("/pravega/bookkeeper/ledgers")
                                            .bookiePorts(bookiePorts)
                                            .build();
        runner.startAll();
        BK_SERVICE.set(runner);

        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + BK_PORT.get())
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build());
        zkClient.get().start();

        // Setup config to use the port and namespace.
        config.set(BookkeeperStorageConfig
                .builder()
                .with(BookkeeperStorageConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookkeeperStorageConfig.ZK_METADATA_PATH, namespace)
                .with(BookkeeperStorageConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .with(BookkeeperStorageConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookkeeperStorageConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookkeeperStorageConfig.BK_ACK_QUORUM_SIZE, 1)
                .with(BookkeeperStorageConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                .build());
    }

    @AfterClass
    public static void tearDownBookKeeper() throws Exception {
        val process = BK_SERVICE.getAndSet(null);
        if (process != null) {
            process.close();
        }
    }

    @Override
    public void testFencing() throws Exception {

    }

    @Override
    protected Storage createStorage() {
        return new BookkeeperStorage(config.get(), zkClient.get(), executorService());
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        if (readOnly) {
            return BookkeeperSegmentHandle.readHandle(segmentName);
        } else {
            return BookkeeperSegmentHandle.writeHandle(segmentName);
        }
    }
}