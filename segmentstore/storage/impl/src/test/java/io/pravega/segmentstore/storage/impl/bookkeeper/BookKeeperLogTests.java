/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogTestBase;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for BookKeeperLog. These require that a compiled BookKeeper distribution exists on the local
 * filesystem. It starts up the local sandbox and uses that for testing purposes.
 */
public class BookKeeperLogTests extends DurableDataLogTestBase {
    //region Setup, Config and Cleanup

    private static final int CONTAINER_ID = 9999;
    private static final int WRITE_COUNT = 500;
    private static final int BOOKIE_COUNT = 3;
    private static final int THREAD_POOL_SIZE = 20;

    private static final AtomicReference<BookKeeperServiceRunner> BK_SERVICE = new AtomicReference<>();
    private static final AtomicInteger BK_PORT = new AtomicInteger();
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private final AtomicReference<BookKeeperConfig> config = new AtomicReference<>();
    private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private final AtomicReference<BookKeeperLogFactory> factory = new AtomicReference<>();

    /**
     * Start BookKeeper once for the duration of this class. This is pretty strenuous, so in the interest of running time
     * we only do it once.
     */
    @BeforeClass
    public static void setUpBookKeeper() throws Exception {
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
        runner.start();
        BK_SERVICE.set(runner);
    }

    @AfterClass
    public static void tearDownBookKeeper() throws Exception {
        val process = BK_SERVICE.getAndSet(null);
        if (process != null) {
            process.close();
        }
    }

    /**
     * Before each test, we create a new namespace; this ensures that data created from a previous test does not leak
     * into the current one (namespaces cannot be deleted (at least not through the API)).
     */
    @Before
    public void setUp() throws Exception {
        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + BK_PORT.get())
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build());
        this.zkClient.get().start();

        // Setup config to use the port and namespace.
        this.config.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookKeeperConfig.MAX_CONCURRENT_WRITES, 10)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, WRITE_MAX_LENGTH * Math.max(10, WRITE_COUNT / 100)) // Very frequent rollovers.
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .build());

        // Create default factory.
        val factory = new BookKeeperLogFactory(this.config.get(), this.zkClient.get(), executorService());
        factory.initialize();
        this.factory.set(factory);
    }

    @After
    public void tearDown() throws Exception {
        val factory = this.factory.getAndSet(null);
        if (factory != null) {
            factory.close();
        }

        val zkClient = this.zkClient.getAndSet(null);
        if (zkClient != null) {
            zkClient.close();
        }
    }

    @Test
    public void testCreateDefaultConfig() throws Exception {
        BookKeeperConfig bkConfig = BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, WRITE_MAX_LENGTH * 10) // Very frequent rollovers.
                .with(BookKeeperConfig.ZK_METADATA_PATH, this.zkClient.get().getNamespace())
                .build();
        @Cleanup
        val factory = new BookKeeperLogFactory(bkConfig, this.zkClient.get(), executorService());
        AssertExtensions.assertThrows("",
                factory::initialize,
                ex -> ex instanceof DataLogNotAvailableException &&
                        ex.getCause() instanceof KeeperException.NoNodeException &&
                        ex.getCause().getMessage().
                                indexOf(this.zkClient.get().getNamespace() + "/bookkeeper/ledgers/available") > 0
        );
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    //endregion

    //region DurableDataLogTestBase implementation

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.get().createDurableDataLog(CONTAINER_ID);
    }

    @Override
    protected DurableDataLog createDurableDataLog(Object sharedContext) {
        return createDurableDataLog(); // Nothing different for shared context.
    }

    @Override
    protected Object createSharedContext() {
        return null; // No need for shared context.
    }

    @Override
    protected LogAddress createLogAddress(long seqNo) {
        return new LedgerAddress(seqNo, seqNo);
    }

    @Override
    protected int getWriteCount() {
        return WRITE_COUNT;
    }

    //endregion
}