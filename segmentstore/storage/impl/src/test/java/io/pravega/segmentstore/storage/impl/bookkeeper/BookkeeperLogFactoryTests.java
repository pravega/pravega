/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.Timer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Test to exercise the re-creation of the Bookkeeper client upon successive log creation attempts.
 */
public class BookkeeperLogFactoryTests extends BookKeeperClusterTestCase {

    private ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(2, "test");

    public BookkeeperLogFactoryTests() {
        super(1);
    }

    @Before
    public void setUp() throws Exception {
        this.baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        this.baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        boolean successfulSetup = false;
        while (interfaces.hasMoreElements()) {
            try {
                super.setUp();
                successfulSetup = true;
                break;
            } catch (Exception e) {
                // On some environments, using default interface does not allow to resolve the host name. We keep
                // iterating over existing interfaces to start the Bookkeeper cluster.
                super.tearDown();
                this.baseConf.setListeningInterface(interfaces.nextElement().getName());
            }
        }
        assert successfulSetup;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ExecutorServiceHelpers.shutdown(executorService);
    }

    @Test
    public void testBookkeeperClientReCreation() throws Exception {
        BookKeeperConfig bookKeeperConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers")
                .with(BookKeeperConfig.ZK_METADATA_PATH, "ledgers")
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.ZK_ADDRESS, zkUtil.getZooKeeperConnectString())
                .build();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3, 10000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(30000)
                .build();
        zkClient.start();
        BookKeeperLogFactory factory = new BookKeeperLogFactory(bookKeeperConfig, zkClient, this.executorService);
        // Initialize the log, and therefore the Bookkeeper client.
        factory.initialize();
        // Set a timer with a longer period than the inspection period to allow client re-creation.
        factory.getLastBookkeeperClientReset().set(new FakeTimer());
        BookKeeper oldBookkeeperClient = factory.getBookKeeperClient();
        // Create a log the first time.
        Assert.assertNull(factory.getLogInitializationTracker().get(0));
        factory.createDebugLogWrapper(0);
        // The first time we create the log the Bookkeeper client should be the same and the record for this log should
        // be initialized.
        Assert.assertEquals(oldBookkeeperClient, factory.getBookKeeperClient());
        Assert.assertNotNull(factory.getLogInitializationTracker().get(0));
        // From this point onwards, the second attempt to create the same log within the inspection period should lead
        // to a Bookkeeper client recreation.
        factory.createDebugLogWrapper(0);
        Assert.assertEquals(oldBookkeeperClient, factory.getBookKeeperClient());
        factory.createDebugLogWrapper(0);
        Assert.assertNotEquals(oldBookkeeperClient, factory.getBookKeeperClient());
        // Get a reference to the new Bookkeeper client.
        oldBookkeeperClient = factory.getBookKeeperClient();
        // The timer for this log should have been updated, so even if there are more initialization attempts, they should
        // not lead to a new Bookkeeper client re-creation until the inspection period expires.
        factory.createDebugLogWrapper(0);
        Assert.assertEquals(oldBookkeeperClient, factory.getBookKeeperClient());
        factory.createDebugLogWrapper(0);
        Assert.assertEquals(oldBookkeeperClient, factory.getBookKeeperClient());
    }

    static class FakeTimer extends Timer {

        @Override
        public long getElapsedNanos() {
            return Long.MAX_VALUE;
        }

    }

}
