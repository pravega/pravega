/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test basic functionality of Bookkeeper commands.
 */
public class BookkeeperCommandsTest extends BookKeeperClusterTestCase {

    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    public BookkeeperCommandsTest() {
        super(3);
    }

    @Before
    public void setUp() throws Exception {
        baseConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        baseClientConf.setLedgerManagerFactoryClassName("org.apache.bookkeeper.meta.FlatLedgerManagerFactory");
        super.setUp();

        STATE.set(new AdminCommandState());
        Properties bkProperties = new Properties();
        bkProperties.setProperty("pravegaservice.container.count", "4");
        bkProperties.setProperty("pravegaservice.zk.connect.uri", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.ledger.path", "/ledgers");
        bkProperties.setProperty("bookkeeper.zk.metadata.path", "ledgers");
        bkProperties.setProperty("pravegaservice.clusterName", "");
        STATE.get().getConfigBuilder().include(bkProperties);
    }

    @After
    public void tierDown() {
        STATE.get().close();
    }

    @Test
    public void testBookKeeperListCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk list", STATE.get());
        Assert.assertTrue(commandResult.contains("log_summary") && commandResult.contains("logId\": 0"));
    }

    @Test
    public void testBookKeeperDetailsCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk details 0", STATE.get());
        Assert.assertTrue(commandResult.contains("log_summary") && commandResult.contains("logId\": 0"));
    }

    @Test
    public void testBookKeeperDisableAndEnableCommands() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk disable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("enabled\": false"));
        commandResult = TestUtils.executeCommand("bk enable 0", STATE.get());
        Assert.assertTrue(commandResult.contains("enabled\": true"));
    }

    @Test
    public void testBookKeeperCleanupCommand() throws Exception {
        createLedgerInBookkeeperTestCluster(0);
        String commandResult = TestUtils.executeCommand("bk cleanup", STATE.get());
        Assert.assertTrue(commandResult.contains("no Ledgers eligible for deletion"));
    }

    private void createLedgerInBookkeeperTestCluster(int logId) throws Exception {
        BookKeeperConfig bookKeeperConfig = BookKeeperConfig.builder().with(BookKeeperConfig.ZK_METADATA_PATH, "ledgers")
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers")
                .with(BookKeeperConfig.ZK_ADDRESS, zkUtil.getZooKeeperConnectString()).build();
        @Cleanup
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zkUtil.getZooKeeperConnectString(), new RetryOneTime(5000));
        curatorFramework.start();
        @Cleanup
        BookKeeperLogFactory bookKeeperLogFactory = new BookKeeperLogFactory(bookKeeperConfig, curatorFramework, Executors.newSingleThreadScheduledExecutor());
        bookKeeperLogFactory.initialize();
        @Cleanup
        DurableDataLog log = bookKeeperLogFactory.createDurableDataLog(logId);
        log.initialize(Duration.ofSeconds(5));
    }
}