/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.AdminCommandState;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.cli.admin.TestUtils;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        bkProperties.setProperty("pravegaservice.containerCount", "4");
        bkProperties.setProperty("pravegaservice.zkURL", zkUtil.getZooKeeperConnectString());
        bkProperties.setProperty("bookkeeper.bkLedgerPath", "/ledgers");
        STATE.get().getConfigBuilder().include(bkProperties);
    }

    @Test
    public void testBookKeeperListCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("bk list", STATE.get());
        System.err.println(commandResult);
        Assert.assertTrue(commandResult.contains("log_no_metadata"));
    }

    @Test
    public void testBookKeeperDetailsCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("bk details 0", STATE.get());
        System.err.println(commandResult);
        Assert.assertTrue(commandResult.contains("log_no_metadata"));
    }
}