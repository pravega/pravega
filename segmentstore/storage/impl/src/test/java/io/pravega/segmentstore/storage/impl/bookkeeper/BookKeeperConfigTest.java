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

import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the BookKeeperConfig class.
 */
public class BookKeeperConfigTest {

    @Test
    public void testDefaultValues() {
        BookKeeperConfig cfg = BookKeeperConfig.builder()
                .build();
        Assert.assertEquals("localhost:2181", cfg.getZkAddress());
        Assert.assertEquals(Duration.ofMillis(10000), cfg.getZkSessionTimeout());
        Assert.assertEquals(Duration.ofMillis(10000), cfg.getZkConnectionTimeout());
        Assert.assertEquals("/segmentstore/containers", cfg.getZkMetadataPath());
        Assert.assertEquals(2, cfg.getZkHierarchyDepth());
        Assert.assertEquals(5, cfg.getMaxWriteAttempts());
        Assert.assertEquals(3, cfg.getBkEnsembleSize());
        Assert.assertEquals(2, cfg.getBkAckQuorumSize());
        Assert.assertEquals(3, cfg.getBkWriteQuorumSize());
        Assert.assertEquals(5000, cfg.getBkWriteTimeoutMillis());
        Assert.assertEquals(5000, cfg.getBkReadTimeoutMillis());
        Assert.assertEquals(1024 * 1024 * 1024, cfg.getBkLedgerMaxSize());
        Assert.assertEquals(0, cfg.getBKPassword().length);
        Assert.assertEquals("", cfg.getBkLedgerPath());
        Assert.assertEquals(false, cfg.isTLSEnabled());
        Assert.assertEquals("config/client.truststore.jks", cfg.getTlsTrustStore());
        Assert.assertEquals("", cfg.getTlsTrustStorePasswordPath());
    }

    @Test
    public void testZkServers() {
        // When multiple servers are specified and separated by comma, it should replace it by semicolon
        BookKeeperConfig cfg1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "foo:12345,bar:54321")
                .build();
        Assert.assertEquals("foo:12345;bar:54321", cfg1.getZkAddress());

        // No changes should be made to the following configs
        BookKeeperConfig cfg2 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "foo:12345")
                .build();
        Assert.assertEquals("foo:12345", cfg2.getZkAddress());

        BookKeeperConfig cfg3 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "10.20.30.40:12345;bar:2181")
                .build();
        Assert.assertEquals("10.20.30.40:12345;bar:2181", cfg3.getZkAddress());
    }

    @Test
    public void testQuorumSize() {
        AssertExtensions.assertThrows("BookKeeperConfig did not throw InvalidPropertyValueException",
                () -> BookKeeperConfig.builder()
                        .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 3)
                        .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 2)
                        .build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }

    @Test
    public void testZkHierarchyDepth() {
        AssertExtensions.assertThrows("BookKeeperConfig did not throw InvalidPropertyValueException",
                () -> BookKeeperConfig.builder()
                        .with(BookKeeperConfig.ZK_HIERARCHY_DEPTH, -1)
                        .build(),
                ex -> ex instanceof InvalidPropertyValueException);
    }
}
