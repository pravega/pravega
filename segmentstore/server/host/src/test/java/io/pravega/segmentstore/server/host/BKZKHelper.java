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

import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.util.ArrayList;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Helper class for Segment store integration tests.
 * This class sets up the bookkeeper and zookeeper and sets up the correct values in config.
 */
public class BKZKHelper {

    private static final int BOOKIE_COUNT = 3;
    private final ServiceBuilderConfig.Builder configBuilder;
    private File baseDir = null;
    private BookKeeperServiceRunner bkRunner;
    @Getter
    private CuratorFramework zkClient;

    public BKZKHelper(ServiceBuilderConfig.Builder configBuilder) {
        this.configBuilder = configBuilder;
    }

    @SneakyThrows
    public void setUp() {
        // BookKeeper
        // Pick random ports to reduce chances of collisions during concurrent test executions.
        int zkPort = TestUtils.getAvailableListenPort();
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        this.bkRunner = BookKeeperServiceRunner.builder()
                                               .startZk(true)
                                               .zkPort(zkPort)
                                               .ledgersPath("/ledgers")
                                               .bookiePorts(bookiePorts)
                                               .build();
        this.bkRunner.start();

        // Create a ZKClient with a base namespace.
        String baseNamespace = "pravega/" + Long.toHexString(System.nanoTime());
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + zkPort)
                .namespace(baseNamespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .build();
        this.zkClient.start();

        // Attach a sub-namespace for the Container Metadata.
        String logMetaNamespace = "segmentstore/containers";
        this.configBuilder.include(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zkPort)
                .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers"));
    }

    @SneakyThrows
    public void tearDown() {
        // BookKeeper
        val bk = this.bkRunner;
        if (bk != null) {
            bk.close();
            this.bkRunner = null;
        }

        val zk = this.zkClient;
        if (zk != null) {
            zk.close();
            this.zkClient = null;
        }
    }
}
