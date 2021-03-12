/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class FileSystemIntegrationTest extends BookKeeperIntegrationTestBase {
    /**
     * Starts BookKeeper.
     */
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.configBuilder.include(FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, getBaseDir().getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder configBuilder, int instanceId, boolean useChunkedSegmentStorage) {
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);

        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withStorageFactory(setup -> useChunkedSegmentStorage ?
                        new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                                .journalSnapshotCheckpointFrequency(Duration.ofMillis(1))
                                .selfCheckEnabled(true)
                                .build(),
                                setup.getConfig(FileSystemStorageConfig::builder),
                                setup.getStorageExecutor())
                        : new FileSystemStorageFactory(setup.getConfig(FileSystemStorageConfig::builder), setup.getStorageExecutor())
                )
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }

    /**
     * SegmentStore is used to create some segments, write data to them and let them flush to the storage.
     * This test only uses this storage to restore the container metadata segments in a new durable data log. Segment
     * properties are matched for verification after the restoration.
     * @throws Exception If an exception occurred.
     */
    @Test
    public void testDataRecovery() throws Exception {
        testSegmentRestoration();
    }
}
