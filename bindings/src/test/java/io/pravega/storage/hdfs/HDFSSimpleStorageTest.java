/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import lombok.Getter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/***
 * Unit tests for {@link HDFSChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class HDFSSimpleStorageTest extends SimpleStorageTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private TestContext testContext = new TestContext();

    @Before
    public void before() throws Exception {
        super.before();
        testContext.setUp();
    }

    @After
    public void after() throws Exception {
        testContext.tearDown();
        super.after();
    }

    protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
        return testContext.getChunkStorage(executor);
    }

    /**
     * {@link ChunkManagerRollingTests} tests for {@link HDFSChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSRollingTests extends ChunkManagerRollingTests {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private TestContext testContext = new TestContext();

        @Before
        public void before() throws Exception {
            super.before();
            testContext.setUp();
        }

        @After
        public void after() throws Exception {
            testContext.tearDown();
            super.after();
        }

        protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return testContext.getChunkStorage(executor);
        }
    }

    /**
     * {@link ChunkStorageProviderTests} tests for {@link HDFSChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSChunkStorageProviderTests extends ChunkStorageProviderTests {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private TestContext testContext = new TestContext();

        @Before
        public void before() throws Exception {
            testContext.setUp();
            super.before();
        }

        @After
        public void after() throws Exception {
            testContext.tearDown();
            super.after();
        }

        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return testContext.getChunkStorage(new ScheduledThreadPoolExecutor(getThreadPoolSize()));
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link HDFSChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSChunkStorageSystemJournalTests extends SystemJournalTests {
        private TestContext testContext = new TestContext();

        @Before
        public void before() throws Exception {
            testContext.setUp();
            super.before();
        }

        @After
        public void after() throws Exception {
            testContext.tearDown();
            super.after();
        }

        @Override
        protected ChunkStorageProvider getStorageProvider() throws Exception {
            return testContext.getChunkStorage(executorService());
        }
    }

    /**
     * Test context.
     */
    private static class TestContext {
        @Getter
        private File baseDir = null;

        @Getter
        private MiniDFSCluster hdfsCluster = null;

        @Getter
        private HDFSStorageConfig adapterConfig = null;

        private void setUp() throws Exception {
            this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
            this.adapterConfig = HDFSStorageConfig
                    .builder()
                    .with(HDFSStorageConfig.REPLICATION, 1)
                    .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                    .build();
        }

        private void tearDown() {
            if (hdfsCluster != null) {
                hdfsCluster.shutdown();
                hdfsCluster = null;
                FileHelpers.deleteFileOrDirectory(baseDir);
                baseDir = null;
            }
        }

        private ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return new HDFSChunkStorageProvider(executor, adapterConfig);
        }
    }
}
