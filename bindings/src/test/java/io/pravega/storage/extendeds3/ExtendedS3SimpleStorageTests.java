/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import org.junit.After;
import org.junit.Before;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Unit tests for {@link ExtendedS3ChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class ExtendedS3SimpleStorageTests extends SimpleStorageTests {
    private ExtendedS3TestContext testContext = null;

    @Before
    public void before() throws Exception {
        super.before();
        this.testContext = new ExtendedS3TestContext();
    }

    @After
    public void after() throws Exception {
        if (this.testContext != null) {
            this.testContext.close();
        }
        super.after();
    }

    @Override
    protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
        this.testContext = new ExtendedS3TestContext();
        return new ExtendedS3ChunkStorageProvider(executor, testContext.client, testContext.adapterConfig);
    }

    /**
     * {@link ChunkManagerRollingTests} tests for {@link ExtendedS3ChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3StorageProviderRollingTests extends ChunkManagerRollingTests {
        private ExtendedS3TestContext testContext = null;

        @Before
        public void setUp() throws Exception {
            this.testContext = new ExtendedS3TestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
        }

        @Override
        protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return new ExtendedS3ChunkStorageProvider(executor, testContext.client, testContext.adapterConfig);
        }
    }

    /**
     * {@link ChunkStorageProviderTests} tests for {@link ExtendedS3ChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageProviderTests extends ChunkStorageProviderTests {
        private ExtendedS3TestContext testContext = null;

        @Before
        public void before() throws Exception {
            super.before();
            this.testContext = new ExtendedS3TestContext();
        }

        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            return new ExtendedS3ChunkStorageProvider(new ScheduledThreadPoolExecutor(getThreadPoolSize()), testContext.client, testContext.adapterConfig);
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link ExtendedS3ChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageSystemJournalTests extends SystemJournalTests {
        private ExtendedS3TestContext testContext = null;

        @Before
        public void before() throws Exception {
            super.before();
            this.testContext = new ExtendedS3TestContext();
        }

        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorageProvider getStorageProvider() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            return new ExtendedS3ChunkStorageProvider(executorService(), testContext.client, testContext.adapterConfig);
        }
    }
}
