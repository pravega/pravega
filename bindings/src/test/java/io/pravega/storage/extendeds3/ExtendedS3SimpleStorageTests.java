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

import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class ExtendedS3SimpleStorageTests extends SimpleStorageTests {
    private ExtendedS3TestContext testContext = null;

    @Before
    public void before() throws Exception {
        this.testContext = new ExtendedS3TestContext();
        super.before();
    }

    @After
    public void after() throws Exception {
        if (this.testContext != null) {
            this.testContext.close();
        }
        super.after();
    }

    @Override
    protected ChunkStorage getChunkStorage()  throws Exception {
        return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig);
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3StorageProviderRollingTests extends ChunkedRollingStorageTests {
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
        protected ChunkStorage getChunkStorage()  throws Exception {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig);
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageTests extends ChunkStorageTests {
        private ExtendedS3TestContext testContext = null;

        @Before
        public void before() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            super.before();
        }

        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig);
        }

        /**
         * Test default capabilities.
         */
        @Test
        public void testCapabilities() {
            assertEquals(true, getChunkStorage().supportsAppend());
            assertEquals(false, getChunkStorage().supportsTruncation());
            assertEquals(true, getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageSystemJournalTests extends SystemJournalTests {
        private ExtendedS3TestContext testContext = null;

        @Before
        public void before() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            super.before();
        }

        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() throws Exception {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig);
        }
    }
}
