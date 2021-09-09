/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.extendeds3;

import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class ExtendedS3SimpleStorageTests extends SimpleStorageTests {
    private ExtendedS3TestContext testContext = null;

    @Override
    @Before
    public void before() throws Exception {
        this.testContext = new ExtendedS3TestContext();
        super.before();
    }

    @Override
    @After
    public void after() throws Exception {
        if (this.testContext != null) {
            this.testContext.close();
        }
        super.after();
    }

    @Override
    protected ChunkStorage getChunkStorage() {
        return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), true, false);
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3StorageRollingTests extends ChunkedRollingStorageTests {
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
        protected ChunkStorage getChunkStorage() {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), true, false);
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageTests extends ChunkStorageTests {
        private ExtendedS3TestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorage createChunkStorage() {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), true, false);
        }

        /**
         * Test default capabilities.
         */
        @Override
        @Test
        public void testCapabilities() {
            assertTrue(getChunkStorage().supportsAppend());
            assertFalse(getChunkStorage().supportsTruncation());
            assertTrue(getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ExtendedS3ChunkStorageSystemJournalTests extends SystemJournalTests {
        private ExtendedS3TestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            return new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), true, false);
        }
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class NoAppendExtendedS3StorageRollingTests extends ChunkedRollingStorageTests {
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
        protected ChunkStorage getChunkStorage() {
            val ret = new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), false, false);
            return ret;
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link ExtendedS3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class NoAppendExtendedS3ChunkStorageSystemJournalTests extends SystemJournalTests {
        private ExtendedS3TestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new ExtendedS3TestContext();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            val ret = new ExtendedS3ChunkStorage(testContext.client, testContext.adapterConfig, executorService(), false, false);
            return ret;
        }
    }
}
