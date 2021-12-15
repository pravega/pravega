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
package io.pravega.storage.s3;

import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link S3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class S3SimpleStorageTests extends SimpleStorageTests {
    private S3TestContext testContext = null;

    @Override
    @Before
    public void before() throws Exception {
        this.testContext = new S3TestContext();
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
        return new S3ChunkStorage(testContext.s3Client, testContext.adapterConfig, executorService(), false);
    }

    @Override
    protected ChunkedSegmentStorageConfig getDefaultConfig() {
        return this.testContext.defaultConfig;
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link S3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class S3StorageRollingTests extends ChunkedRollingStorageTests {
        private S3TestContext testContext = null;

        @Before
        public void setUp() throws Exception {
            this.testContext = new S3TestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            return new S3ChunkStorage(testContext.s3Client, testContext.adapterConfig, executorService(), false);
        }

        @Override
        protected ChunkedSegmentStorageConfig getDefaultConfig() {
            return this.testContext.defaultConfig;
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link S3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class S3ChunkStorageTests extends ChunkStorageTests {
        private S3TestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new S3TestContext();
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
            return new S3ChunkStorage(testContext.s3Client, testContext.adapterConfig, executorService(), false);
        }

        @Override
        protected int getMinimumConcatSize() {
            return Math.max(1, Math.toIntExact(this.testContext.defaultConfig.getMinSizeLimitForConcat()));
        }

        /**
         * Test default capabilities.
         */
        @Override
        @Test
        public void testCapabilities() {
            assertFalse(getChunkStorage().supportsAppend());
            assertFalse(getChunkStorage().supportsTruncation());
            assertTrue(getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link S3ChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class S3ChunkStorageSystemJournalTests extends SystemJournalTests {
        private S3TestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new S3TestContext();
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
            return new S3ChunkStorage(testContext.s3Client, testContext.adapterConfig, executorService(), false);
        }
    }
}
