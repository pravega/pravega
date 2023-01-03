/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.gcp;

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

/**
 * Unit tests for {@link GCPChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class GCPSimpleStorageTests extends SimpleStorageTests {
    private GCPTestContext testContext = null;

    @Override
    @Before
    public void before() throws Exception {
        this.testContext = new GCPTestContext();
        super.before();
    }

    @Override
    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected ChunkStorage getChunkStorage() {
        return new GCPChunkStorage(testContext.storage, testContext.adapterConfig, executorService());
    }

    @Override
    protected ChunkedSegmentStorageConfig getDefaultConfig() {
        return this.testContext.defaultConfig;
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link GCPChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class GCPStorageRollingTests extends ChunkedRollingStorageTests {
        private GCPTestContext testContext = null;

        @Before
        public void setUp() throws Exception {
            this.testContext = new GCPTestContext();
        }

        @After
        public void tearDown() throws Exception {
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            return new GCPChunkStorage(testContext.storage, testContext.adapterConfig, executorService());
        }

        @Override
        protected ChunkedSegmentStorageConfig getDefaultConfig() {
            return this.testContext.defaultConfig;
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link GCPChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class GCPChunkStorageTests extends ChunkStorageTests {
        private GCPTestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new GCPTestContext();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            super.after();
        }

        @Override
        protected ChunkStorage createChunkStorage() {
            return new GCPChunkStorage(testContext.storage, testContext.adapterConfig, executorService());
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
            assertFalse(getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link GCPChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class GCPChunkStorageSystemJournalTests extends SystemJournalTests {
        private GCPTestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new GCPTestContext();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            return new GCPChunkStorage(testContext.storage, testContext.adapterConfig, executorService());
        }
    }
}
