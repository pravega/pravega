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
package io.pravega.storage.ecschunk;

import io.pravega.segmentstore.storage.chunklayer.*;
import io.pravega.storage.chunk.ECSChunkObjectStorage;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class ECSChunkStorageTests extends SimpleStorageTests {
    private ECSChunkTestContext testContext = null;

    @Override
    @Before
    public void before() throws Exception {
        this.testContext = new ECSChunkTestContext();
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
        return new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ECSStorageRollingTests extends ChunkedRollingStorageTests {
        private ECSChunkTestContext testContext = null;

        @Before
        public void setUp() throws Exception {
            this.testContext = new ECSChunkTestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            return new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ECSChunkStorageTestSuite extends ChunkStorageTests {
        private ECSChunkTestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new ECSChunkTestContext();
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
            return new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
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
     * {@link SystemJournalTests} tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class ECSChunkStorageSystemJournalTests extends SystemJournalTests {
        private ECSChunkTestContext testContext = null;

        @Override
        @Before
        public void before() throws Exception {
            this.testContext = new ECSChunkTestContext();
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
            return new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
        }
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class NoAppendECSStorageRollingTests extends ChunkedRollingStorageTests {
        private ECSChunkTestContext testContext = null;

        @Before
        public void setUp() throws Exception {
            this.testContext = new ECSChunkTestContext();
        }

        @After
        public void tearDown() throws Exception {
            if (this.testContext != null) {
                this.testContext.close();
            }
        }

        @Override
        protected ChunkStorage getChunkStorage() {
            val ret = new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
            return ret;
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link ECSChunkObjectStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class NoAppendECSChunkStorageSystemJournalTests extends SystemJournalTests {
        private ECSChunkTestContext testContext = null;

        @Before
        public void before() throws Exception {
            this.testContext = new ECSChunkTestContext();
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
        protected ChunkStorage getChunkStorage() {
            val ret = new ECSChunkObjectStorage(testContext.clients, testContext.adapterConfig, executorService());
            return ret;
        }
    }
}
