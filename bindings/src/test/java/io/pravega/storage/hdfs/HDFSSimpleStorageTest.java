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
package io.pravega.storage.hdfs;

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import lombok.Getter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

/***
 * Unit tests for {@link HDFSChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class HDFSSimpleStorageTest extends SimpleStorageTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private TestContext testContext = new TestContext(executorService());

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        testContext.setUp();
    }

    @Override
    @After
    public void after() throws Exception {
        testContext.tearDown();
        super.after();
    }

    @Override
    protected ChunkStorage getChunkStorage()  throws Exception {
        return testContext.getChunkStorage(executorService());
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link HDFSChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSRollingTests extends ChunkedRollingStorageTests {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private TestContext testContext = new TestContext(executorService());

        @Override
        @Before
        public void before() throws Exception {
            super.before();
            testContext.setUp();
        }

        @Override
        @After
        public void after() throws Exception {
            testContext.tearDown();
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage()  throws Exception {
            return testContext.getChunkStorage(executorService());
        }
    }

    /**
     * {@link ChunkStorageTests} tests for {@link HDFSChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSChunkStorageTests extends ChunkStorageTests {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private TestContext testContext = new TestContext(executorService());

        @Override
        @Before
        public void before() throws Exception {
            testContext.setUp();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            super.after();
            testContext.tearDown();
        }

        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return testContext.getChunkStorage(executorService());
        }

        /**
         * Test default capabilities.
         */
        @Override
        @Test
        public void testCapabilities() {
            assertEquals(true, getChunkStorage().supportsAppend());
            assertEquals(false, getChunkStorage().supportsTruncation());
            assertEquals(true, getChunkStorage().supportsConcat());
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link HDFSChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class HDFSChunkStorageSystemJournalTests extends SystemJournalTests {
        private TestContext testContext = new TestContext(executorService());

        @Override
        @Before
        public void before() throws Exception {
            testContext.setUp();
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            testContext.tearDown();
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() throws Exception {
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

        private Executor executor;

        private TestContext(Executor executor) {
            this.executor = executor;
        }

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

        private ChunkStorage getChunkStorage(Executor executor)  throws Exception {
            return new HDFSChunkStorage(adapterConfig, executor);
        }
    }
}
