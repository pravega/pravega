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

import io.pravega.segmentstore.storage.chunklayer.ChunkStorageException;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageFullException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.util.DiskChecker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for {@link HDFSChunkStorage} that uses mocks.
 */
public class HDFSChunkStorageMockTest extends ThreadPooledTestSuite {
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    private HDFSStorageConfig storageConfig;

    @Before
    public void setUp() throws Exception {
        this.storageConfig = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, "hdfs://dummy")
                .build();
    }

    @Test
    public void testStorageFull() throws Exception {
        val mockFs = spy(FileSystem.get(new Configuration()));
        HDFSChunkStorage storage = new MockHDFSChunkStorage(storageConfig, executorService(), mockFs);
        storage.initialize();
        doThrow(new QuotaByStorageTypeExceededException("No space left on device")).when(mockFs).getFileStatus(any());
        AssertExtensions.assertFutureThrows("should throw ChunkStorageFull exception",
                storage.getInfo("test"),
                ex -> ex instanceof ChunkStorageFullException);

        doThrow(new DiskChecker.DiskOutOfSpaceException("No space left on device")).when(mockFs).getFileStatus(any());
        AssertExtensions.assertFutureThrows("should throw ChunkStorageFull exception",
                storage.getInfo("test"),
                ex -> ex instanceof ChunkStorageFullException);
    }

    @Test
    public void testGetUsedException() throws Exception {
        val mockFs = spy(FileSystem.get(new Configuration()));
        HDFSChunkStorage storage = new MockHDFSChunkStorage(storageConfig, executorService(), mockFs);
        storage.initialize();
        doThrow(new IOException("Intentional")).when(mockFs).getUsed();
        AssertExtensions.assertFutureThrows("should throw ChunkStorageFull exception",
                storage.getUsedSpace(),
                ex -> ex instanceof ChunkStorageException);
    }

    private static class MockHDFSChunkStorage extends HDFSChunkStorage {
        FileSystem fs;
        public MockHDFSChunkStorage(HDFSStorageConfig config, Executor executor, FileSystem fs) {
            super(config, executor);
            this.fs = fs;
        }

        @Override
        protected FileSystem openFileSystem(Configuration conf) throws IOException {
            return fs;
        }
    }
}
