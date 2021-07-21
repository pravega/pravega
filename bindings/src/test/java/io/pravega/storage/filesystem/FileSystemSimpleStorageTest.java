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
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class FileSystemSimpleStorageTest extends SimpleStorageTests {
    private static ChunkStorage newChunkStorage(Executor executor) throws IOException {
        File baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        return new FileSystemChunkStorage(FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .build(),
                executor);
    }

    @Override
    protected ChunkStorage getChunkStorage()  throws Exception {
        return newChunkStorage(executorService());
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemRollingTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage()  throws Exception {
            return newChunkStorage(executorService());
        }

    }

    /**
     * {@link ChunkStorageTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return newChunkStorage(executorService());
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
     * {@link SystemJournalTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        protected ChunkStorage getChunkStorage() throws Exception {
            return FileSystemSimpleStorageTest.newChunkStorage(executorService());
        }
    }
}
