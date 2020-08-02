/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class FileSystemSimpleStorageTest extends SimpleStorageTests {
    private static ChunkStorage newChunkStorage() throws IOException {
        File baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        return new FileSystemChunkStorage(FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .build());
    }

    protected ChunkStorage getChunkStorage()  throws Exception {
        return newChunkStorage();
    }

    /**
     * {@link ChunkedRollingStorageTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemRollingTests extends ChunkedRollingStorageTests {
        protected ChunkStorage getChunkStorage()  throws Exception {
            return newChunkStorage();
        }

    }

    /**
     * {@link ChunkStorageTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return newChunkStorage();
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
     * {@link SystemJournalTests} tests for {@link FileSystemChunkStorage} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        protected ChunkStorage getChunkStorage() throws Exception {
            return FileSystemSimpleStorageTest.newChunkStorage();
        }
    }
}
