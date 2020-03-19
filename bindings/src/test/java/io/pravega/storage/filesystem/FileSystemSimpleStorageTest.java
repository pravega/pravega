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

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;
import io.pravega.segmentstore.storage.chunklayer.SystemJournalTests;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Unit tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class FileSystemSimpleStorageTest extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider(Executor executor) throws IOException {
        File baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        return new FileSystemChunkStorageProvider(executor, FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .build());
    }

    protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
        return getChunkStorageProvider(executor);
    }

    /**
     * {@link ChunkManagerRollingTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return getChunkStorageProvider(executor);
        }

    }

    /**
     * {@link ChunkStorageProviderTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider(new ScheduledThreadPoolExecutor(getThreadPoolSize()));
        }
    }

    /**
     * {@link SystemJournalTests} tests for {@link FileSystemChunkStorageProvider} based {@link io.pravega.segmentstore.storage.Storage}.
     */
    public static class FileSystemChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        protected ChunkStorageProvider getStorageProvider() throws Exception {
            return FileSystemSimpleStorageTest.getChunkStorageProvider(executorService());
        }
    }
}
