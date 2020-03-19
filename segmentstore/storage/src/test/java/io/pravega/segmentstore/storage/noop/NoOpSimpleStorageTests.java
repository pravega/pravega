/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Unit tests for NoOpSimpleStorage using SimpleStorageTests.
 */
public class NoOpSimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider(Executor executor) throws IOException {
        return new NoOpChunkStorageProvider(executor);
    }

    protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
        return getChunkStorageProvider(executor);
    }

    @Override
    protected void populate(byte[] data) {
        // Do nothing keep data uninitialized.
    }

    /**
     * Unit tests for NoOpChunkStorageProvider using ChunkManagerRollingTests
     */
    public static class NoOpRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage(Executor executor)  throws Exception {
            return getChunkStorageProvider(executor);
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }

    /**
     * Unit tests for NoOpChunkStorageProvider using ChunkStorageProviderTests
     */
    public static class NoOpChunkStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider(new ScheduledThreadPoolExecutor(getThreadPoolSize()));
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }
}
