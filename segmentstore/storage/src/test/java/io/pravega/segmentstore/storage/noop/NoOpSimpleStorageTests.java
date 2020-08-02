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

import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;

import java.io.IOException;

/**
 * Unit tests for {@link NoOpChunkStorage} using {@link SimpleStorageTests}.
 */
public class NoOpSimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorage getChunkStorageProvider() throws IOException {
        return new NoOpChunkStorage();
    }

    protected ChunkStorage getChunkStorage() throws Exception {
        return getChunkStorageProvider();
    }

    @Override
    protected void populate(byte[] data) {
        // Do nothing keep data uninitialized.
    }

    /**
     * Unit tests for {@link NoOpChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class NoOpRollingStorageTests extends ChunkedRollingStorageTests {
        protected ChunkStorage getChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }

    /**
     * Unit tests for {@link NoOpChunkStorage} using {@link ChunkStorageTests}.
     */
    public static class NoOpChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }
}
