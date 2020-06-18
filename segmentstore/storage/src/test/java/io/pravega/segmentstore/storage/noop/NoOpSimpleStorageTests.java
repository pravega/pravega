/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.chunklayer.ChunkManagerRollingTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProviderTests;

import java.io.IOException;

/**
 * Unit tests for {@link NoOpChunkStorageProvider} using {@link SimpleStorageTests}.
 */
public class NoOpSimpleStorageTests extends SimpleStorageTests {
    private static ChunkStorageProvider getChunkStorageProvider() throws IOException {
        return new NoOpChunkStorageProvider();
    }

    protected ChunkStorageProvider getChunkStorage() throws Exception {
        return getChunkStorageProvider();
    }

    @Override
    protected void populate(byte[] data) {
        // Do nothing keep data uninitialized.
    }

    /**
     * Unit tests for {@link NoOpChunkStorageProvider} using {@link ChunkManagerRollingTests}.
     */
    public static class NoOpRollingTests extends ChunkManagerRollingTests {
        protected ChunkStorageProvider getChunkStorage() throws Exception {
            return getChunkStorageProvider();
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }

    /**
     * Unit tests for {@link NoOpChunkStorageProvider} using {@link ChunkStorageProviderTests}.
     */
    public static class NoOpChunkStorageProviderTests extends ChunkStorageProviderTests {
        @Override
        protected ChunkStorageProvider createChunkStorageProvider() throws Exception {
            return getChunkStorageProvider();
        }

        @Override
        protected void populate(byte[] data) {
            // Do nothing keep data uninitialized.
        }
    }
}
