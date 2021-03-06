/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link InMemoryChunkStorage} using {@link SimpleStorageTests}.
 */
public class NoAppendSimpleStorageTests extends SimpleStorageTests {

    protected static InMemoryChunkStorage getNoAppendInMemoryChunkStorage(Executor executor) {
        val ret = new InMemoryChunkStorage(executor);
        ret.setShouldSupportAppend(false);
        return ret;
    }

    protected ChunkStorage getChunkStorage() {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Unit tests for {@link InMemoryChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class NoAppendSimpleStorageRollingStorageTests extends ChunkedRollingStorageTests {
        protected ChunkStorage getChunkStorage() {
            return getNoAppendInMemoryChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link InMemoryChunkStorage} using {@link ChunkStorageTests}.
     */
    public static class NoAppendChunkStorageTests extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return getNoAppendInMemoryChunkStorage(executorService());
        }

        /**
         * Test default capabilities.
         */
        @Test
        public void testCapabilities() {
            assertEquals(false, chunkStorage.supportsAppend());
            assertEquals(true, chunkStorage.supportsTruncation());
            assertEquals(false, chunkStorage.supportsConcat());
        }

        /**
         * Test simple reads and writes for exceptions.
         */
        @Test
        public void testSimpleWriteExceptions() throws Exception {
        }
    }
}

