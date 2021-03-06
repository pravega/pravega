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
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Unit tests specifically targeted at test {@link ChunkStorage} implementation.
 */
public class ChunkStorageTests extends BaseChunkStorageTests {
    Random rnd = new Random();

    /**
     * Derived classes should return appropriate {@link ChunkStorage}.
     */
    @Override
    protected ChunkStorage createChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Populate the data.
     *
     * @param data
     */
    @Override
    protected void populate(byte[] data) {
        rnd.nextBytes(data);
    }

    /**
     * Test readonly.
     */
    @Test
    @Override
    public void testReadonly() throws Exception {
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncate() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            val h = chunkStorage.createWithContent(chunkName, 9, new ByteArrayInputStream(new byte[9])).get();
            int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
            assertEquals(9, bytesWritten);
            assertTrue(chunkStorage.exists(chunkName).get());

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());

            // Write some data
            chunkStorage.truncate(hWrite, 4).get();
            val info = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(4, info.getLength());
            // truncate at end.
            chunkStorage.truncate(hWrite, 1).get();
            val info2 = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(1, info2.getLength());
        } catch (ExecutionException e) {
            assertTrue( e.getCause() instanceof UnsupportedOperationException);
            assertFalse(chunkStorage.supportsTruncation());
        }
    }
}
