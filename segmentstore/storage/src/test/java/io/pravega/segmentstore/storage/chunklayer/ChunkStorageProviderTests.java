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

import io.pravega.common.io.BoundedInputStream;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Unit tests specifically targeted at test {@link ChunkStorageProvider} implementation.
 */
public abstract class ChunkStorageProviderTests extends ThreadPooledTestSuite {
    Random rnd = new Random();
    ChunkStorageProvider chunkStorage;

    /**
     * Derived classes should return appropriate {@link ChunkStorageProvider}.
     */
    abstract protected ChunkStorageProvider createChunkStorageProvider()  throws Exception;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        chunkStorage = createChunkStorageProvider();
    }

    @Override
    @After
    public void after() throws Exception {
        if (null != chunkStorage) {
            chunkStorage.close();
        }
        super.before();
    }

    /**
     * Populate the data.
     * @param data
     */
    protected void populate(byte[] data) {
        rnd.nextBytes(data);
    }

    /**
     * Test basic chunk lifecycle.
     */
    @Test
    public void testChunkLifeCycle() throws Exception {
        // try reading non existent chunk
        String chunkName = "testchunk";
        testNotExists(chunkName);

        // Perform basic operations.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openRead(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(true, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openWrite(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        testAlreadyExists(chunkName);

        assertEquals(true, chunkStorage.delete(chunkHandle));
        testNotExists(chunkName);
    }

    /**
     * Test basic read and write.
     */
    @Test
    public void testSimpleReadWrite() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int bytesWritten = chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer));
        assertEquals(writeBuffer.length, bytesWritten);

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0);
        assertEquals(writeBuffer.length, bytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        assertEquals(true, chunkStorage.delete(chunkHandle));
    }

    /**
     * Test consecutive reads.
     */
    @Test
    public void testConsecutiveReads() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        int bytesWritten = chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer));
        assertEquals(writeBuffer.length, bytesWritten);

        // Read back in multiple reads.
        byte[] readBuffer = new byte[writeBuffer.length];

        int totalBytesRead = 0;
        for (int i = 1; i <= 5; i++) {
            int remaining = i;
            while (remaining > 0) {
                int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead);
                remaining -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        assertEquals(true, chunkStorage.delete(chunkHandle));
    }

    /**
     * Test consecutive writes.
     */
    @Test
    public void testConsecutiveWrites() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write multiple times.
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        int totalBytesWritten = 0;
        for (int i = 1; i <= 5; i++) {
            int bytesWritten = chunkStorage.write(chunkHandle, totalBytesWritten, i, new ByteArrayInputStream(writeBuffer, totalBytesWritten, i));
            assertEquals(i, bytesWritten);
            totalBytesWritten += i;
        }

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int totalBytesRead = 0;
        int remaining = writeBuffer.length;
        while (remaining > 0) {
            int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead);
            remaining -= bytesRead;
            totalBytesRead += bytesRead;
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        assertEquals(true, chunkStorage.delete(chunkHandle));
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleReadWriteExceptions() throws Exception {
        String chunkName = "testchunk";

        ChunkHandle chunkHandle = chunkStorage.create(chunkName);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        int bytesWritten = chunkStorage.write(chunkHandle, 0, length, new ByteArrayInputStream(writeBuffer));
        assertEquals(length, bytesWritten);

        byte[] readBuffer = new byte[writeBuffer.length];

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, -1, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, -1, 0, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, 0, readBuffer, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, 0, null, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, length + 1, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, length, new byte[length - 1], 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () ->  chunkStorage.read(chunkHandle, 0, length, readBuffer, length),
                ex -> ex instanceof IndexOutOfBoundsException);

        assertEquals(true, chunkStorage.delete(chunkHandle));
    }


    /**
     * Test one simple scenario involving all operations.
     */
    @Test
    public void testSimpleScenario() throws Exception {

        String chunknameA = "A";
        String chunknameB = "B";
        assertFalse(chunkStorage.exists(chunknameA));
        assertFalse(chunkStorage.exists(chunknameB));

        // Create chunks
        chunkStorage.create(chunknameA);
        chunkStorage.create(chunknameB);

        assertTrue(chunkStorage.exists(chunknameA));
        assertTrue(chunkStorage.exists(chunknameB));

        ChunkInfo chunkInfoA = chunkStorage.getInfo(chunknameA);
        assertEquals(chunknameA, chunkInfoA.getName());
        assertEquals(0, chunkInfoA.getLength());

        // Open wirtable handles
        ChunkHandle handleA = chunkStorage.openWrite(chunknameA);
        ChunkHandle handleB = chunkStorage.openWrite(chunknameB);
        assertFalse(handleA.isReadOnly());
        assertFalse(handleB.isReadOnly());

        // Write some data to A
        byte[] bytes = new byte[10];
        int totalBytesWritten = 0;
        for (int i = 1; i < 5; i++) {
            try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bytes), i)) {
                int bytesWritten = chunkStorage.write(handleA, totalBytesWritten, i, bis);
                assertEquals(i, bytesWritten);
            }
            totalBytesWritten += i;
        }

        chunkInfoA = chunkStorage.getInfo(chunknameA);
        assertEquals(totalBytesWritten, chunkInfoA.getLength());

        // Write some data to segment B
        chunkStorage.write(handleB, 0,  bytes.length, new ByteArrayInputStream(bytes));
        totalBytesWritten += bytes.length;
        ChunkInfo chunkInfoB = chunkStorage.getInfo(chunknameB);
        assertEquals(chunknameB, chunkInfoB.getName());
        assertEquals(bytes.length, chunkInfoB.getLength());

        // Read some data
        int totalBytesRead = 0;
        byte[] buffer = new byte[10];
        for (int i = 1; i < 5; i++) {
            totalBytesRead += chunkStorage.read(handleA, totalBytesRead, i, buffer,  totalBytesRead);
        }

        // Concat
        if (chunkStorage.supportsConcat()) {
            chunkInfoA = chunkStorage.getInfo(chunknameA);
            chunkInfoB = chunkStorage.getInfo(chunknameB);

            chunkStorage.concat(handleA, handleB);

            ChunkInfo chunkInfoAAfterConcat = chunkStorage.getInfo(chunknameA);
            assertEquals(chunkInfoA.getLength() + chunkInfoB.getLength(), chunkInfoAAfterConcat.getLength());
        }
        // delete
        chunkStorage.delete(handleA);
        chunkStorage.delete(handleB);
    }

    /**
     * Test default capabilities.
     */
    @Test
    public void testCapabilities() {
        assertEquals(true, chunkStorage.supportsAppend());
        assertEquals(false, chunkStorage.supportsTruncation());
        assertEquals(false, chunkStorage.supportsConcat());
    }

    private void testAlreadyExists(String chunkName) throws InterruptedException, java.util.concurrent.ExecutionException, IOException {
        try {
            ChunkHandle chunkHandle = chunkStorage.create(chunkName);
            Assert.fail("FileAlreadyExistsException was expected");
        } catch (FileAlreadyExistsException e) {
            //Assert.assertTrue(e.getCause().getClass().getName().endsWith("FileAlreadyExistsException"));
            //Assert.assertTrue(e.getCause().getMessage().endsWith(chunkName));
        }
    }

    private void testNotExists(String chunkName) throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        assertEquals(Boolean.FALSE, chunkStorage.exists(chunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(chunkName),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " openRead should throw exception.",
                () -> chunkStorage.openRead(chunkName),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " openWrite should throw exception.",
                () -> chunkStorage.openWrite(chunkName),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(chunkName),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));

        // TODO Fix this
        AssertExtensions.assertThrows(
                " setReadonly should throw exception.",
                () -> chunkStorage.setReadonly(ChunkHandle.writeHandle(chunkName), false),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(ChunkHandle.writeHandle(chunkName), 0, 1, new byte[1], 0),
                ex -> ex instanceof FileNotFoundException && ex.getMessage().contains(chunkName));
    }

}
