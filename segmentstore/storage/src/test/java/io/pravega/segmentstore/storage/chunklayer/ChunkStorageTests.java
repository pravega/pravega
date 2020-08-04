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
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Unit tests specifically targeted at test {@link ChunkStorage} implementation.
 */
public abstract class ChunkStorageTests extends ThreadPooledTestSuite {
    Random rnd = new Random();

    @Getter
    ChunkStorage chunkStorage;

    /**
     * Derived classes should return appropriate {@link ChunkStorage}.
     */
    abstract protected ChunkStorage createChunkStorage() throws Exception;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        chunkStorage = createChunkStorage();
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
     *
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

        chunkStorage.delete(chunkHandle);
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
        chunkStorage.delete(chunkHandle);
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
        chunkStorage.delete(chunkHandle);
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
        chunkStorage.delete(chunkHandle);
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

        // Should be able to write 0 bytes.
        chunkStorage.write(chunkHandle, length, 0, new ByteArrayInputStream(new byte[0]));

        byte[] readBuffer = new byte[writeBuffer.length];

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, -1, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, -1, 0, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, readBuffer, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, null, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length + 1, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, new byte[length - 1], 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, readBuffer, length),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 11, 1, readBuffer, 0),
                ex -> ex instanceof IllegalArgumentException);

        // Write exceptions.
        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, -1, new ByteArrayInputStream(writeBuffer)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, -1, 0, new ByteArrayInputStream(writeBuffer)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, new ByteArrayInputStream(writeBuffer)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, length + 1, new ByteArrayInputStream(writeBuffer)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 11, length, new ByteArrayInputStream(writeBuffer)),
                ex -> ex instanceof IllegalArgumentException);

        chunkStorage.delete(chunkHandle);
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

        // Open writable handles
        ChunkHandle handleA = chunkStorage.openWrite(chunknameA);
        ChunkHandle handleB = chunkStorage.openWrite(chunknameB);
        assertFalse(handleA.isReadOnly());
        assertFalse(handleB.isReadOnly());

        // Write some data to A
        byte[] bytes = new byte[10];
        populate(bytes);
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
        chunkStorage.write(handleB, 0, bytes.length, new ByteArrayInputStream(bytes));
        totalBytesWritten += bytes.length;
        ChunkInfo chunkInfoB = chunkStorage.getInfo(chunknameB);
        assertEquals(chunknameB, chunkInfoB.getName());
        assertEquals(bytes.length, chunkInfoB.getLength());

        // Read some data
        int totalBytesRead = 0;
        byte[] buffer = new byte[10];
        for (int i = 1; i < 5; i++) {
            totalBytesRead += chunkStorage.read(handleA, totalBytesRead, i, buffer, totalBytesRead);
        }

        // Concat
        if (chunkStorage.supportsConcat() || chunkStorage.supportsAppend()) {
            chunkInfoA = chunkStorage.getInfo(chunknameA);
            chunkInfoB = chunkStorage.getInfo(chunknameB);
            if (chunkStorage.supportsConcat()) {
                chunkStorage.concat(new ConcatArgument[]{ConcatArgument.fromChunkInfo(chunkInfoA), ConcatArgument.fromChunkInfo(chunkInfoB)});
                ChunkInfo chunkInfoAAfterConcat = chunkStorage.getInfo(chunknameA);
                assertEquals(chunkInfoA.getLength() + chunkInfoB.getLength(), chunkInfoAAfterConcat.getLength());
            }
        }
        // delete
        chunkStorage.delete(handleA);
        try {
            chunkStorage.delete(handleB);
        } catch (ChunkNotFoundException e) {
            // chunkStorage may have natively deleted it as part of concat Eg. HDFS.
        }
    }

    /**
     * Test concat operation for non-existent chunks.
     */
    @Test
    public void testConcatNotExists() throws Exception {
        String existingChunkName = "test";
        ChunkHandle existingChunkHandle = chunkStorage.create(existingChunkName);
        try {
            AssertExtensions.assertThrows(
                    " concat should throw ChunkNotFoundException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                               ConcatArgument.builder().name(existingChunkName).length(0).build(),
                               ConcatArgument.builder().name("NonExistent").length(1).build()
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException);
            AssertExtensions.assertThrows(
                    " concat should throw ChunkNotFoundException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name("NonExistent").length(0).build(),
                                    ConcatArgument.builder().name(existingChunkName).length(0).build(),
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException);
        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            chunkStorage.delete(existingChunkHandle);
        }
    }

    /**
     * Test operations on open handles when underlying chunk is deleted.
     */
    @Test
    public void testDeleteAfterOpen() throws Exception {
        String testChunkName = "test";
        ChunkHandle writeHandle = chunkStorage.create(testChunkName);
        ChunkHandle readHandle = chunkStorage.openRead(testChunkName);
        chunkStorage.delete(writeHandle);
        byte[] bufferRead = new byte[10];
        AssertExtensions.assertThrows(
                " read should throw ChunkNotFoundException.",
                () -> chunkStorage.read(readHandle, 0, 10, bufferRead, 0),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        AssertExtensions.assertThrows(
                " write should throw ChunkNotFoundException.",
                () -> chunkStorage.write(writeHandle, 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        AssertExtensions.assertThrows(
                " truncate should throw ChunkNotFoundException.",
                () -> chunkStorage.truncate(writeHandle, 0),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                " setReadOnly should throw ChunkNotFoundException.",
                () -> chunkStorage.setReadOnly(writeHandle, false),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                " delete should throw ChunkNotFoundException.",
                () -> chunkStorage.delete(writeHandle),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));

    }

    /**
     * Test one simple concat operation.
     */
    @Test
    public void testConcat() throws Exception {
        String tragetChunkName = "Concat_0";
        try {

            ChunkHandle[] chunksToConcat = new ChunkHandle[5];
            ConcatArgument[] chunkInfos = new ConcatArgument[5];
            byte[] bufferWritten = new byte[10];
            populate(bufferWritten);
            int offset = 0;
            for (int i = 0; i < 5; i++) {
                String chunkname = "Concat_" + i;
                chunksToConcat[i] = chunkStorage.create(chunkname);
                chunkInfos[i] = new ConcatArgument(i, chunkname);
                // Write some data to chunk
                if (i > 0) {
                    try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bufferWritten, offset, i), i)) {
                        int bytesWritten = chunkStorage.write(chunksToConcat[i], 0, i, bis);
                        assertEquals(i, bytesWritten);
                    }
                    ChunkInfo chunkInfo = chunkStorage.getInfo(chunkname);
                    assertEquals(i, chunkInfo.getLength());
                    offset += i;
                }
            }

            chunkStorage.concat(chunkInfos);

            ChunkInfo chunkInfoAfterConcat = chunkStorage.getInfo(tragetChunkName);
            assertEquals(10, chunkInfoAfterConcat.getLength());
            // Read some data
            byte[] bufferRead = new byte[10];

            int bytesRead = 0;
            while (bytesRead < 10) {
                bytesRead += chunkStorage.read(ChunkHandle.readHandle(tragetChunkName), bytesRead, bufferRead.length, bufferRead, bytesRead);
            }
            assertArrayEquals(bufferWritten, bufferRead);

        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            for (int i = 0; i < 5; i++) {
                String chunkname = "Concat_" + i;
                if (chunkStorage.exists(chunkname)) {
                    chunkStorage.delete(ChunkHandle.writeHandle(chunkname));
                }
            }
        }
    }

    /**
     * Test readonly.
     */
    @Test
    public void testReadonly() throws Exception {
        String chunkName = "chunk";
        // Create chunks
        chunkStorage.create(chunkName);
        assertTrue(chunkStorage.exists(chunkName));

        // Open writable handle
        ChunkHandle hWrite = chunkStorage.openWrite(chunkName);
        assertFalse(hWrite.isReadOnly());

        // Write some data
        int bytesWritten = chunkStorage.write(hWrite, 0, 1, new ByteArrayInputStream(new byte[1]));
        assertEquals(1, bytesWritten);

        AssertExtensions.assertThrows(
                " write should throw IllegalArgumentException.",
                () -> chunkStorage.write(ChunkHandle.readHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " delete should throw IllegalArgumentException.",
                () -> chunkStorage.delete(ChunkHandle.readHandle(chunkName)),
                ex -> ex instanceof IllegalArgumentException);
        // Make readonly and open.
        chunkStorage.setReadOnly(hWrite, true);
        chunkStorage.openWrite(chunkName);

        // Make writable and open again.
        chunkStorage.setReadOnly(hWrite, false);
        ChunkHandle hWrite2 = chunkStorage.openWrite(chunkName);
        assertFalse(hWrite2.isReadOnly());

        bytesWritten = chunkStorage.write(hWrite, 1, 1, new ByteArrayInputStream(new byte[1]));
        assertEquals(1, bytesWritten);
        chunkStorage.delete(hWrite);
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncate() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            chunkStorage.create(chunkName);
            assertTrue(chunkStorage.exists(chunkName));

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName);
            assertFalse(hWrite.isReadOnly());

            // Write some data
            int bytesWritten = chunkStorage.write(hWrite, 0, 5, new ByteArrayInputStream(new byte[5]));
            assertEquals(5, bytesWritten);

            chunkStorage.truncate(hWrite, 4);
            val info = chunkStorage.getInfo(chunkName);
            Assert.assertEquals(4, info.getLength());

            // append at end.
            bytesWritten = chunkStorage.write(hWrite, 4, 1, new ByteArrayInputStream(new byte[1]));
            assertEquals(1, bytesWritten);

            val info2 = chunkStorage.getInfo(chunkName);
            Assert.assertEquals(5, info2.getLength());

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), 0),
                    ex -> ex instanceof IllegalArgumentException);

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), -1),
                    ex -> ex instanceof IllegalArgumentException);
        } catch (UnsupportedOperationException e) {
            assertFalse(chunkStorage.supportsTruncation());
        }
    }

    /**
     * Test default capabilities.
     */
    @Test
    public void testCapabilities() {
        assertEquals(true, chunkStorage.supportsAppend());
        assertEquals(true, chunkStorage.supportsTruncation());
        assertEquals(false, chunkStorage.supportsConcat());
    }

    private void testAlreadyExists(String chunkName) throws Exception {
        try {
            ChunkHandle chunkHandle = chunkStorage.create(chunkName);
            Assert.fail("ChunkAlreadyExistsException was expected");
        } catch (ChunkAlreadyExistsException e) {
            //Assert.assertTrue(e.getCause().getClass().getName().endsWith("FileAlreadyExistsException"));
            //Assert.assertTrue(e.getCause().getMessage().endsWith(chunkName));
        }
    }

    private void testNotExists(String chunkName) throws Exception {
        assertFalse(chunkStorage.exists(chunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " openRead should throw exception.",
                () -> chunkStorage.openRead(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " openWrite should throw exception.",
                () -> chunkStorage.openWrite(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.writeHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " setReadOnly should throw exception.",
                () -> chunkStorage.setReadOnly(ChunkHandle.writeHandle(chunkName), false),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(ChunkHandle.writeHandle(chunkName), 0, 1, new byte[1], 0),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertThrows(
                " delete should throw exception.",
                () -> chunkStorage.delete(ChunkHandle.writeHandle(chunkName)),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));
    }

    @Test
    public void testInvalidName() {
        String emptyChunkName = "";

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openRead should throw exception.",
                () -> chunkStorage.openRead(emptyChunkName),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openWrite should throw exception.",
                () -> chunkStorage.openWrite(emptyChunkName),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " setReadOnly should throw exception.",
                () -> chunkStorage.setReadOnly(ChunkHandle.writeHandle(emptyChunkName), false),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new byte[1], 0),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " delete should throw exception.",
                () -> chunkStorage.delete(ChunkHandle.writeHandle(emptyChunkName)),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException.",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                                ConcatArgument.builder().name("NonExistent").length(1).build()
                        }
                ),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException.",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name("test").length(0).build(),
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                        }
                ),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));
    }

}
