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

import io.pravega.common.Exceptions;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

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
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openRead(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(true, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openWrite(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        testAlreadyExists(chunkName);

        chunkStorage.delete(chunkHandle).join();
        testNotExists(chunkName);
    }

    @Test
    public void testChunkLifeCycleCreateWithContent() throws Exception {
        // try reading non existent chunk
        String chunkName = "testchunk";
        testNotExists(chunkName);

        // Perform basic operations.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openRead(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(true, chunkHandle.isReadOnly());

        chunkHandle = chunkStorage.openWrite(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        testAlreadyExists(chunkName);

        chunkStorage.delete(chunkHandle).join();
        testNotExists(chunkName);
    }

    /**
     * Test basic read and write.
     */
    @Test
    public void testSimpleReadWrite() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int bytesWritten = chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(writeBuffer.length, bytesWritten);

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0).get();
        assertEquals(writeBuffer.length, bytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    @Test
    public void testSimpleReadWriteCreateWithContent() throws Exception {
        String chunkName = "testchunk";

        // Create.
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int bytesRead = chunkStorage.read(chunkHandle, 0, writeBuffer.length, readBuffer, 0).get();
        assertEquals(writeBuffer.length, bytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test consecutive reads.
     */
    @Test
    public void testConsecutiveReads() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        int bytesWritten = chunkStorage.write(chunkHandle, 0, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(writeBuffer.length, bytesWritten);

        // Read back in multiple reads.
        byte[] readBuffer = new byte[writeBuffer.length];

        int totalBytesRead = 0;
        for (int i = 1; i <= 5; i++) {
            int remaining = i;
            while (remaining > 0) {
                int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
                remaining -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test consecutive reads.
     */
    @Test
    public void testConsecutiveReadsCreateWithContent() throws Exception {
        String chunkName = "testchunk";

        // Create.
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        // Create.
        ChunkHandle chunkHandle = chunkStorage.createWithContent(chunkName, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Read back in multiple reads.
        byte[] readBuffer = new byte[writeBuffer.length];

        int totalBytesRead = 0;
        for (int i = 1; i <= 5; i++) {
            int remaining = i;
            while (remaining > 0) {
                int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
                remaining -= bytesRead;
                totalBytesRead += bytesRead;
            }
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }


    /**
     * Test consecutive writes.
     */
    @Test
    public void testConsecutiveWrites() throws Exception {
        String chunkName = "testchunk";

        // Create.
        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

        // Write multiple times.
        byte[] writeBuffer = new byte[15];
        populate(writeBuffer);
        int totalBytesWritten = 0;
        for (int i = 1; i <= 5; i++) {
            int bytesWritten = chunkStorage.write(chunkHandle, totalBytesWritten, i, new ByteArrayInputStream(writeBuffer, totalBytesWritten, i)).get();
            assertEquals(i, bytesWritten);
            totalBytesWritten += i;
        }

        // Read back.
        byte[] readBuffer = new byte[writeBuffer.length];
        int totalBytesRead = 0;
        int remaining = writeBuffer.length;
        while (remaining > 0) {
            int bytesRead = chunkStorage.read(chunkHandle, totalBytesRead, remaining, readBuffer, totalBytesRead).get();
            remaining -= bytesRead;
            totalBytesRead += bytesRead;
        }
        assertEquals(writeBuffer.length, totalBytesRead);
        assertArrayEquals(writeBuffer, readBuffer);

        // Delete.
        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleReadWriteExceptions() throws Exception {
        String chunkName = "testchunk";

        ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());
        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        int bytesWritten = chunkStorage.write(chunkHandle, 0, length, new ByteArrayInputStream(writeBuffer)).get();
        assertEquals(length, bytesWritten);

        // Should be able to write 0 bytes.
        chunkStorage.write(chunkHandle, length, 0, new ByteArrayInputStream(new byte[0]));

        byte[] readBuffer = new byte[writeBuffer.length];

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, -1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, -1, 0, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, readBuffer, -1).get(),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, 0, null, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length + 1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, new byte[length - 1], 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 0, length, readBuffer, length).get(),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(chunkHandle, 11, 1, readBuffer, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        // Write exceptions.
        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, -1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, -1, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, 0, null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 0, length + 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(chunkHandle, 11, length, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof InvalidOffsetException);

        chunkStorage.delete(chunkHandle).join();
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testCreateWithContentExceptions() throws Exception {
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        // Create exceptions.
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(null, writeBuffer.length, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, -1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, 0, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " createWithContent should throw exception.",
                () -> chunkStorage.createWithContent(chunkName, 1, null).get(),
                ex -> ex instanceof IllegalArgumentException);

    }

    /**
     * Test one simple scenario involving all operations.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        String chunknameA = "A";
        String chunknameB = "B";
        assertFalse(chunkStorage.exists(chunknameA).get());
        assertFalse(chunkStorage.exists(chunknameB).get());

        // Create chunks
        chunkStorage.create(chunknameA).get();
        chunkStorage.create(chunknameB).get();

        assertTrue(chunkStorage.exists(chunknameA).get());
        assertTrue(chunkStorage.exists(chunknameB).get());

        ChunkInfo chunkInfoA = chunkStorage.getInfo(chunknameA).get();
        assertEquals(chunknameA, chunkInfoA.getName());
        assertEquals(0, chunkInfoA.getLength());

        // Open writable handles
        ChunkHandle handleA = chunkStorage.openWrite(chunknameA).get();
        ChunkHandle handleB = chunkStorage.openWrite(chunknameB).get();
        assertFalse(handleA.isReadOnly());
        assertFalse(handleB.isReadOnly());

        // Write some data to A
        byte[] bytes = new byte[10];
        populate(bytes);
        int totalBytesWritten = 0;
        for (int i = 1; i < 5; i++) {
            try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bytes), i)) {
                int bytesWritten = chunkStorage.write(handleA, totalBytesWritten, i, bis).get();
                assertEquals(i, bytesWritten);
            }
            totalBytesWritten += i;
        }

        chunkInfoA = chunkStorage.getInfo(chunknameA).get();
        assertEquals(totalBytesWritten, chunkInfoA.getLength());

        // Write some data to segment B
        chunkStorage.write(handleB, 0, bytes.length, new ByteArrayInputStream(bytes)).get();
        totalBytesWritten += bytes.length;
        ChunkInfo chunkInfoB = chunkStorage.getInfo(chunknameB).get();
        assertEquals(chunknameB, chunkInfoB.getName());
        assertEquals(bytes.length, chunkInfoB.getLength());

        // Read some data
        int totalBytesRead = 0;
        byte[] buffer = new byte[10];
        for (int i = 1; i < 5; i++) {
            totalBytesRead += chunkStorage.read(handleA, totalBytesRead, i, buffer, totalBytesRead).get();
        }

        // Concat
        if (chunkStorage.supportsConcat() || chunkStorage.supportsAppend()) {
            chunkInfoA = chunkStorage.getInfo(chunknameA).get();
            chunkInfoB = chunkStorage.getInfo(chunknameB).get();
            if (chunkStorage.supportsConcat()) {
                chunkStorage.concat(new ConcatArgument[]{ConcatArgument.fromChunkInfo(chunkInfoA), ConcatArgument.fromChunkInfo(chunkInfoB)}).join();
                ChunkInfo chunkInfoAAfterConcat = chunkStorage.getInfo(chunknameA).get();
                assertEquals(chunkInfoA.getLength() + chunkInfoB.getLength(), chunkInfoAAfterConcat.getLength());
            }
        }
        // delete
        chunkStorage.delete(handleA).join();
        try {
            chunkStorage.delete(handleB).join();
        } catch (CompletionException e) {
            // chunkStorage may have natively deleted it as part of concat Eg. HDFS.
        }
    }

    /**
     * Test concat operation for non-existent chunks.
     */
    @Test
    public void testConcatNotExists() throws Exception {
        String existingChunkName = "test";
        ChunkHandle existingChunkHandle = chunkStorage.create(existingChunkName).get();
        try {
            AssertExtensions.assertFutureThrows(
                    " concat should throw ChunkNotFoundException.",
                    chunkStorage.concat(
                            new ConcatArgument[]{
                               ConcatArgument.builder().name(existingChunkName).length(0).build(),
                               ConcatArgument.builder().name("NonExistent").length(1).build()
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException);
            AssertExtensions.assertFutureThrows(
                    " concat should throw ChunkNotFoundException.",
                    chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name("NonExistent").length(0).build(),
                                    ConcatArgument.builder().name(existingChunkName).length(0).build(),
                            }
                    ),
                    ex -> ex instanceof ChunkNotFoundException);
        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            chunkStorage.delete(existingChunkHandle).join();
        }
    }

    /**
     * Test operations on open handles when underlying chunk is deleted.
     */
    @Test
    public void testDeleteAfterOpen() throws Exception {
        String testChunkName = "test";
        ChunkHandle writeHandle = chunkStorage.create(testChunkName).get();
        ChunkHandle readHandle = chunkStorage.openRead(testChunkName).get();
        chunkStorage.delete(writeHandle).join();
        byte[] bufferRead = new byte[10];
        AssertExtensions.assertFutureThrows(
                " read should throw ChunkNotFoundException.",
                chunkStorage.read(readHandle, 0, 10, bufferRead, 0),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        AssertExtensions.assertFutureThrows(
                " write should throw ChunkNotFoundException.",
                chunkStorage.write(writeHandle, 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));
        AssertExtensions.assertFutureThrows(
                " truncate should throw ChunkNotFoundException.",
                chunkStorage.truncate(writeHandle, 0),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertFutureThrows(
                " setReadOnly should throw ChunkNotFoundException.",
                chunkStorage.setReadOnly(writeHandle, false),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName))
                        || ex.getCause() instanceof UnsupportedOperationException);
        AssertExtensions.assertFutureThrows(
                " delete should throw ChunkNotFoundException.",
                chunkStorage.delete(writeHandle),
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
                chunksToConcat[i] = chunkStorage.create(chunkname).get();
                chunkInfos[i] = new ConcatArgument(i, chunkname);
                // Write some data to chunk
                if (i > 0) {
                    try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bufferWritten, offset, i), i)) {
                        int bytesWritten = chunkStorage.write(chunksToConcat[i], 0, i, bis).get();
                        assertEquals(i, bytesWritten);
                    }
                    ChunkInfo chunkInfo = chunkStorage.getInfo(chunkname).get();
                    assertEquals(i, chunkInfo.getLength());
                    offset += i;
                }
            }

            chunkStorage.concat(chunkInfos).get();

            ChunkInfo chunkInfoAfterConcat = chunkStorage.getInfo(tragetChunkName).get();
            assertEquals(10, chunkInfoAfterConcat.getLength());
            // Read some data
            byte[] bufferRead = new byte[10];

            int bytesRead = 0;
            while (bytesRead < 10) {
                bytesRead += chunkStorage.read(ChunkHandle.readHandle(tragetChunkName), bytesRead, bufferRead.length, bufferRead, bytesRead).get();
            }
            assertArrayEquals(bufferWritten, bufferRead);

        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            for (int i = 0; i < 5; i++) {
                String chunkname = "Concat_" + i;
                if (chunkStorage.exists(chunkname).get()) {
                    chunkStorage.delete(ChunkHandle.writeHandle(chunkname)).join();
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
        chunkStorage.create(chunkName).get();
        assertTrue(chunkStorage.exists(chunkName).get());

        // Open writable handle
        ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
        assertFalse(hWrite.isReadOnly());

        // Write some data
        int bytesWritten = chunkStorage.write(hWrite, 0, 1, new ByteArrayInputStream(new byte[1])).get();
        assertEquals(1, bytesWritten);

        AssertExtensions.assertThrows(
                " write should throw IllegalArgumentException.",
                () -> chunkStorage.write(ChunkHandle.readHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " delete should throw IllegalArgumentException.",
                () -> chunkStorage.delete(ChunkHandle.readHandle(chunkName)).get(),
                ex -> ex instanceof IllegalArgumentException);
        try {
            // Make readonly and open.
            chunkStorage.setReadOnly(hWrite, true).join();
            chunkStorage.openWrite(chunkName);

            // Make writable and open again.
            chunkStorage.setReadOnly(hWrite, false).join();
            ChunkHandle hWrite2 = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite2.isReadOnly());

            bytesWritten = chunkStorage.write(hWrite, 1, 1, new ByteArrayInputStream(new byte[1])).get();
            assertEquals(1, bytesWritten);
            chunkStorage.delete(hWrite).join();
        } catch (Exception e) {
            val ex = Exceptions.unwrap(e);
            assertTrue(ex.getCause() instanceof UnsupportedOperationException);
        }
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncate() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            chunkStorage.create(chunkName).get();
            assertTrue(chunkStorage.exists(chunkName).get());

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());

            // Write some data
            int bytesWritten = chunkStorage.write(hWrite, 0, 5, new ByteArrayInputStream(new byte[5])).get();
            assertEquals(5, bytesWritten);

            chunkStorage.truncate(hWrite, 4).get();
            val info = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(4, info.getLength());

            // append at end.
            bytesWritten = chunkStorage.write(hWrite, 4, 1, new ByteArrayInputStream(new byte[1])).get();
            assertEquals(1, bytesWritten);

            val info2 = chunkStorage.getInfo(chunkName).get();
            Assert.assertEquals(5, info2.getLength());

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), 0).get(),
                    ex -> ex instanceof IllegalArgumentException);

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), -1).get(),
                    ex -> ex instanceof IllegalArgumentException);
        } catch (ExecutionException e) {
            assertTrue( e.getCause() instanceof UnsupportedOperationException);
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
            ChunkHandle chunkHandle = chunkStorage.create(chunkName).get();
            Assert.fail("ChunkAlreadyExistsException was expected");
        } catch (ExecutionException e) {
            //Assert.assertTrue(e.getCause().getClass().getName().endsWith("FileAlreadyExistsException"));
            //Assert.assertTrue(e.getCause().getMessage().endsWith(chunkName));
        }
    }

    private void testNotExists(String chunkName) throws Exception {
        assertFalse(chunkStorage.exists(chunkName).get());

        AssertExtensions.assertFutureThrows(
                " getInfo should throw exception.",
                chunkStorage.getInfo(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " openRead should throw exception.",
                chunkStorage.openRead(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " openWrite should throw exception.",
                chunkStorage.openWrite(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " getInfo should throw exception.",
                chunkStorage.getInfo(chunkName),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " write should throw exception.",
                chunkStorage.write(ChunkHandle.writeHandle(chunkName), 0, 1, new ByteArrayInputStream(new byte[1])),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " setReadOnly should throw exception.",
                chunkStorage.setReadOnly(ChunkHandle.writeHandle(chunkName), false),
                ex -> (ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName))
                        || ex.getCause() instanceof UnsupportedOperationException);

        AssertExtensions.assertFutureThrows(
                " read should throw exception.",
                chunkStorage.read(ChunkHandle.writeHandle(chunkName), 0, 1, new byte[1], 0),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

        AssertExtensions.assertFutureThrows(
                " delete should throw exception.",
                chunkStorage.delete(ChunkHandle.writeHandle(chunkName)),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));
    }

    @Test
    public void testInvalidName() {
        String emptyChunkName = "";

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openRead should throw exception.",
                () -> chunkStorage.openRead(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " openWrite should throw exception.",
                () -> chunkStorage.openWrite(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " getInfo should throw exception.",
                () -> chunkStorage.getInfo(emptyChunkName).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new ByteArrayInputStream(new byte[1])).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " setReadOnly should throw exception.",
                () -> chunkStorage.setReadOnly(ChunkHandle.writeHandle(emptyChunkName), false).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " read should throw exception.",
                () -> chunkStorage.read(ChunkHandle.writeHandle(emptyChunkName), 0, 1, new byte[1], 0).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " delete should throw exception.",
                () -> chunkStorage.delete(ChunkHandle.writeHandle(emptyChunkName)).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException.",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                                ConcatArgument.builder().name("NonExistent").length(1).build()
                        }
                ).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));

        AssertExtensions.assertThrows(
                " concat should throw ChunkNotFoundException.",
                () -> chunkStorage.concat(
                        new ConcatArgument[]{
                                ConcatArgument.builder().name("test").length(0).build(),
                                ConcatArgument.builder().name(emptyChunkName).length(0).build(),
                        }
                ).get(),
                ex -> ex instanceof IllegalArgumentException && ex.getMessage().contains(emptyChunkName));
    }

}
