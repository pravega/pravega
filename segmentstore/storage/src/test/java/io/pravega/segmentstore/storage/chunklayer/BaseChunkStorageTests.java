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
import io.pravega.segmentstore.storage.noop.NoOpChunkStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Unit tests specifically targeted at test {@link ChunkStorage} implementation.
 */
public class BaseChunkStorageTests extends ThreadPooledTestSuite {
    @Getter
    ChunkStorage chunkStorage;

    /**
     * Derived classes should return appropriate {@link ChunkStorage}.
     */
    protected ChunkStorage createChunkStorage() throws Exception {
        return new NoOpChunkStorage(executorService());
    }

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
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleReadExceptions() throws Exception {
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        val chunkHandle = chunkStorage.createWithContent(chunkName, 10, new ByteArrayInputStream(writeBuffer)).get();
        int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
        assertEquals(length, bytesWritten);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

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
    }

    /**
     * Test simple reads and writes for exceptions.
     */
    @Test
    public void testSimpleWriteExceptions() throws Exception {
        String chunkName = "testchunk";

        byte[] writeBuffer = new byte[10];
        populate(writeBuffer);
        int length = writeBuffer.length;
        val chunkHandle = chunkStorage.createWithContent(chunkName, 10, new ByteArrayInputStream(writeBuffer)).get();
        int bytesWritten = Math.toIntExact(chunkStorage.getInfo(chunkName).get().getLength());
        assertEquals(length, bytesWritten);
        assertEquals(chunkName, chunkHandle.getChunkName());
        assertEquals(false, chunkHandle.isReadOnly());

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

        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(null, 0, 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " write should throw exception.",
                () -> chunkStorage.write(ChunkHandle.readHandle("test"), 0, 1, new ByteArrayInputStream(writeBuffer)).get(),
                ex -> ex instanceof IllegalArgumentException);

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

        // Open writable handles
        ChunkHandle handleA;
        ChunkInfo chunkInfoA;
        byte[] bytes = new byte[10];
        populate(bytes);
        int totalBytesWritten = 0;

        // Write some data to A
        if (chunkStorage.supportsAppend()) {
            // Create chunks
            chunkStorage.create(chunknameA).get();
            assertTrue(chunkStorage.exists(chunknameA).get());

            chunkInfoA = chunkStorage.getInfo(chunknameA).get();
            assertEquals(chunknameA, chunkInfoA.getName());
            assertEquals(0, chunkInfoA.getLength());

            handleA = chunkStorage.openWrite(chunknameA).get();
            assertFalse(handleA.isReadOnly());
            for (int i = 1; i < 5; i++) {
                try (BoundedInputStream bis = new BoundedInputStream(new ByteArrayInputStream(bytes), i)) {
                    int bytesWritten = chunkStorage.write(handleA, totalBytesWritten, i, bis).get();
                    assertEquals(i, bytesWritten);
                }
                totalBytesWritten += i;
            }
        } else {
            handleA = chunkStorage.createWithContent(chunknameA, bytes.length, new ByteArrayInputStream(bytes)).get();
            totalBytesWritten = 10;
        }

        chunkInfoA = chunkStorage.getInfo(chunknameA).get();
        assertEquals(totalBytesWritten, chunkInfoA.getLength());

        // Write some data to segment B
        byte[] bytes2 = new byte[5];
        populate(bytes2);
        ChunkHandle handleB = chunkStorage.createWithContent(chunknameB, bytes2.length, new ByteArrayInputStream(bytes2)).get();
        totalBytesWritten += bytes.length;
        ChunkInfo chunkInfoB = chunkStorage.getInfo(chunknameB).get();
        assertEquals(chunknameB, chunkInfoB.getName());
        assertEquals(bytes2.length, chunkInfoB.getLength());
        assertFalse(handleA.isReadOnly());
        assertFalse(handleB.isReadOnly());
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
     * Test concat operation for non-existent chunks.
     */
    @Test
    public void testConcatException() throws Exception {
        String existingChunkName1 = "test1";
        String existingChunkName2 = "test2";
        ChunkHandle existingChunkHandle1 = chunkStorage.createWithContent(existingChunkName1, 1, new ByteArrayInputStream(new byte[1])).get();
        ChunkHandle existingChunkHandle2 = chunkStorage.createWithContent(existingChunkName2, 1, new ByteArrayInputStream(new byte[1])).get();
        try {
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(0).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(-1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(0).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(-1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    null,
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    null,
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    " concat should throw IllegalArgumentException.",
                    () -> chunkStorage.concat(
                            new ConcatArgument[]{
                                    ConcatArgument.builder().name(existingChunkName1).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(1).build(),
                                    ConcatArgument.builder().name(existingChunkName2).length(1).build(),
                            }
                    ).get(),
                    ex -> ex instanceof IllegalArgumentException);
        } catch (UnsupportedOperationException e) {
            // The storage provider may not have native concat.
        } finally {
            chunkStorage.delete(existingChunkHandle1).join();
            chunkStorage.delete(existingChunkHandle2).join();
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
                        || ex instanceof UnsupportedOperationException);
        AssertExtensions.assertFutureThrows(
                " delete should throw ChunkNotFoundException.",
                chunkStorage.delete(writeHandle),
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(testChunkName));

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
        // Make readonly and open.
        chunkStorage.setReadOnly(hWrite, true).join();
        chunkStorage.openWrite(chunkName);

        // Make writable and open again.
        chunkStorage.setReadOnly(hWrite, false).join();
        ChunkHandle hWrite2 = chunkStorage.openWrite(chunkName).get();
        assertFalse(hWrite2.isReadOnly());

        bytesWritten = chunkStorage.write(hWrite2, 1, 1, new ByteArrayInputStream(new byte[1])).get();
        assertEquals(1, bytesWritten);
        chunkStorage.delete(hWrite).join();
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
        } catch (ExecutionException e) {
            assertTrue( e.getCause() instanceof UnsupportedOperationException);
            assertFalse(chunkStorage.supportsTruncation());
        }
    }

    /**
     * Test truncate.
     */
    @Test
    public void testTruncateExceptions() throws Exception {
        try {
            String chunkName = "chunk";
            // Create chunks
            chunkStorage.create(chunkName).get();
            assertTrue(chunkStorage.exists(chunkName).get());

            // Open writable handle
            ChunkHandle hWrite = chunkStorage.openWrite(chunkName).get();
            assertFalse(hWrite.isReadOnly());

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.readHandle(chunkName), 0).get(),
                    ex -> ex instanceof IllegalArgumentException);

            AssertExtensions.assertThrows(
                    " truncate should throw IllegalArgumentException.",
                    () -> chunkStorage.truncate(ChunkHandle.writeHandle(chunkName), -1).get(),
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
                ex -> ex instanceof ChunkNotFoundException && ex.getMessage().contains(chunkName));

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

    @Test
    public void testNullHandle() {
        AssertExtensions.assertThrows(
                " getInfo should throw IllegalArgumentException.",
                () -> chunkStorage.getInfo(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " openRead should throw IllegalArgumentException.",
                () -> chunkStorage.openRead(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " openWrite should throw IllegalArgumentException.",
                () -> chunkStorage.openWrite(null).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " write should throw IllegalArgumentException.",
                () -> chunkStorage.write(null, 0, 1, new ByteArrayInputStream(new byte[1])).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " setReadOnly should throw IllegalArgumentException.",
                () -> chunkStorage.setReadOnly(null, false).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " read should throw IllegalArgumentException.",
                () -> chunkStorage.read(null, 0, 1, new byte[1], 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " delete should throw IllegalArgumentException.",
                () -> chunkStorage.delete(null).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " truncate should throw IllegalArgumentException.",
                () -> chunkStorage.truncate(null, 0).get(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                " concat should throw IllegalArgumentException.",
                () -> chunkStorage.concat(null).get(),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                " concat should throw IllegalArgumentException.",
                () -> chunkStorage.concat(new ConcatArgument[]{null}).get(),
                ex -> ex instanceof IllegalArgumentException);

    }
}
