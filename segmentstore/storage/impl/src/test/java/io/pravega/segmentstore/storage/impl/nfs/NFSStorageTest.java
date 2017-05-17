/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.nfs;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Unit tests for NFSStorage.
 */
public class NFSStorageTest extends StorageTestBase {
    private File baseDir = null;
    private NFSStorageConfig adapterConfig;

    @Before
    public void setUp() throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.OFF);

        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        this.adapterConfig = NFSStorageConfig
                .builder()
                .with(NFSStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
    }

    @After
    public void tearDown() {
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
    }

    //region Fencing tests

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Test
    @Override
    public void testFencing() throws Exception {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute read-only operations.
            //verifyWriteOperationsFail(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    private void verifyReadOnlyOperationsSucceed(SegmentHandle handle, Storage storage) {
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    private void verifyWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        storage.concat(handle, si.getLength() + data.length, concatHandle.getSegmentName(), TIMEOUT).join();
    }

    private void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength(), concatHandle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    private void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    private void verifyFinalWriteOperationsFail(SegmentHandle handle, Storage storage) {
        AssertExtensions.assertThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    /**
     * Tests the write() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, TIMEOUT).join();

            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createHandle(segmentName + "_1", false, DEFAULT_EPOCH), 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        return new NFSStorage(this.adapterConfig, executorService());
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        FileChannel channel = null;
        if (readOnly) {
            try {
                channel = new RandomAccessFile(Paths.get(this.adapterConfig.getNfsRoot(),
                        segmentName).toString(), "r").getChannel();
            } catch (IOException e) {
            }
            return NFSSegmentHandle.getReadHandle(segmentName, channel);
        } else {
            try {
                channel = new RandomAccessFile(Paths.get(this.adapterConfig.getNfsRoot(),
                        segmentName).toString(), "r").getChannel();
            } catch (IOException e) {
            }
            return NFSSegmentHandle.getWriteHandle(segmentName, channel);
        }
    }


}
