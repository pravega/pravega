/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the WriteOperation class.
 */
public class WriteOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;
    private static final int WRITES_PER_FILE = 10;
    private static final int WRITE_SIZE = 100;

    /**
     * Tests a normal write across many epochs.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testNormalWrite() throws Exception {
        val rnd = new Random(0);
        @Cleanup
        val fs = new MockFileSystem();
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        int offset = 0;
        val writtenData = new ByteArrayOutputStream();
        List<FileDescriptor> files = null;
        for (int fileId = 0; fileId < FILE_COUNT; fileId++) {
            val context = newContext(fileId, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            files = handle.getFiles();
            for (int writeId = 0; writeId < WRITES_PER_FILE; writeId++) {
                byte[] data = new byte[WRITE_SIZE];
                rnd.nextBytes(data);

                // BadOffset write.
                AssertExtensions.assertThrows(
                        "WriteOperation allowed writing at wrong offset.",
                        new WriteOperation(handle, offset + 1, new ByteArrayInputStream(data), data.length, context)::run,
                        ex -> ex instanceof BadOffsetException);

                // Successful write.
                new WriteOperation(handle, offset, new ByteArrayInputStream(data), data.length, context).run();
                writtenData.write(data);
                offset += data.length;

                // Zero-length write (should succeed, but be a no-op.
                new WriteOperation(handle, offset, new ByteArrayInputStream(data), 0, context).run();
            }
        }

        // Check written data via file system reads. ReadOperationTests verifies the same using ReadOperations.
        byte[] expectedData = writtenData.toByteArray();
        int expectedDataOffset = 0;
        for (int i = 0; i < files.size(); i++) {
            FileDescriptor f = files.get(i);
            int len = (int) fs.getFileStatus(f.getPath()).getLen();
            int expectedLen = WRITE_SIZE * WRITES_PER_FILE;
            if (i < files.size() - 1) {
                // This is because OpenWrite combines previous files into one.
                expectedLen *= FILE_COUNT - 1;
            }

            Assert.assertEquals("Unexpected length for file " + f, expectedLen, len);
            @Cleanup
            val inputStream = fs.open(f.getPath(), WRITE_SIZE);
            byte[] fileReadBuffer = new byte[len];
            inputStream.readFully(0, fileReadBuffer);
            AssertExtensions.assertArrayEquals("Unexpected contents for file " + f, expectedData, expectedDataOffset, fileReadBuffer, 0, len);
            expectedDataOffset += len;
        }
    }

    /**
     * Tests the case when the current file (previously empty) has disappeared due to it being fenced out.
     * Expected behavior: StorageNotPrimaryException with no side effects.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testFenceOutMissingFile() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();

        val context2 = newContext(2, fs);
        val handle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();

        AssertExtensions.assertThrows(
                "WriteOperation did not fail when it was fenced out by removing a file.",
                new WriteOperation(handle1, 0, new ByteArrayInputStream(new byte[1]), 1, context1)::run,
                ex -> ex instanceof StorageNotPrimaryException);

        Assert.assertEquals("Unexpected number of files in the filesystem.", 1, fs.getFileCount());
        Assert.assertEquals("Unexpected size of the file in the filesystem.", 0, fs.getFileStatus(handle2.getLastFile().getPath()).getLen());
    }

    /**
     * Tests the case when the current file (non-empty) has been marked as read-only due to it being fenced out.
     * Expected behavior: StorageNotPrimaryException with no side effects.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testFenceOutReadOnlyFile() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        new WriteOperation(handle1, 0, new ByteArrayInputStream(new byte[1]), 1, context1).run();

        val context2 = newContext(2, fs);
        val handle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();

        AssertExtensions.assertThrows(
                "WriteOperation did not fail when it was fenced out by making a file read-only.",
                new WriteOperation(handle1, 1, new ByteArrayInputStream(new byte[1]), 1, context1)::run,
                ex -> ex instanceof StorageNotPrimaryException);

        Assert.assertEquals("Unexpected number of files in the filesystem.", 2, fs.getFileCount());
        Assert.assertEquals("Unexpected size of the first file in the filesystem.", 1, fs.getFileStatus(handle1.getLastFile().getPath()).getLen());
        Assert.assertEquals("Unexpected size of the last file in the filesystem.", 0, fs.getFileStatus(handle2.getLastFile().getPath()).getLen());
    }
}
