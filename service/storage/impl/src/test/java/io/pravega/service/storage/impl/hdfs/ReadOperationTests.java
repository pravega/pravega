/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.util.Random;

import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ReadOperation class.
 */
public class ReadOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;
    private static final int WRITES_PER_FILE = 10;
    private static final int WRITE_SIZE = 100;

    /**
     * Tests a read scenario with no issues or failures.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testNormalRead() throws Exception {
        // Write data.
        val rnd = new Random(0);
        @Cleanup
        val fs = new MockFileSystem();
        val creationContext = newContext(0, fs);
        new CreateOperation(SEGMENT_NAME, creationContext).call();
        int length = 0;
        val writtenData = new ByteArrayOutputStream();
        for (int fileId = 0; fileId < FILE_COUNT; fileId++) {
            val context = newContext(fileId, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            for (int writeId = 0; writeId < WRITES_PER_FILE; writeId++) {
                byte[] data = new byte[WRITE_SIZE];
                rnd.nextBytes(data);
                new WriteOperation(handle, length, new ByteArrayInputStream(data), data.length, context).run();
                writtenData.write(data);
                length += data.length;
            }
        }

        // Check written data via a Read Operation, from every offset from 0 to length/2
        byte[] expectedData = writtenData.toByteArray();
        val readHandle = new OpenReadOperation(SEGMENT_NAME, creationContext).call();
        Assert.assertEquals("Unexpected number of bytes in read handle.", length, readHandle.getLastFile().getLastOffset());
        for (int startOffset = 0; startOffset < length / 2; startOffset++) {
            int readLength = length - 2 * startOffset;
            byte[] actualData = new byte[readLength];
            int readBytes = new ReadOperation(readHandle, startOffset, actualData, 0, actualData.length, creationContext).call();

            Assert.assertEquals("Unexpected number of bytes read with start offset " + startOffset, actualData.length, readBytes);
            AssertExtensions.assertArrayEquals("Unexpected data read back with start offset " + startOffset,
                    expectedData, startOffset, actualData, 0, readLength);
        }
    }

    /**
     * Tests the case when the handle has become stale and needs refreshing (triggered by call to offset+length beyond
     * current known limits).
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testRefreshHandleOffset() throws Exception {
        val rnd = new Random(0);
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val writeHandle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        byte[] data = new byte[WRITE_SIZE];
        int length = 0;
        val writtenData = new ByteArrayOutputStream();

        // Write #1.
        rnd.nextBytes(data);
        new WriteOperation(writeHandle1, length, new ByteArrayInputStream(data), data.length, context1).run();
        writtenData.write(data);
        length += data.length;

        // Open read handle now, with information as of this moment.
        val readHandle = new OpenReadOperation(SEGMENT_NAME, context1).call();

        // Write #2 (after this, the read handle will become stale).
        val context2 = newContext(context1.epoch + 1, fs);
        val writeHandle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();
        rnd.nextBytes(data);
        new WriteOperation(writeHandle2, length, new ByteArrayInputStream(data), data.length, context1).run();
        writtenData.write(data);
        length += data.length;

        // Check written data via a Read Operation, from every offset from 0 to length/2
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[length];
        int readBytes = new ReadOperation(readHandle, 0, actualData, 0, actualData.length, context1).call();

        Assert.assertEquals("Unexpected number of bytes read.", actualData.length, readBytes);
        Assert.assertArrayEquals("Unexpected data read back.", expectedData, actualData);
    }

    /**
     * Tests the case when the handle has become stale due to the segment having been compacted externally. The read operation
     * should refresh the handle and continue working as expected.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testRefreshHandleMissingFile() throws Exception {
        val rnd = new Random(0);
        @Cleanup
        val fs = new MockFileSystem();
        int length = 0;
        val writtenData = new ByteArrayOutputStream();
        for (int i = 0; i < FILE_COUNT; i++) {
            val context = newContext(i, fs);
            val path = context.createEmptyFile(SEGMENT_NAME, length);
            byte[] data = new byte[WRITE_SIZE];
            rnd.nextBytes(data);
            try (val stream = fs.append(path)) {
                stream.write(data);
            }

            writtenData.write(data);
            length += data.length;
        }

        val finalContext = newContext(FILE_COUNT, fs);

        // Get a read handle with all the original files.
        val readHandle = new OpenReadOperation(SEGMENT_NAME, finalContext).call();

        // Verify that the read handle files are read-write (this was done on purpose).
        Assert.assertEquals("Unexpected number of files in the read handle before external compaction.", FILE_COUNT, readHandle.getFiles().size());
        for (FileDescriptor fd : readHandle.getFiles()) {
            Assert.assertFalse("Read Handle contains read-only files.", fd.isReadOnly());
        }

        // Create a write handle, which should compact all files into one (plus an empty one).
        val writeHandle = new OpenWriteOperation(SEGMENT_NAME, finalContext).call();
        Assert.assertEquals("Unexpected number of files in the write handle after external compaction.", 2, writeHandle.getFiles().size());
        Assert.assertTrue("Write Handle first file is not read-only.", writeHandle.getFiles().get(0).isReadOnly());
        Assert.assertFalse("Write Handle last file is read-only.", writeHandle.getLastFile().isReadOnly());

        byte[] actualData = new byte[length];
        new ReadOperation(readHandle, 0, actualData, 0, actualData.length, finalContext).call();
        AssertExtensions.assertListEquals("Unexpected files in read handle after read operation.",
                writeHandle.getFiles(), readHandle.getFiles(), (f1, f2) ->
                        f1.getPath().toString().equals(f2.getPath().toString())
                                && f1.getOffset() == f2.getOffset()
                                && f1.getLength() == f2.getLength()
                                && f1.isReadOnly() == f2.isReadOnly());

        byte[] expectedData = writtenData.toByteArray();
        Assert.assertArrayEquals("Unexpected data read.", expectedData, actualData);

        // Delete the first file, and try again.
        fs.delete(writeHandle.getFiles().get(0).getPath(), true);

        // With a write handle.
        AssertExtensions.assertThrows(
                "ReadOperation did not throw when file was actually missing (write handle).",
                () -> new ReadOperation(writeHandle, 0, actualData, 0, actualData.length, finalContext).call(),
                ex -> ex instanceof FileNotFoundException || ex instanceof SegmentFilesCorruptedException);

        // With a read handle.
        AssertExtensions.assertThrows(
                "ReadOperation did not throw when file was actually missing (read handle).",
                () -> new ReadOperation(readHandle, 0, actualData, 0, actualData.length, finalContext).call(),
                ex -> ex instanceof FileNotFoundException || ex instanceof SegmentFilesCorruptedException);
    }
}
