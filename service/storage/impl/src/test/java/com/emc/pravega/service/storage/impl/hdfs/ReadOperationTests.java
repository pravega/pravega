/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

    @Test
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
    @Test
    public void testRefreshHandle() throws Exception {
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
}
