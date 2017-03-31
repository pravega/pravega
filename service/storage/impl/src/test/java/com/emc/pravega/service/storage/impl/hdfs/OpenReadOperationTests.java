/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the OpenReadOperation class.
 */
public class OpenReadOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;

    /**
     * Tests the OpenReadOperation.
     */
    @Test
    public void testOpenRead() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        long expectedLength = 0;
        val fileList = new ArrayList<String>();
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        for (int i = 0; i < FILE_COUNT; i++) {
            val context = newContext(i, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            fileList.add(handle.getLastFile().getPath());
            byte[] data = new byte[i + 1];
            new WriteOperation(handle, expectedLength, new ByteArrayInputStream(data), data.length, context).run();
            expectedLength += data.length;
        }

        val openContext = newContext(FILE_COUNT, fs);
        HDFSSegmentHandle handle = new OpenReadOperation(SEGMENT_NAME, openContext).call();
        Assert.assertTrue("Unexpected value for isReadOnly", handle.isReadOnly());
        Assert.assertEquals("Unexpected value for getSegmentName", SEGMENT_NAME, handle.getSegmentName());
        Assert.assertEquals("Unexpected count of files.", fileList.size(), handle.getFiles().size());
        expectedLength = 0;
        for (int i = 0; i < fileList.size(); i++) {
            val file = handle.getFiles().get(i);
            Assert.assertEquals("Unexpected value for FileDescriptor.getPath for index " + i, fileList.get(i), file.getPath());
            Assert.assertEquals("Unexpected value for FileDescriptor.getOffset for index " + i, expectedLength, file.getOffset());
            Assert.assertEquals("Unexpected value for FileDescriptor.getLength for index " + i, i + 1, file.getLength());
            Assert.assertEquals("Unexpected value for FileDescriptor.getEpoch for index " + i, i, file.getEpoch());
            Assert.assertEquals("Unexpected value for FileDescriptor.isReadOnly for index " + i, i < fileList.size() - 1, file.isReadOnly());

            expectedLength += i + 1;
        }

        // Delete first file.
        fs.delete(new Path(fileList.get(0)), true);
        AssertExtensions.assertThrows(
                "GetInfo succeeded on corrupted segment.",
                new OpenReadOperation(SEGMENT_NAME, openContext)::call,
                ex -> ex instanceof SegmentFilesCorruptedException);

        // Inexistent segment.
        fs.clear();
        AssertExtensions.assertThrows(
                "GetInfo succeeded on missing segment.",
                new OpenReadOperation(SEGMENT_NAME, openContext)::call,
                ex -> ex instanceof FileNotFoundException);
    }
}
