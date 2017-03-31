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
 * Unit tests for the DeleteOperation class.
 */
public class DeleteOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";

    /**
     * Tests the ability to delete segments.
     */
    @Test
    public void testDelete() throws Exception {
        final int fileCount = 10;

        // Create a segment made of multiple files, each 1 byte long.
        val fileList = new ArrayList<String>();
        @Cleanup
        val fs = new MockFileSystem();
        for (int i = 0; i < fileCount; i++) {
            val context = newContext(i, fs);
            fileList.add(context.getFileName(SEGMENT_NAME, i));
            context.createEmptyFile(SEGMENT_NAME, i);
            val handle = (HDFSSegmentHandle) new OpenWriteOperation(SEGMENT_NAME, context).call();
            new WriteOperation(handle, i, new ByteArrayInputStream(new byte[]{(byte) i}), 1, context).run();
        }

        // Delete it.
        val deleteContext = newContext(fileCount, fs);
        val handle = (HDFSSegmentHandle) new OpenWriteOperation(SEGMENT_NAME, deleteContext).call();
        new DeleteOperation(handle, deleteContext).run();

        for (String path : fileList) {
            Assert.assertFalse("Not all files were deleted.", fs.exists(new Path(path)));
        }

        AssertExtensions.assertThrows(
                "Delete worked on non-existent segment.",
                new DeleteOperation(handle, deleteContext)::run,
                ex -> ex instanceof FileNotFoundException);
    }
}
