/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CreateOperation class.
 */
public class CreateOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";

    /**
     * Tests CreateOperation with no fencing involved. Verifies basic segment creation works, as well as rejection in
     * case the segment already exists.
     */
    @Test
    public void testNormalCall() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);
        val result = new CreateOperation(SEGMENT_NAME, context).call();
        Assert.assertNotNull("Unexpected result from successful CreateOperation.", result);
        checkFileExists(context);

        AssertExtensions.assertThrows(
                "Segment was created twice with the same name.",
                new CreateOperation(SEGMENT_NAME, context)::call,
                ex -> ex instanceof FileAlreadyExistsException);

        fs.clear();
        context.createEmptyFile(SEGMENT_NAME, 10);
        AssertExtensions.assertThrows(
                "Segment was created twice when file with different offset exists.",
                new CreateOperation(SEGMENT_NAME, context)::call,
                ex -> ex instanceof FileAlreadyExistsException);
    }

    /**
     * Tests CreateOperation with fencing resolution.
     */
    @Test
    public void testFencedOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(2, fs);

        // This should wipe out the file created by the first call.
        new CreateOperation(SEGMENT_NAME, context2).call();

        // This should fail
        AssertExtensions.assertThrows(
                "Lower epoch segment creation did not fail.",
                new CreateOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException || ex instanceof FileAlreadyExistsException);
    }

    private void checkFileExists(TestContext context) throws Exception {
        String expectedFileName = context.getFileName(SEGMENT_NAME, 0);
        val fsStatus = context.fileSystem.getFileStatus(new Path(expectedFileName));
        Assert.assertEquals("Created file is not empty.", 0, fsStatus.getLen());
        Assert.assertFalse("Created file is read-only.", context.isReadOnly(fsStatus));
    }
}
