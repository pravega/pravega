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

import io.pravega.service.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
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
    @Test (timeout = TEST_TIMEOUT_MILLIS)
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
     * Tests CreateOperation with fencing resolution for lower-epoch creation.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testLowerEpochFencedOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(2, fs);

        // This should wipe out the file created by the first call.
        new CreateOperation(SEGMENT_NAME, context2).call();

        // This should fail because we attempt to create the segment using a lower epoch.
        AssertExtensions.assertThrows(
                "Lower epoch segment creation did not fail.",
                new CreateOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException || ex instanceof FileAlreadyExistsException);
    }

    /**
     * Tests CreateOperation with fencing resolution for concurrent operations.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentFencedOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(2, fs);

        // Part 1: CreateOperation has higher epoch than competitor -> it should succeed.
        Path fencedOutFile = context1.getFileName(SEGMENT_NAME, 0);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(fencedOutFile));

        // This should wipe out the file created by the first call.
        new CreateOperation(SEGMENT_NAME, context2).call();
        checkFileExists(context2);
        Assert.assertFalse("Fenced-out file was not deleted (lower-epoch test).", fs.exists(fencedOutFile));

        // Part 2: CreateOperation has lower epoch than competitor -> it should back off and fail
        fs.clear();
        Path survivingFile = context2.getFileName(SEGMENT_NAME, 0);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(survivingFile));

        // This should wipe out the file created by the first call.
        AssertExtensions.assertThrows(
                "Fenced-out operation did not fail.",
                new CreateOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException);
        checkFileExists(context2);
        Assert.assertFalse("Fenced-out file was not deleted (higher-epoch test).", fs.exists(context1.getFileName(SEGMENT_NAME, 0)));
    }

    private void checkFileExists(TestContext context) throws Exception {
        Path expectedFileName = context.getFileName(SEGMENT_NAME, 0);
        val fsStatus = context.fileSystem.getFileStatus(expectedFileName);
        Assert.assertEquals("Created file is not empty.", 0, fsStatus.getLen());
        Assert.assertFalse("Created file is read-only.", context.isReadOnly(fsStatus));
    }
}
