/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.hdfs;

import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the DeleteOperation class.
 */
public class DeleteOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;

    /**
     * Tests the ability to delete segments without outside interference.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testNormalDelete() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        createFiles(fs);

        // Delete it.
        val deleteContext = newContext(FILE_COUNT, fs);
        val handle = new OpenWriteOperation(SEGMENT_NAME, deleteContext).call();
        new DeleteOperation(handle, deleteContext).run();
        Assert.assertEquals("Not all files were deleted.", 0, fs.getFileCount());

        AssertExtensions.assertThrows(
                "Delete worked on non-existent segment.",
                new DeleteOperation(handle, deleteContext)::run,
                ex -> ex instanceof FileNotFoundException);
    }

    /**
     * Tests the ability to delete segment when an outside interference happens.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentDelete() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        createFiles(fs);
        AtomicBoolean interfered = new AtomicBoolean();
        val deleteContext = newContext(FILE_COUNT, fs);
        val handle = new OpenWriteOperation(SEGMENT_NAME, deleteContext).call();
        fs.setOnDelete(path -> {
            if (!interfered.getAndSet(true)) {
                // Create exactly one file back.
                return fs.new CreateNewFileAction(deleteContext.getFileName(SEGMENT_NAME, 0));
            }

            return null;
        });

        new DeleteOperation(handle, deleteContext).run();
        Assert.assertEquals("Not all files were deleted.", 0, fs.getFileCount());
    }

    private void createFiles(MockFileSystem fs) throws Exception {
        // Create a set of files, each 1 byte long.
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        for (int i = 0; i < FILE_COUNT; i++) {
            val context = newContext(i, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            new WriteOperation(handle, i, new ByteArrayInputStream(new byte[]{(byte) i}), 1, context).run();
        }
    }
}
