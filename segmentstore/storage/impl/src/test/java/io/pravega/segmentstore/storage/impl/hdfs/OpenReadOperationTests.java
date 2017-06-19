/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the OpenReadOperation class.
 */
public class OpenReadOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT_SECONDS);

    /**
     * Tests the OpenReadOperation.
     */
    @Test
    public void testOpenRead() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        long expectedLength = 0;
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        List<FileDescriptor> fileList = null;
        for (int i = 0; i < FILE_COUNT; i++) {
            val context = newContext(i, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            fileList = handle.getFiles();

            byte[] data = new byte[i + 1];
            new WriteOperation(handle, expectedLength, new ByteArrayInputStream(data), data.length, context).run();
            expectedLength += data.length;
        }

        val openContext = newContext(FILE_COUNT, fs);
        HDFSSegmentHandle readHandle = new OpenReadOperation(SEGMENT_NAME, openContext).call();
        Assert.assertTrue("Unexpected value for isReadOnly", readHandle.isReadOnly());
        Assert.assertEquals("Unexpected value for getSegmentName", SEGMENT_NAME, readHandle.getSegmentName());
        Assert.assertEquals("Unexpected count of files.", fileList.size(), readHandle.getFiles().size());
        for (int i = 0; i < fileList.size(); i++) {
            val actualFile = readHandle.getFiles().get(i);
            val expectedFile = fileList.get(i);
            Assert.assertEquals("Unexpected value for FileDescriptor.getPath for index " + i, expectedFile.getPath(), actualFile.getPath());
            Assert.assertEquals("Unexpected value for FileDescriptor.getOffset for index " + i, expectedFile.getOffset(), actualFile.getOffset());
            Assert.assertEquals("Unexpected value for FileDescriptor.getLength for index " + i, expectedFile.getLength(), actualFile.getLength());
            Assert.assertEquals("Unexpected value for FileDescriptor.getEpoch for index " + i, expectedFile.getEpoch(), actualFile.getEpoch());
            Assert.assertEquals("Unexpected value for FileDescriptor.isReadOnly for index " + i, expectedFile.isReadOnly(), actualFile.isReadOnly());
        }

        // Delete first file.
        fs.delete(fileList.get(0).getPath(), true);
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
