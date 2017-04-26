/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.contracts.SegmentProperties;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the GetInfoOperation class.
 */
public class GetInfoOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;

    @Test(timeout = 10000)
    public void testGetInfo() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        long expectedLength = 0;
        val fileList = new ArrayList<Path>();
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        for (int i = 0; i < FILE_COUNT; i++) {
            val context = newContext(i, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            fileList.add(handle.getLastFile().getPath());
            byte[] data = new byte[i + 1];
            new WriteOperation(handle, expectedLength, new ByteArrayInputStream(data), data.length, context).run();
            expectedLength += data.length;
        }

        val getInfoContext = newContext(FILE_COUNT, fs);
        SegmentProperties result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("pre-seal", result, expectedLength, false);

        // Seal.
        val sealHandle = new OpenWriteOperation(SEGMENT_NAME, getInfoContext).call();
        new SealOperation(sealHandle, getInfoContext).run();
        result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("post-seal", result, expectedLength, true);

        // Delete first file.
        fs.delete(fileList.get(0), true);
        AssertExtensions.assertThrows(
                "GetInfo succeeded on corrupted segment.",
                new GetInfoOperation(SEGMENT_NAME, getInfoContext)::call,
                ex -> ex instanceof SegmentFilesCorruptedException);

        // Inexistent segment.
        fs.clear();
        AssertExtensions.assertThrows(
                "GetInfo succeeded on missing segment.",
                new GetInfoOperation(SEGMENT_NAME, getInfoContext)::call,
                ex -> ex instanceof FileNotFoundException);
    }

    /**
     * Tests the case when a concurrent operation modified (removed) the last file - such as a SealOperation when the last
     * file is empty.
     */
    @Test
    public void testConcurrentLastFileModification() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        long expectedLength = 0;
        val fileList = new ArrayList<Path>();
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        val context = newContext(1, fs);
        val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
        fileList.add(handle.getLastFile().getPath());
        byte[] data = new byte[10];
        new WriteOperation(handle, expectedLength, new ByteArrayInputStream(data), data.length, context).run();
        expectedLength += data.length;

        // Test the case when this is a transient issue, and a refresh (or two) will solve the issue.
        val validResult = new GetInfoOperationWithFakeResult(SEGMENT_NAME, context, 2).call();
        checkResult("valid", validResult, expectedLength, false);

        // Test the case when this is a real deletion/corruption, from which there is no recovery.
        AssertExtensions.assertThrows(
                "Unexpected behavior from GetInfoOperation when unrecoverable file system corruption was injected.",
                new GetInfoOperationWithFakeResult(SEGMENT_NAME, context, 10)::call,
                ex -> ex instanceof FileNotFoundException);
    }

    private void checkResult(String stage, SegmentProperties sp, long expectedLength, boolean expectedSealed) {
        Assert.assertNotNull("No result from GetInfoOperation (" + stage + ").", sp);
        Assert.assertEquals("Unexpected name (" + stage + ").", SEGMENT_NAME, sp.getName());
        Assert.assertEquals("Unexpected length (" + stage + ").", expectedLength, sp.getLength());
        Assert.assertEquals("Unexpected sealed status (" + stage + ").", expectedSealed, sp.isSealed());
    }

    private static class GetInfoOperationWithFakeResult extends GetInfoOperation {
        private final AtomicInteger findAllFakeCount;

        GetInfoOperationWithFakeResult(String segmentName, OperationContext context, int findAllFakeCount) {
            super(segmentName, context);
            this.findAllFakeCount = new AtomicInteger(findAllFakeCount);
        }

        @Override
        List<FileDescriptor> findAll(String segmentName, boolean enforceExistence) throws IOException {
            val result = super.findAll(segmentName, enforceExistence);
            if (this.findAllFakeCount.getAndDecrement() > 0) {
                val lastFile = result.get(result.size() - 1);
                result.add(new FileDescriptor(new Path("foo"), lastFile.getLastOffset(), 0, lastFile.getEpoch(), lastFile.isReadOnly()));
            }

            return result;
        }
    }
}
