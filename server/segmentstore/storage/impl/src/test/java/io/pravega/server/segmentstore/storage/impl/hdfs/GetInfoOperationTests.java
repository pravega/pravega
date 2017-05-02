/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.storage.impl.hdfs;

import io.pravega.server.segmentstore.contracts.SegmentProperties;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the GetInfoOperation class.
 */
public class GetInfoOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int WRITE_COUNT = 10;

    /**
     * Tests general GetInfoOperation behavior.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testGetInfo() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        long expectedLength = 0;
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        for (int i = 0; i < WRITE_COUNT; i++) {
            val context = newContext(i, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            byte[] data = new byte[i + 1];
            new WriteOperation(handle, expectedLength, new ByteArrayInputStream(data), data.length, context).run();
            expectedLength += data.length;
        }

        val getInfoContext = newContext(WRITE_COUNT, fs);
        SegmentProperties result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("pre-seal", result, expectedLength, false);

        // Seal.
        val sealHandle = new OpenWriteOperation(SEGMENT_NAME, getInfoContext).call();
        new SealOperation(sealHandle, getInfoContext).run();
        result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("post-seal", result, expectedLength, true);

        // Inexistent segment.
        fs.clear();
        AssertExtensions.assertThrows(
                "GetInfo succeeded on missing segment.",
                new GetInfoOperation(SEGMENT_NAME, getInfoContext)::call,
                ex -> ex instanceof FileNotFoundException);
    }

    /**
     * Tests the behavior of the GetInfoOperation on a segment that is missing the first file.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testCorruptedSegment() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();

        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        new WriteOperation(handle1, 0, new ByteArrayInputStream(new byte[1]), 1, context1).run();

        val context2 = newContext(context1.epoch + 1, fs);
        val handle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();
        new WriteOperation(handle2, 1, new ByteArrayInputStream(new byte[1]), 1, context1).run();

        // Delete first file.
        fs.delete(handle2.getFiles().get(0).getPath(), true);
        AssertExtensions.assertThrows(
                "GetInfo succeeded on corrupted segment.",
                new GetInfoOperation(SEGMENT_NAME, context2)::call,
                ex -> ex instanceof SegmentFilesCorruptedException);
    }

    private void checkResult(String stage, SegmentProperties sp, long expectedLength, boolean expectedSealed) {
        Assert.assertNotNull("No result from GetInfoOperation (" + stage + ").", sp);
        Assert.assertEquals("Unexpected name (" + stage + ").", SEGMENT_NAME, sp.getName());
        Assert.assertEquals("Unexpected length (" + stage + ").", expectedLength, sp.getLength());
        Assert.assertEquals("Unexpected sealed status (" + stage + ").", expectedSealed, sp.isSealed());
    }
}
