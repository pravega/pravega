/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.contracts.SegmentProperties;
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
 * Tests the GetInfoOperation class.
 */
public class GetInfoOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    private static final int FILE_COUNT = 10;

    @Test
    public void testGetInfo() throws Exception {
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

        val getInfoContext = newContext(FILE_COUNT, fs);
        SegmentProperties result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("pre-seal", result, expectedLength, false);

        // Seal.
        val sealHandle = new OpenWriteOperation(SEGMENT_NAME, getInfoContext).call();
        new SealOperation(sealHandle, getInfoContext).run();
        result = new GetInfoOperation(SEGMENT_NAME, getInfoContext).call();
        checkResult("post-seal", result, expectedLength, true);

        // Delete first file.
        fs.delete(new Path(fileList.get(0)), true);
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

    private void checkResult(String stage, SegmentProperties sp, long expectedLength, boolean expectedSealed) {
        Assert.assertNotNull("No result from GetInfoOperation (" + stage + ").", sp);
        Assert.assertEquals("Unexpected name (" + stage + ").", SEGMENT_NAME, sp.getName());
        Assert.assertEquals("Unexpected length (" + stage + ").", expectedLength, sp.getLength());
        Assert.assertEquals("Unexpected sealed status (" + stage + ").", expectedSealed, sp.isSealed());
    }
}
