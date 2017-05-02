/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.storage.impl.hdfs;

import io.pravega.server.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the OpenWriteOperation class
 */
public class OpenWriteOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";

    /**
     * Tests the case when the last file has en epoch larger than ours.
     * Expected outcome: StorageNotPrimaryException and no side effects.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testLargerEpoch() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val lowContext = newContext(1, fs);
        val highContext = newContext(lowContext.epoch + 1, fs);
        new CreateOperation(SEGMENT_NAME, highContext).call();
        val writeHandle = new OpenWriteOperation(SEGMENT_NAME, highContext).call();

        AssertExtensions.assertThrows(
                "OpenWrite allowed opening a segment that does not have the highest epoch.",
                new OpenWriteOperation(SEGMENT_NAME, lowContext)::call,
                ex -> ex instanceof StorageNotPrimaryException);

        Assert.assertEquals("Unexpected number of files in the file system.", 1, fs.getFileCount());
        Assert.assertTrue("Higher epoch file was deleted.", fs.exists(writeHandle.getLastFile().getPath()));
    }

    /**
     * Tests the case when the last file is read-only and sealed.
     * Expected outcome: Return a read-only handle and no side effects.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testReadOnlySealed() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val lowContext = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, lowContext).call();
        val lowContextHandle = new OpenWriteOperation(SEGMENT_NAME, lowContext).call();
        new SealOperation(lowContextHandle, lowContext).run();

        val highContext = newContext(lowContext.epoch + 1, fs);
        val highContextHandle = new OpenWriteOperation(SEGMENT_NAME, highContext).call();
        Assert.assertTrue("OpenWrite did not return a read-only handle for a sealed segment.", highContextHandle.isReadOnly());
        Assert.assertEquals("OpenWrite returned a handle with the wrong number of files.", 1, highContextHandle.getFiles().size());
        Assert.assertEquals("OpenWrite returned a handle with the wrong file.",
                lowContextHandle.getLastFile().getPath(), highContextHandle.getLastFile().getPath());
        Assert.assertEquals("Unexpected number of files in the file system.", 1, fs.getFileCount());
        Assert.assertTrue("Higher epoch file was deleted.", fs.exists(lowContextHandle.getLastFile().getPath()));
    }

    /**
     * Tests the case when the last file is read only but not sealed.
     * Expected outcome: Create new read-write file; don't touch other files.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testReadOnlyNotSealed() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        context1.makeReadOnly(handle1.getLastFile());

        // Read-only file with same epoch: this should be rejected with StorageNotPrimaryException.
        AssertExtensions.assertThrows(
                "OpenWrite did not fail when the last file has the same epoch as the context but is read-only",
                new OpenWriteOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException);

        checkFenceLowerEpochFile(handle1, fs);
    }

    /**
     * Tests the case when the last file is not read only, but it has the same epoch as us.
     * Expected outcome: reuse last file and no side effects.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testNotReadOnlySameEpoch() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context).call();
        Path expectedFile = context.getFileName(SEGMENT_NAME, 0);
        val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();

        Assert.assertEquals("Unexpected number of files in the handle.", 1, handle.getFiles().size());
        Assert.assertEquals("Unexpected file in handle.", expectedFile, handle.getLastFile().getPath());

        Assert.assertEquals("Unexpected number of files in the file system.", 1, fs.getFileCount());
        Assert.assertTrue("Unexpected file in filesystem.", fs.exists(expectedFile));
    }

    /**
     * Tests the case when the last file is not read-only, and it has lower epoch than us.
     * Expected outcome: Make last file read-only and create new one.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testNotReadOnlySmallerEpoch() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle1 = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        checkFenceLowerEpochFile(handle1, fs);
    }

    /**
     * Tests the case when the OpenWriteOperation thinks it is the highest epoch, fences out, but then it finds out
     * it was beaten to it by a higher epoch instance.
     * Expected outcome: StorageNotPrimaryException and no side effects (it should back off).
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentFenceOutLower() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();

        val context2 = newContext(context1.epoch + 1, fs);
        val context3 = newContext(context2.epoch + 1, fs);
        Path survivingFilePath = context3.getFileName(SEGMENT_NAME, 0);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(survivingFilePath));
        AssertExtensions.assertThrows(
                "OpenWrite did not fail when a concurrent higher epoch file was created.",
                new OpenWriteOperation(SEGMENT_NAME, context2)::call,
                ex -> ex instanceof StorageNotPrimaryException);

        // In a real-world situation, we'd have just one surviving file. However we were not able to successfully carry
        // out the fencing operation, hence no cleanup could be done (testConcurrentFenceOutHigher should check this though).
        Assert.assertEquals("Unexpected number of files in the file system.", 2, fs.getFileCount());
        Assert.assertTrue("Original file was deleted.", fs.exists(context1.getFileName(SEGMENT_NAME, 0)));
        Assert.assertTrue("Higher epoch file was deleted.", fs.exists(survivingFilePath));
    }

    /**
     * Tests the case when the OpenWriteOperation correctly thinks it is the highest epoch, fences out, but then it finds out
     * that a lower-epoch file was also created.
     * Expected outcome: succeed and delete the file.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testConcurrentFenceOutHigher() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context1).call();

        val context2 = newContext(context1.epoch + 1, fs);
        val context3 = newContext(context2.epoch + 1, fs);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(context2.getFileName(SEGMENT_NAME, 0)));
        val handle = new OpenWriteOperation(SEGMENT_NAME, context3).call();

        Path survivingFile = context3.getFileName(SEGMENT_NAME, 0);
        Assert.assertEquals("Unexpected number of files in the file system.", 1, fs.getFileCount());
        Assert.assertTrue("Higher epoch file was deleted.", fs.exists(survivingFile));
        Assert.assertEquals("Unexpected number of files in the handle.", 1, handle.getFiles().size());
        Assert.assertEquals("Unexpected file in the handle.", survivingFile, handle.getLastFile().getPath());
    }

    /**
     * Tests the case when the OpenWriteOperation needs to consolidate multiple files of the same segment into fewer by
     * means of concatenation.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testMultiFileCoalescing() throws Exception {
        final int iterations = Byte.MAX_VALUE;
        final int emptyFileEvery = 10;
        val fs = new MockFileSystem();
        new CreateOperation(SEGMENT_NAME, newContext(0, fs)).call();
        byte[] expectedData = new byte[iterations];
        Arrays.fill(expectedData, (byte) -1);
        int epoch = 0;
        for (int iteration = 0; iteration < iterations; iteration++) {
            val context = newContext(epoch++, fs);
            val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
            byte toWrite = (byte) iteration;
            new WriteOperation(handle, iteration, new ByteArrayInputStream(new byte[]{toWrite}), 1, context).run();
            expectedData[iteration] = toWrite;
            if (epoch % emptyFileEvery == 0) {
                // Every now and then, just open the file without writing anything. The next time OpenWrite is called
                // it will be forced to concat an empty file.
                new OpenWriteOperation(SEGMENT_NAME, newContext(epoch++, fs)).call();
            }
        }

        Assert.assertEquals("Unexpected number of files in the file system.", 2, fs.getFileCount());
        val readContext = newContext(iterations, fs);
        val readHandle = new OpenReadOperation(SEGMENT_NAME, readContext).call();
        Assert.assertEquals("Unexpected total segment length in the file system.", expectedData.length, readHandle.getLastFile().getLastOffset());
        byte[] actualData = new byte[expectedData.length];
        new ReadOperation(readHandle, 0, actualData, 0, actualData.length, readContext).call();
        Assert.assertArrayEquals("Unexpected data read back from the file system.", expectedData, actualData);
        Assert.assertTrue("First file in the chain is not read-only.", readHandle.getFiles().get(0).isReadOnly());
    }

    private void checkFenceLowerEpochFile(HDFSSegmentHandle originalHandle, MockFileSystem fs) throws Exception {
        // Empty file: replace.
        val context2 = newContext(originalHandle.getLastFile().getEpoch() + 1, fs);
        val handle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();
        Assert.assertNotEquals("Fencing out empty file did not cause a new one to be created.", originalHandle.getLastFile().getPath(), handle2.getLastFile().getPath());
        Assert.assertEquals("Unexpected number of files in the file system after fencing out empty file.", 1, fs.getFileCount());
        Assert.assertTrue("Higher epoch file is not present after fencing out empty file.", fs.exists(handle2.getLastFile().getPath()));

        // Non-empty file: keep and ensure read-only.
        new WriteOperation(handle2, 0, new ByteArrayInputStream(new byte[1]), 1, context2).run();
        val context3 = newContext(3, fs);
        val handle3 = new OpenWriteOperation(SEGMENT_NAME, context3).call();

        // Check handle.
        Assert.assertNotEquals("Fencing out non-empty file did not cause a new one to be created.", handle2.getLastFile().getPath(), handle3.getLastFile().getPath());
        val handleFiles = handle3.getFiles();
        Assert.assertEquals("Unexpected number of files in the handle after fencing out non-empty file.", 2, handleFiles.size());
        Assert.assertEquals("Unexpected files in the handle after fencing out non-empty file.", handle2.getLastFile().getPath(), handleFiles.get(0).getPath());

        // Check file system.
        Assert.assertEquals("Unexpected number of files in the file system after fencing out non-empty file.", 2, fs.getFileCount());
        Assert.assertTrue("Lower epoch file is not present after fencing out non-empty file.", fs.exists(handle2.getLastFile().getPath()));
        Assert.assertTrue("Lower epoch file is not marked read-only after being fenced-out.", context3.isReadOnly(fs.getFileStatus(handle2.getLastFile().getPath())));
        Assert.assertTrue("Higher epoch file is not present after fencing out non-empty file.", fs.exists(handle3.getLastFile().getPath()));
    }
}
