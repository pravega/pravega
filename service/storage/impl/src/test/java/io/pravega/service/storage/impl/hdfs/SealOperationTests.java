/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.storage.StorageNotPrimaryException;
import io.pravega.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SealOperation class.
 */
public class SealOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";

    /**
     * Tests the case when the last file is non-empty non-read-only. This is a normal operation.
     * Expected outcome: Last file is made read-only and marked as 'sealed'.
     */
    @Test
    public void testLastFileNonReadOnly() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);
        new CreateOperation(SEGMENT_NAME, context).call();
        val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
        new WriteOperation(handle, 0, new ByteArrayInputStream(new byte[1]), 1, context).run();
        new SealOperation(handle, context).run();

        Assert.assertTrue("Last file in handle was not marked as read-only.", handle.getLastFile().isReadOnly());
        Assert.assertTrue("Last file in file system was not set as 'sealed'.", context.isSealed(handle.getLastFile()));
    }

    /**
     * Tests the case when the last file is empty (read-only or not).
     * Expected outcome: Last file is deleted and previous one is set as sealed (except when only one file)
     */
    @Test
    public void testLastFileEmpty() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);

        // Part 1: empty segment.
        new CreateOperation(SEGMENT_NAME, context1).call();
        HDFSSegmentHandle handle = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        new SealOperation(handle, context1).run();

        Assert.assertEquals("Unexpected number of files in the filesystem after sealing empty segment.", 1, fs.getFileCount());
        Assert.assertTrue("Last file in handle was not marked as read-only (empty segment).", handle.getLastFile().isReadOnly());
        Assert.assertTrue("Last file in file system was not set as 'sealed' (empty segment).", context1.isSealed(handle.getLastFile()));

        // Part 2: non-empty segment.
        fs.clear();
        new CreateOperation(SEGMENT_NAME, context1).call();
        handle = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        new WriteOperation(handle, 0, new ByteArrayInputStream(new byte[1]), 1, context1).run();

        val context2 = newContext(context1.epoch + 1, fs);
        new SealOperation(handle, context2).run();

        Assert.assertEquals("Unexpected number of files in the filesystem after sealing segment with empty last file.", 1, fs.getFileCount());
        Assert.assertTrue("Last file in handle was not marked as read-only (empty last file).", handle.getLastFile().isReadOnly());
        Assert.assertTrue("Last file in file system was not set as 'sealed' (empty last file).", context2.isSealed(handle.getLastFile()));
    }

    /**
     * Tests the case when the last file was thought to be read-only but it got changed in the background and
     * fenced out by a higher-epoch instance.
     * Expected outcome: StorageNotPrimaryException.
     */
    @Test
    public void testLastFileEmptyNonReadOnlyFencedOut() throws Exception {
        final String emptySegment = SEGMENT_NAME;
        final String nonEmptySegment = SEGMENT_NAME + "nonempty";
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(context1.epoch + 1, fs);
        new CreateOperation(emptySegment, context1).call();
        val emptySegmentHandle1 = new OpenWriteOperation(emptySegment, context1).call();

        // Part 1: empty segment first file (it will be deleted when OpenWrite is invoked with epoch 2)
        new OpenWriteOperation(emptySegment, context2).call();
        AssertExtensions.assertThrows(
                "SealOperation did not fail when it was fenced out (empty segment).",
                new SealOperation(emptySegmentHandle1, context1)::run,
                ex -> ex instanceof StorageNotPrimaryException);

        Assert.assertFalse("Last file in emptySegmentHandle1 was marked as read-only.", emptySegmentHandle1.getLastFile().isReadOnly());
        Assert.assertFalse("Last file in file system (empty segment) was set as 'sealed'.", fs.exists(emptySegmentHandle1.getLastFile().getPath()));

        // Part 1: non-empty segment first file (it will be marked read-only when OpenWrite is invoked with epoch 2)
        new CreateOperation(nonEmptySegment, context1).call();
        val nonEmptySegmentHandle1 = new OpenWriteOperation(nonEmptySegment, context1).call();
        new WriteOperation(nonEmptySegmentHandle1, 0, new ByteArrayInputStream(new byte[1]), 1, context1).run();
        new OpenWriteOperation(nonEmptySegment, context2).call();
        AssertExtensions.assertThrows(
                "SealOperation did not fail when it was fenced out (non-empty segment).",
                new SealOperation(nonEmptySegmentHandle1, context1)::run,
                ex -> ex instanceof StorageNotPrimaryException);

        Assert.assertFalse("Last file in nonEmptySegmentHandle1 was marked as read-only.", emptySegmentHandle1.getLastFile().isReadOnly());
        Assert.assertFalse("Last file in file system (empty segment) was set as 'sealed'.", context1.isSealed(nonEmptySegmentHandle1.getLastFile()));
    }
}
