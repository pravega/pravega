/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.testcommon.AssertExtensions;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the FileDescriptor class.
 */
public class FileDescriptorTests {
    /**
     * Tests the ability to change read-only status and lengths.
     */
    @Test
    public void testMutators() {
        FileDescriptor fd = new FileDescriptor(new Path("foo"), 1, 2, 3, false);

        // Length & LastOffset
        Assert.assertEquals("Unexpected initial value for getLastOffset.", 3, fd.getLastOffset());
        fd.increaseLength(10);
        Assert.assertEquals("increaseLength did not increase length.", 12, fd.getLength());
        Assert.assertEquals("Unexpected value for getLastOffset after increaseLength.", 13, fd.getLastOffset());

        fd.setLength(123);
        Assert.assertEquals("setLength did not increase length.", 123, fd.getLength());
        Assert.assertEquals("Unexpected value for getLastOffset after setLength.", 124, fd.getLastOffset());

        fd.markReadOnly();
        Assert.assertTrue("markReadOnly did not mark the descriptor as read-only.", fd.isReadOnly());

        AssertExtensions.assertThrows(
                "setLength did not fail for a read-only file",
                () -> fd.setLength(1234),
                ex -> ex instanceof IllegalStateException);
        AssertExtensions.assertThrows(
                "increaseLength did not fail for a read-only file",
                () -> fd.increaseLength(1234),
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("setLength did increased length after failed call.", 123, fd.getLength());
    }
}
