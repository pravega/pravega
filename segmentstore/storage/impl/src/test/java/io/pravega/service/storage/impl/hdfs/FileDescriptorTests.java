/*
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
    @Test(timeout = 10000)
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
