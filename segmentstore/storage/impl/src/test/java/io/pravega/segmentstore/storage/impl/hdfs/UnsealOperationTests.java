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

import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the SealOperation class.
 */
public class UnsealOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT_SECONDS);

    /**
     * Tests the basic functionality of Unseal.
     */
    @Test
    public void testUnseal() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        // Create, write and seal a file.
        new CreateOperation(SEGMENT_NAME, context).call();
        val handle = new OpenWriteOperation(SEGMENT_NAME, context).call();
        new WriteOperation(handle, 0, new ByteArrayInputStream(new byte[1]), 1, context).run();
        new SealOperation(handle, context).run();

        new UnsealOperation(handle, context).run();
        Assert.assertFalse("Last file in handle was not marked as read-write.", handle.getLastFile().isReadOnly());
        Assert.assertFalse("Last file in file system was not set as 'not-sealed'.", context.isSealed(handle.getLastFile()));

        // Repeat (this time file is not sealed).
        val handle2 = new OpenReadOperation(SEGMENT_NAME, context).call();
        new UnsealOperation(handle2, context).run();
        Assert.assertFalse("Last file in read-only handle was not marked as read-write.", handle2.getLastFile().isReadOnly());
        Assert.assertFalse("Last file in file system was not set as 'not-sealed' (read-only).", context.isSealed(handle2.getLastFile()));
    }

    /**
     * Tests the ability to detect fence-outs.
     */
    @Test
    public void testFenceOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);

        // Create, write and seal a file. Then open-write it a few times, each time with a higher epoch.
        new CreateOperation(SEGMENT_NAME, context1).call();
        val handle = new OpenWriteOperation(SEGMENT_NAME, context1).call();
        new WriteOperation(handle, 0, new ByteArrayInputStream(new byte[1]), 1, context1).run();

        val context2 = newContext(2, fs);
        val handle2 = new OpenWriteOperation(SEGMENT_NAME, context2).call();
        new WriteOperation(handle2, 1, new ByteArrayInputStream(new byte[1]), 1, context2).run();

        val context3 = newContext(3, fs);
        val handle3 = new OpenWriteOperation(SEGMENT_NAME, context3).call();
        new WriteOperation(handle3, 2, new ByteArrayInputStream(new byte[1]), 1, context3).run();
        new SealOperation(handle3, context3).run();

        val context4 = newContext(4, fs);
        new OpenWriteOperation(SEGMENT_NAME, context4).call();

        // Verify Unseal fails due to fence-out while using a lower epoch.
        AssertExtensions.assertThrows(
                "Unseal did not fail when fenced out.",
                new UnsealOperation(handle2, context2)::run,
                ex -> ex instanceof StorageNotPrimaryException);
    }
}