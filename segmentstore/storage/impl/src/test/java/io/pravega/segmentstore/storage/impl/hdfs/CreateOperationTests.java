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

import com.google.common.util.concurrent.Runnables;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.function.RunnableWithException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.test.common.AssertExtensions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the CreateOperation class.
 */
public class CreateOperationTests extends FileSystemOperationTestBase {
    private static final String SEGMENT_NAME = "segment";
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT_SECONDS);

    /**
     * Tests CreateOperation with no fencing involved. Verifies basic segment creation works, as well as rejection in
     * case the segment already exists.
     */
    @Test
    public void testNormalCall() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);
        val result = new CreateOperation(SEGMENT_NAME, context).call();
        Assert.assertNotNull("Unexpected result from successful CreateOperation.", result);
        checkFileExists(context);

        AssertExtensions.assertThrows(
                "Segment was created twice with the same name.",
                new CreateOperation(SEGMENT_NAME, context)::call,
                ex -> ex instanceof FileAlreadyExistsException);

        fs.clear();
        context.createEmptyFile(SEGMENT_NAME, 10);
        AssertExtensions.assertThrows(
                "Segment was created twice when file with different offset exists.",
                new CreateOperation(SEGMENT_NAME, context)::call,
                ex -> ex instanceof FileAlreadyExistsException);
    }

    /**
     * Tests CreateOperation with fencing resolution for lower-epoch creation.
     */
    @Test
    public void testLowerEpochFencedOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(2, fs);

        // This should wipe out the file created by the first call.
        new CreateOperation(SEGMENT_NAME, context2).call();

        // This should fail because we attempt to create the segment using a lower epoch.
        AssertExtensions.assertThrows(
                "Lower epoch segment creation did not fail.",
                new CreateOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException || ex instanceof FileAlreadyExistsException);
    }

    /**
     * Tests CreateOperation with fencing resolution for concurrent operations.
     */
    @Test
    public void testConcurrentFencedOut() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context1 = newContext(1, fs);
        val context2 = newContext(2, fs);

        // Part 1: CreateOperation has higher epoch than competitor -> it should succeed.
        Path fencedOutFile = context1.getFileName(SEGMENT_NAME, 0);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(fencedOutFile));

        // This should wipe out the file created by the first call.
        new CreateOperation(SEGMENT_NAME, context2).call();
        checkFileExists(context2);
        Assert.assertFalse("Fenced-out file was not deleted (lower-epoch test).", fs.exists(fencedOutFile));

        // Part 2: CreateOperation has lower epoch than competitor -> it should back off and fail
        fs.clear();
        Path survivingFile = context2.getFileName(SEGMENT_NAME, 0);
        fs.setOnCreate(path -> fs.new CreateNewFileAction(survivingFile));

        // This should wipe out the file created by the first call.
        AssertExtensions.assertThrows(
                "Fenced-out operation did not fail.",
                new CreateOperation(SEGMENT_NAME, context1)::call,
                ex -> ex instanceof StorageNotPrimaryException);
        checkFileExists(context2);
        Assert.assertFalse("Fenced-out file was not deleted (higher-epoch test).", fs.exists(context1.getFileName(SEGMENT_NAME, 0)));
    }

    /**
     * Tests CreateOperation for two concurrent Create requests on the same segment. This actually tests atomicCreate()
     * on FileSystemOperation.
     */
    @Test
    public void testAtomicCreate() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);
        val path = context.getFileName(SEGMENT_NAME, 0);

        val wasInvoked = new AtomicBoolean(); // Verifies we don't invoke FileSystem.create() multiple times.
        val creationInvoked = new CompletableFuture<Void>(); // Completed when FileSystem.create() is in progress.
        val creationReleased = new CompletableFuture<Void>(); // Completed when ready to unblock FileSystem.create().
        val creationCompleted = new CompletableFuture<Void>(); // Completed when FileSystem.create() is done.
        fs.setOnCreate(p -> {
            Assert.assertFalse("Multiple invocations to FileSystem.create().", wasInvoked.getAndSet(true));
            creationInvoked.complete(null);
            return fs.new WaitAction(path, creationReleased);
        });

        // Execute the first create and wait for it to have been invoked (which means we should have registered the creation).
        val op1 = new AtomicCreateOperation(SEGMENT_NAME, context);
        ExecutorServiceHelpers.execute(
                () -> {
                    op1.run();
                    creationCompleted.complete(null);
                },
                ex -> Assert.fail("Unexpected exception on first call. " + ex),
                Runnables.doNothing(),
                ForkJoinPool.commonPool());
        creationInvoked.join();

        // Execute the second operation and verify it fails.
        val op2 = new AtomicCreateOperation(SEGMENT_NAME, context);
        AssertExtensions.assertThrows(
                "Second concurrent call did not fail with appropriate exception.",
                op2::run,
                ex -> ex instanceof FileAlreadyExistsException);

        // Complete the first creation, wait for it to actually be done, and verify the file has been created.
        creationReleased.complete(null);
        creationCompleted.join();
        checkFileExists(context);

        AssertExtensions.assertThrows(
                "Non-concurrent call did not fail with appropriate exception.",
                op2::run,
                ex -> ex instanceof FileAlreadyExistsException);
    }

    private void checkFileExists(TestContext context) throws Exception {
        Path expectedFileName = context.getFileName(SEGMENT_NAME, 0);
        val fsStatus = context.fileSystem.getFileStatus(expectedFileName);
        Assert.assertEquals("Created file is not empty.", 0, fsStatus.getLen());
        Assert.assertFalse("Created file is read-only.", context.isReadOnly(fsStatus));
    }

    /**
     * CreateOperation does a few checks before actually executing, so it's not very useful in testing atomicCreate().
     * This class invokes that method directly, which gives us more testing control over it.
     */
    private static class AtomicCreateOperation extends FileSystemOperation<String> implements RunnableWithException {
        AtomicCreateOperation(String target, OperationContext context) {
            super(target, context);
        }

        @Override
        public void run() throws Exception {
            atomicCreate(getFilePath(getTarget(), 0, this.context.epoch));
        }
    }
}
