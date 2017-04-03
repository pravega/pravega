/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.storage.StorageNotPrimaryException;
import com.emc.pravega.testcommon.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ConcatOperation class.
 */
public class ConcatOperationTests extends FileSystemOperationTestBase {
    private static final String TARGET_SEGMENT = "target" + FileSystemOperation.PART_SEPARATOR + "segment";
    private static final String SOURCE_SEGMENT = "source" + FileSystemOperation.PART_SEPARATOR + "segment#1234";
    private static final int WRITE_LENGTH = 100;
    private final Random rnd = new Random(0);

    /**
     * Tests various combinations of bad input to the Concat command.
     */
    @Test
    public void testInvalidInput() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        val targetHandle = createNonEmptySegment(TARGET_SEGMENT, context, null);
        val sourceHandle = createNonEmptySegment(SOURCE_SEGMENT, context, null);

        // Same source and target.
        AssertExtensions.assertThrows(
                "ConcatOperation worked with same source and target.",
                () -> new ConcatOperation(targetHandle, WRITE_LENGTH, TARGET_SEGMENT, context).run(),
                ex -> ex instanceof IllegalArgumentException);

        // Verify source is not sealed.
        AssertExtensions.assertThrows(
                "ConcatOperation worked on non-sealed source segment.",
                new ConcatOperation(targetHandle, WRITE_LENGTH, SOURCE_SEGMENT, context)::run,
                ex -> ex instanceof IllegalStateException);

        // Seal it.
        new SealOperation(sourceHandle, context).run();

        // Verify target offset.
        AssertExtensions.assertThrows(
                "ConcatOperation worked with bad offset.",
                new ConcatOperation(targetHandle, WRITE_LENGTH - 1, SOURCE_SEGMENT, context)::run,
                ex -> ex instanceof BadOffsetException);

        // Verify target is sealed.
        new SealOperation(targetHandle, context).run();
        AssertExtensions.assertThrows(
                "ConcatOperation worked with sealed target.",
                new ConcatOperation(targetHandle, WRITE_LENGTH, SOURCE_SEGMENT, context)::run,
                ex -> ex instanceof AclException);
    }

    /**
     * Tests a normal concatenation for single-file sources.
     */
    @Test
    public void testConcatSingleFile() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        @Cleanup
        ByteArrayOutputStream expectedData = new ByteArrayOutputStream();

        // Create target and write once to it.
        val targetHandle = createNonEmptySegment(TARGET_SEGMENT, context, expectedData);
        int expectedFileCount = targetHandle.getFiles().size();

        // Create source and write once to it, then seal it.
        val sourceHandle = createNonEmptySegment(SOURCE_SEGMENT, context, expectedData);
        new SealOperation(sourceHandle, context).run();
        expectedFileCount += sourceHandle.getFiles().size();

        // Concatenate source into target.
        new ConcatOperation(targetHandle, WRITE_LENGTH, SOURCE_SEGMENT, context).run();
        expectedFileCount++; // No empty files, plus we create a new empty file at the end for fencing.
        verifyConcatOutcome(targetHandle, expectedFileCount, expectedData, context);
    }

    /**
     * Tests a normal concatenation for empty source segment.
     */
    @Test
    public void testConcatEmptySource() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        // Empty Source.
        ByteArrayOutputStream expectedData = new ByteArrayOutputStream();
        HDFSSegmentHandle targetHandle = createNonEmptySegment(TARGET_SEGMENT, context, expectedData);
        createEmptySegment(SOURCE_SEGMENT, context);
        HDFSSegmentHandle sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
        new SealOperation(sourceHandle, context).run();
        int expectedFileCount = targetHandle.getFiles().size();
        new ConcatOperation(targetHandle, WRITE_LENGTH, SOURCE_SEGMENT, context).run();
        verifyConcatOutcome(targetHandle, expectedFileCount, expectedData, context);
    }

    /**
     * Tests a normal concatenation for empty target segment (files).
     */
    @Test
    public void testConcatEmptyTarget() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        // Empty Source.
        ByteArrayOutputStream expectedData = new ByteArrayOutputStream();
        createEmptySegment(TARGET_SEGMENT, context);
        HDFSSegmentHandle targetHandle = new OpenWriteOperation(TARGET_SEGMENT, context).call();
        HDFSSegmentHandle sourceHandle = createNonEmptySegment(SOURCE_SEGMENT, context, expectedData);
        new SealOperation(sourceHandle, context).run();
        int expectedFileCount = sourceHandle.getFiles().size();
        new ConcatOperation(targetHandle, 0, SOURCE_SEGMENT, context).run();
        expectedFileCount++; //We create an empty new file to re-fence the segment.
        verifyConcatOutcome(targetHandle, expectedFileCount, expectedData, context);
    }

    /**
     * Tests a normal concatenation for multi-file sources.
     */
    @Test
    public void testConcatMultipleFiles() throws Exception {
        final int epochs = 50;
        @Cleanup
        val fs = new MockFileSystem();

        @Cleanup
        ByteArrayOutputStream targetDataStream = new ByteArrayOutputStream();
        @Cleanup
        ByteArrayOutputStream sourceDataStream = new ByteArrayOutputStream();

        TestContext context = newContext(0, fs);
        createEmptySegment(TARGET_SEGMENT, context);
        createEmptySegment(SOURCE_SEGMENT, context);
        for (int epoch = 0; epoch < epochs; epoch++) {
            context = newContext(epoch, fs);

            // Write to target.
            byte[] data = new byte[WRITE_LENGTH];
            rnd.nextBytes(data);
            val targetHandle = new OpenWriteOperation(TARGET_SEGMENT, context).call();
            new WriteOperation(targetHandle, targetDataStream.size(), new ByteArrayInputStream(data), data.length, context).run();
            targetDataStream.write(data);

            // Write to source.
            data = new byte[WRITE_LENGTH];
            rnd.nextBytes(data);
            val sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
            new WriteOperation(sourceHandle, sourceDataStream.size(), new ByteArrayInputStream(data), data.length, context).run();
            sourceDataStream.write(data);
        }

        // Concat.
        val targetHandle = new OpenWriteOperation(TARGET_SEGMENT, context).call();
        val sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
        new SealOperation(sourceHandle, context).run();
        int expectedFileCount = targetHandle.getFiles().size() + sourceHandle.getFiles().size();
        new ConcatOperation(targetHandle, targetDataStream.size(), SOURCE_SEGMENT, context).run();
        expectedFileCount++; // Because we add an empty file for fencing.
        targetDataStream.write(sourceDataStream.toByteArray());

        verifyConcatOutcome(targetHandle, expectedFileCount, targetDataStream, context);
    }

    /**
     * Tests the scenario where the concat.next attributes result in a circular dependency.
     * Expected outcome: Exception, and no change to the file system.
     */
    @Test
    public void testCircularDependencies() throws Exception {
        final int epochs = 10;
        @Cleanup
        val fs = new MockFileSystem();

        @Cleanup
        ByteArrayOutputStream targetDataStream = new ByteArrayOutputStream();

        TestContext context = newContext(0, fs);
        createNonEmptySegment(TARGET_SEGMENT, context, targetDataStream);

        // Write a number of files to the Source, then seal it.
        createEmptySegment(SOURCE_SEGMENT, context);
        for (int epoch = 0; epoch < epochs; epoch++) {
            context = newContext(epoch, fs);

            byte[] data = new byte[WRITE_LENGTH];
            rnd.nextBytes(data);
            val sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
            new WriteOperation(sourceHandle, epoch * WRITE_LENGTH, new ByteArrayInputStream(data), data.length, context).run();
        }

        new SealOperation(new OpenWriteOperation(SOURCE_SEGMENT, context).call(), context).run();

        // We attempt to set a circular dependency from the last file in the chain to each file in both target and source.
        val targetHandle = new OpenWriteOperation(TARGET_SEGMENT, context).call();
        val originalTargetFiles = new ArrayList<FileDescriptor>(targetHandle.getFiles());
        val originalSourceFiles = context.findAllFiles(SOURCE_SEGMENT);
        val corruptedFile = originalSourceFiles.get(originalSourceFiles.size() - 2);
        val fakeTargets = new ArrayList<FileDescriptor>(targetHandle.getFiles());
        fakeTargets.addAll(originalSourceFiles);
        fakeTargets.remove(fakeTargets.size() - 1); // This is a valid target (we change the second-to-last file).

        for (FileDescriptor fakeTarget : fakeTargets) {
            // Pre-populate the concat chain attributes correctly.
            val op = new ConcatOperation(targetHandle, targetDataStream.size(), SOURCE_SEGMENT, context);
            op.prepareConcatenation();

            // And point the last file to the a file one in the target segment.
            context.setConcatNext(corruptedFile, fakeTarget);

            AssertExtensions.assertThrows(
                    String.format("Circular dependency not detected when pointing from '%s' to '%s'.", corruptedFile, fakeTarget),
                    op::resumeConcatenation,
                    ex -> ex instanceof SegmentFilesCorruptedException);

            // Verify no side effects.
            val testSourceFiles = context.findAllFiles(SOURCE_SEGMENT);
            AssertExtensions.assertListEquals("Unexpected files in Source Segment in filesystem.",
                    originalSourceFiles, testSourceFiles, this::fileDescriptorEquals);

            val testTargetFiles = context.findAllFiles(TARGET_SEGMENT);
            AssertExtensions.assertListEquals("Unexpected files in Target Segment in filesystem.",
                    originalTargetFiles, testTargetFiles, this::fileDescriptorEquals);
        }
    }

    /**
     * Tests the scenario where the concat operation fails because of existing targets (when renaming).
     * Expected outcome: Exception, and no change to the file system.
     */
    @Test
    public void testExistingTargets() throws Exception {
        final int epochs = 5;
        @Cleanup
        val fs = new MockFileSystem();

        @Cleanup
        ByteArrayOutputStream targetDataStream = new ByteArrayOutputStream();

        TestContext context = newContext(0, fs);
        createNonEmptySegment(TARGET_SEGMENT, context, targetDataStream);

        // Write a number of files to the Source, then seal it.
        createEmptySegment(SOURCE_SEGMENT, context);
        for (int epoch = 0; epoch < epochs; epoch++) {
            context = newContext(epoch, fs);

            byte[] data = new byte[WRITE_LENGTH];
            rnd.nextBytes(data);
            val sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
            new WriteOperation(sourceHandle, epoch * WRITE_LENGTH, new ByteArrayInputStream(data), data.length, context).run();
        }

        new SealOperation(new OpenWriteOperation(SOURCE_SEGMENT, context).call(), context).run();

        // We attempt to set a circular dependency from the last file in the chain to each file in both target and source.
        val targetHandle = new OpenWriteOperation(TARGET_SEGMENT, context).call();
        val originalTargetFiles = new ArrayList<FileDescriptor>(targetHandle.getFiles());
        val originalSourceFiles = context.findAllFiles(SOURCE_SEGMENT);

        // We create an empty file which will match one of the rename targets (but not the first one).
        val collisionFilePath = context.createEmptyFile(TARGET_SEGMENT, targetDataStream.size() + WRITE_LENGTH * 3);

        // Pre-populate the concat chain attributes correctly.
        AssertExtensions.assertThrows(
                String.format("Concat operation did not fail when a file collision exists for '%s'.", collisionFilePath.toString()),
                new ConcatOperation(targetHandle, targetDataStream.size(), SOURCE_SEGMENT, context)::run,
                ex -> ex instanceof SegmentFilesCorruptedException);

        // Remove the collision file (so that we may proceed with the rest of the test).
        context.fileSystem.delete(collisionFilePath, true);

        // Verify no side effects.
        val testSourceFiles = context.findAllFiles(SOURCE_SEGMENT);
        AssertExtensions.assertListEquals("Unexpected files in Source Segment in filesystem.",
                originalSourceFiles, testSourceFiles, this::fileDescriptorEquals);

        val testTargetFiles = context.findAllFiles(TARGET_SEGMENT);
        AssertExtensions.assertListEquals("Unexpected files in Target Segment in filesystem.",
                originalTargetFiles, testTargetFiles, this::fileDescriptorEquals);
    }

    /**
     * Tests the scenario where the concat operation is interrupted mid-way and leaves the system in limbo.
     * Verifies that once the concat operation began, accessing the source segment will not work (FileNotFound), while
     * accessing the target segment will result in the operation being completed.
     * <p>
     * There are a few sub-scenarios of this.
     * 1. The operation failed mid-way. This means we may have renamed a few files. At this point, the target handle
     * should be "locked" (last file is read-only). This means in order to use it again, it has to be reopened.
     * 2. The entire process crash. The next time we access the target segment, we have to open it.
     * <p>
     * Upon opening it, the concat operation should be resumed in order to get the two segments out of limbo. This is what
     * this unit test is verifying.
     */
    @Test
    public void testResumeAfterFailure() throws Exception {
        final int epochs = 10;
        @Cleanup
        val fs = new MockFileSystem();

        @Cleanup
        ByteArrayOutputStream expectedDataStream = new ByteArrayOutputStream();

        TestContext context = newContext(0, fs);
        createNonEmptySegment(TARGET_SEGMENT, context, expectedDataStream);
        int expectedFileCount = 1;

        // Write a number of files to the Source, then seal it.
        createEmptySegment(SOURCE_SEGMENT, context);
        for (int epoch = 0; epoch < epochs; epoch++) {
            context = newContext(epoch, fs);

            byte[] data = new byte[WRITE_LENGTH];
            rnd.nextBytes(data);
            val sourceHandle = new OpenWriteOperation(SOURCE_SEGMENT, context).call();
            new WriteOperation(sourceHandle, epoch * WRITE_LENGTH, new ByteArrayInputStream(data), data.length, context).run();
            expectedDataStream.write(data);
            expectedFileCount++;
        }

        new SealOperation(new OpenWriteOperation(SOURCE_SEGMENT, context).call(), context).run();
        val targetHandle1 = new OpenWriteOperation(TARGET_SEGMENT, context).call();

        // Interrupt the concat mid-way.
        AtomicInteger callCount = new AtomicInteger();
        fs.setOnRename(p -> fs.new ThrowException(p,
                () -> callCount.incrementAndGet() < epochs / 2 ? null : new IOException("Intentional")));

        AssertExtensions.assertThrows(
                "Concat operation was not interrupted with the right exception.",
                new ConcatOperation(targetHandle1, targetHandle1.getLastFile().getLastOffset(), SOURCE_SEGMENT, context)::run,
                ex -> ex instanceof IOException && ex.getMessage().contains("Intentional"));

        // Verify source segment is no longer visible.
        boolean sourceExists = new ExistsOperation(SOURCE_SEGMENT, context).call();
        Assert.assertFalse("Source segment still exists after failed concat.", sourceExists);
        AssertExtensions.assertThrows(
                "GetInfo did not throw the correct exception for partially concat-ted segment.",
                new GetInfoOperation(SOURCE_SEGMENT, context)::call,
                ex -> ex instanceof FileNotFoundException);

        // Verify target handle is unusable.
        AssertExtensions.assertThrows(
                "Target handle is still usable after failed concat.",
                new WriteOperation(targetHandle1, targetHandle1.getLastFile().getLastOffset(), new ByteArrayInputStream(new byte[1]), 1, context)::run,
                ex -> ex instanceof StorageNotPrimaryException);

        // Open new handle and verify it was corrected.
        fs.setOnRename(null); // Remove intentional failure.
        val targetHandle2 = new OpenWriteOperation(TARGET_SEGMENT, context).call();
        expectedFileCount++; // Because we create an empty file at the end for fencing.
        verifyConcatOutcome(targetHandle2,expectedFileCount,expectedDataStream,context);
    }

    private HDFSSegmentHandle createNonEmptySegment(String segmentName, TestContext context, OutputStream expectedDataStream) throws Exception {
        createEmptySegment(segmentName, context);
        val handle = new OpenWriteOperation(segmentName, context).call();
        byte[] writeData = new byte[WRITE_LENGTH];
        rnd.nextBytes(writeData);
        new WriteOperation(handle, 0, new ByteArrayInputStream(writeData), writeData.length, context).run();
        if (expectedDataStream != null) {
            expectedDataStream.write(writeData);
        }

        return handle;
    }

    private void createEmptySegment(String segmentName, TestContext context) throws Exception {
        new CreateOperation(segmentName, context).call();
    }

    private void verifyConcatOutcome(HDFSSegmentHandle targetHandle, int expectedFileCount,
                                     ByteArrayOutputStream expectedDataStream, TestContext context) throws Exception {
        // Verify source segment is deleted.
        boolean sourceExists = new ExistsOperation(SOURCE_SEGMENT, context).call();
        Assert.assertFalse("Source segment still exists.", sourceExists);

        // Verify output.
        byte[] expectedData = expectedDataStream.toByteArray();
        val targetInfo = new GetInfoOperation(TARGET_SEGMENT, context).call();

        // Validate Handle.
        Assert.assertEquals("Unexpected value for TargetHandle.files.count.", expectedFileCount, targetHandle.getFiles().size());
        Assert.assertEquals("Unexpected value for TargetHandle.lastFile.lastOffset.", expectedData.length, targetHandle.getLastFile().getLastOffset());
        Assert.assertFalse("Unexpected value for TargetHandle.lastFile.readOnly.", targetHandle.getLastFile().isReadOnly());

        // Validate handle files against actual filesystem files.
        val fileSystemFiles = context.findAllFiles(TARGET_SEGMENT);
        AssertExtensions.assertListEquals("Unexpected files in TargetHandle.", fileSystemFiles, targetHandle.getFiles(), this::fileDescriptorEquals);

        // Validate file system.
        Assert.assertEquals("Unexpected value for FileSystem.length.", expectedData.length, targetInfo.getLength());
        Assert.assertFalse("Unexpected value for FileSystem.sealed.", targetInfo.isSealed());

        byte[] actualData = new byte[expectedData.length];
        int bytesRead = new ReadOperation(targetHandle, 0, actualData, 0, actualData.length, context).call();
        Assert.assertEquals("Unexpected number of bytes read from target file.", actualData.length, bytesRead);
        Assert.assertArrayEquals("Unexpected data read from target file.", expectedData, actualData);
    }

    private boolean fileDescriptorEquals(FileDescriptor e, FileDescriptor a) {
        return e.getPath().equals(a.getPath())
                && e.isReadOnly() == a.isReadOnly()
                && e.getOffset() == a.getOffset()
                && e.getLength() == a.getLength()
                && e.getEpoch() == a.getEpoch();
    }
}
