/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.contracts.BadOffsetException;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Random;
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
    @Test (timeout = TEST_TIMEOUT_MILLIS)
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
    @Test (timeout = TEST_TIMEOUT_MILLIS)
    public void testConcatSingleFile() throws Exception {
        @Cleanup
        val fs = new MockFileSystem();
        val context = newContext(1, fs);

        @Cleanup
        ByteArrayOutputStream expectedData = new ByteArrayOutputStream();

        // Create target and write once to it.
        val targetHandle = createNonEmptySegment(TARGET_SEGMENT, context, expectedData);

        // Create source and write once to it, then seal it.
        val sourceHandle = createNonEmptySegment(SOURCE_SEGMENT, context, expectedData);
        new SealOperation(sourceHandle, context).run();

        // Concatenate source into target.
        new ConcatOperation(targetHandle, WRITE_LENGTH, SOURCE_SEGMENT, context).run();

        // In the end, we expect a single target file (since we concat a file into a read-write file).
        verifyConcatOutcome(targetHandle, 1, expectedData, context);
    }

    /**
     * Tests a normal concatenation for empty source segment.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
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
    @Test (timeout = TEST_TIMEOUT_MILLIS)
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
        new ConcatOperation(targetHandle, 0, SOURCE_SEGMENT, context).run();
        verifyConcatOutcome(targetHandle, 1, expectedData, context);
    }

    /**
     * Tests a normal concatenation for multi-file sources.
     */
    @Test (timeout = TEST_TIMEOUT_MILLIS)
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
        final int expectedFileCount = targetHandle.getFiles().size();
        new ConcatOperation(targetHandle, targetDataStream.size(), SOURCE_SEGMENT, context).run();
        targetDataStream.write(sourceDataStream.toByteArray());
        verifyConcatOutcome(targetHandle, expectedFileCount, targetDataStream, context);
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
