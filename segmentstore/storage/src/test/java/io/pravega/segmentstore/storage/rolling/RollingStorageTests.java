/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RollingStorage class.
 */
public class RollingStorageTests extends StorageTestBase {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);
    private static final String SEGMENT_NAME = "RollingSegment";
    private static final int SMALL_WRITE_LENGTH = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 0.24);
    private static final int LARGE_WRITE_LENGTH = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 1.8);
    private static final int WRITE_COUNT = APPENDS_PER_SEGMENT * 2;

    /**
     * Tests the ability to roll over Segments.
     */
    @Test
    public void testRolling() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = s.openWrite(SEGMENT_NAME);
        val readHandle = s.openRead(SEGMENT_NAME); // Open now, before writing, so we force a refresh.
        val writeStream = new ByteArrayOutputStream();
        populate(s, writeHandle, writeStream);

        // Check that no file has exceeded its maximum length.
        byte[] writtenData = writeStream.toByteArray();
        Assert.assertEquals("Unexpected segment length.", writtenData.length, s.getStreamSegmentInfo(SEGMENT_NAME).getLength());
        int checkedLength = 0;
        while (checkedLength < writtenData.length) {
            String subSegmentName = StreamSegmentNameUtils.getSubSegmentName(SEGMENT_NAME, checkedLength);
            Assert.assertTrue("Inexistent SubSegment: " + subSegmentName, baseStorage.exists(subSegmentName));
            val subSegmentInfo = baseStorage.getStreamSegmentInfo(subSegmentName);
            int expectedLength = (int) Math.min(DEFAULT_ROLLING_POLICY.getMaxLength(), writtenData.length - checkedLength);
            Assert.assertEquals("Unexpected SubSegment length for: " + subSegmentName, expectedLength, subSegmentInfo.getLength());
            checkedLength += expectedLength;

            if (checkedLength < writtenData.length) {
                Assert.assertTrue("Expected SubSegment to be sealed: " + subSegmentName, subSegmentInfo.isSealed());
            }
        }

        checkWrittenData(writtenData, readHandle, s);
    }

    /**
     * Tests the ability to truncate Segments.
     */
    @Test
    public void testTruncate() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val readHandle = s.openRead(SEGMENT_NAME); // Open now, before writing, so we force a refresh.
        val writeStream = new ByteArrayOutputStream();
        populate(s, writeHandle, writeStream);
        byte[] writtenData = writeStream.toByteArray();

        for (int truncateOffset = 0; truncateOffset <= writtenData.length; truncateOffset += DEFAULT_ROLLING_POLICY.getMaxLength() / 2) {
            s.truncate(writeHandle, truncateOffset);

            // Verify we can still read properly.
            checkWrittenData(writtenData, truncateOffset, readHandle, s);

            // Verify each SubSegment's existence.
            for (SubSegment subSegment : writeHandle.subSegments()) {
                boolean expectedExists = subSegment.getLastOffset() > truncateOffset
                        || (subSegment.getStartOffset() == subSegment.getLastOffset() && subSegment.getLastOffset() == truncateOffset);
                Assert.assertEquals("Unexpected SubSegment truncation status for " + subSegment + ", truncation offset = " + truncateOffset,
                        expectedExists, subSegment.exists());
                boolean existsInStorage = baseStorage.exists(subSegment.getName());
                Assert.assertEquals("Expected SubSegment deletion status for " + subSegment + ", truncation offset = " + truncateOffset,
                        expectedExists, existsInStorage);
                if (!expectedExists) {
                    AssertExtensions.assertThrows(
                            "Not expecting a read from a truncated SubSegment to work.",
                            () -> s.read(readHandle, subSegment.getLastOffset() - 1, new byte[1], 0, 1),
                            ex -> ex instanceof StreamSegmentTruncatedException);
                }
            }
        }

        // Do some more writes and verify they are added properly.
        int startOffset = writtenData.length;
        populate(s, writeHandle, writeStream);
        writtenData = writeStream.toByteArray();
        checkWrittenData(writtenData, startOffset, readHandle, s);

        // Verify we cannot concat a truncated segment into another.
        final String targetSegmentName = "TargetSegment";
        s.create(targetSegmentName);
        val targetSegmentHandle = s.openWrite(targetSegmentName);
        s.seal(writeHandle);
        AssertExtensions.assertThrows(
                "concat() allowed using a truncated segment as a source.",
                () -> s.concat(targetSegmentHandle, 0, SEGMENT_NAME),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the case when Delete worked partially (only some SubSegments were deleted, or all SubSegments were deleted
     * but the Header still exists).
     */
    @Test
    public void testDeleteFailure() throws Exception {
        final int failAtIndex = 1;
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        populate(s, writeHandle, null);

        // Simulate a deletion failure that is not a StreamSegmentNotExistsException.
        String failOnDelete = writeHandle.subSegments().get(failAtIndex).getName();
        baseStorage.deleteFailure = sn -> sn.equals(failOnDelete) ? new IntentionalException() : null;
        AssertExtensions.assertThrows(
                "delete() did not propagate proper exception on failure.",
                () -> s.delete(writeHandle),
                ex -> ex instanceof IntentionalException);

        Assert.assertTrue("Not expecting segment to be deleted yet.", s.exists(SEGMENT_NAME));
        Assert.assertFalse("Expected first SubSegment to be marked as deleted.", writeHandle.subSegments().get(failAtIndex - 1).exists());
        Assert.assertTrue("Expected failed-to-delete SubSegment to not be marked as deleted.", writeHandle.subSegments().get(failAtIndex).exists());
        Assert.assertTrue("Expected subsequent SubSegment to not be marked as deleted.", writeHandle.subSegments().get(failAtIndex + 1).exists());

        // Clear the intentional failure, but do delete the SubSegment, to verify it properly handles missing SubSegments.
        baseStorage.deleteFailure = null;
        baseStorage.delete(baseStorage.openRead(failOnDelete));
        s.delete(writeHandle);
        Assert.assertFalse("Expecting the segment to be deleted.", s.exists(SEGMENT_NAME));
        Assert.assertTrue("Expected the handle to be marked as deleted.", writeHandle.isDeleted());
        Assert.assertFalse("Expected all SubSegments to be marked as deleted.", writeHandle.subSegments().stream().anyMatch(SubSegment::exists));
    }

    /**
     * Tests the ability to use native concat for those cases when it's appropriate.
     */
    @Test
    public void testConcatNatively() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int initialSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a target Segment and write a little data to it.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);

        // Create a source Segment and write a little data to it, making sure it is small enough to fit into the target
        // when we need to concat.
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, initialSourceLength, initialSourceLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1, initialTargetLength + initialSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to use native concat for those cases when it's appropriate.
     */
    @Test
    public void testConcatNativelyFailure() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int initialSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength;
        final String sourceSegmentName = "SourceSegment";

        // Concat succeeds, but can't delete header.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a target and a source, making sure they have the right sizes for a native concat.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, initialSourceLength, initialSourceLength, writeStream);
        s.seal(sourceHandle);

        // Attempt to concat, but intentionally fail the deletion of the source header.
        baseStorage.deleteFailure = sn -> sn.equals(sourceHandle.getHeaderHandle().getSegmentName()) ? new IntentionalException() : null;
        AssertExtensions.assertThrows(
                "Unexpected exception when doing native concat.",
                () -> s.concat(targetHandle, initialTargetLength, sourceSegmentName),
                ex -> ex instanceof IntentionalException);

        // However, the concat should have worked, so the source segment is now inaccessible.
        baseStorage.deleteFailure = null;
        checkConcatResult(s, targetHandle, sourceSegmentName, 1, initialTargetLength + initialSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to concat using the header file for those cases when native concat cannot be used because the
     * source Segment has a single SubSegment, but it's too large to fit into the Target's active SubSegment.
     */
    @Test
    public void testConcatHeaderSingleFile() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int bigSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength + 1;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, bigSourceLength, bigSourceLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 2, initialTargetLength + bigSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to concat using the header file for those cases when native concat cannot be used because the
     * source Segment has multiple SubSegments.
     */
    @Test
    public void testConcatHeaderMultiFile() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, APPENDS_PER_SEGMENT, initialTargetLength, initialTargetLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1 + sourceHandle.subSegments().size(), initialTargetLength + (int) sourceHandle.length());
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to handle partially executed concat operations for header concat, such as being able to write
     * the concat entry but not actually concat the source header file.
     */
    @Test
    public void testConcatHeaderFailure() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, APPENDS_PER_SEGMENT, initialTargetLength, initialTargetLength, writeStream);
        s.seal(sourceHandle);

        // Simulate a native concat exception, and try a few times.
        baseStorage.concatFailure = sn -> sn.equals(sourceHandle.getHeaderHandle().getSegmentName()) ? new IntentionalException() : null;
        for (int i = 0; i < 4; i++) {
            AssertExtensions.assertThrows(
                    "Unexpected error reported from concat.",
                    () -> s.concat(targetHandle, initialTargetLength, sourceSegmentName),
                    ex -> ex instanceof IntentionalException);
        }

        // Clear the intentional failure and try again, after which check the results.
        baseStorage.concatFailure = null;
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1 + sourceHandle.subSegments().size(), initialTargetLength + (int) sourceHandle.length());
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    @Override
    public void testFencing() throws Exception {
        // Fencing is left up to the underlying Storage implementation to handle. There's nothing to test here.
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new RollingStorage(new InMemoryStorage(), DEFAULT_ROLLING_POLICY), executorService());
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        val headerName = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        val headerHandle = InMemoryStorage.newHandle(headerName, readOnly);
        return new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());
    }

    private void populate(RollingStorage s, SegmentHandle writeHandle, ByteArrayOutputStream writeStream) throws Exception {
        populate(s, writeHandle, WRITE_COUNT, SMALL_WRITE_LENGTH, LARGE_WRITE_LENGTH, writeStream);
    }

    private void populate(RollingStorage s, SegmentHandle writeHandle, int writeCount, int smallWriteSize, int largeWriteSize, ByteArrayOutputStream writeStream) throws Exception {
        final Random rnd = new Random(0);
        int offset = (int) s.getStreamSegmentInfo(writeHandle.getSegmentName()).getLength();
        for (int i = 0; i < writeCount; i++) {
            byte[] appendData = new byte[i % 2 == 0 ? smallWriteSize : largeWriteSize];
            rnd.nextBytes(appendData);
            s.write(writeHandle, offset, new ByteArrayInputStream(appendData), appendData.length);
            offset += appendData.length;
            if (writeStream != null) {
                writeStream.write(appendData);
            }
        }
    }

    private void checkConcatResult(RollingStorage s, RollingSegmentHandle targetHandle, String sourceSegmentName, int expectedSubSegmentCount, int expectedLength) throws Exception {
        Assert.assertFalse("Expecting the source segment to not exist anymore.", s.exists(sourceSegmentName));
        Assert.assertEquals("Unexpected number of SubSegments in target.", expectedSubSegmentCount, targetHandle.subSegments().size());
        Assert.assertEquals("Unexpected target length.", expectedLength, targetHandle.length());

        // Reload the handle and verify nothing strange happened in Storage.
        val targetHandle2 = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        Assert.assertEquals("Unexpected number of SubSegments in reloaded target handle.", expectedSubSegmentCount, targetHandle2.subSegments().size());
        Assert.assertEquals("Unexpected reloaded target length.", targetHandle.length(), targetHandle2.length());
    }

    private void checkWrittenData(byte[] writtenData, SegmentHandle readHandle, RollingStorage s) throws StreamSegmentException {
        checkWrittenData(writtenData, 0, readHandle, s);
    }

    private void checkWrittenData(byte[] writtenData, int offset, SegmentHandle readHandle, RollingStorage s) throws StreamSegmentException {
        byte[] readBuffer = new byte[writtenData.length - offset];
        int bytesRead = s.read(readHandle, offset, readBuffer, 0, readBuffer.length);
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        AssertExtensions.assertArrayEquals("Unexpected data read back.", writtenData, offset, readBuffer, 0, readBuffer.length);
    }

    //region TestStorage

    private static class TestStorage extends InMemoryStorage {
        private Function<String, IntentionalException> deleteFailure;
        private Function<String, IntentionalException> concatFailure;

        @Override
        public void delete(SegmentHandle handle) throws StreamSegmentNotExistsException {
            maybeThrow(handle.getSegmentName(), this.deleteFailure);
            super.delete(handle);
        }

        @Override
        public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
            maybeThrow(sourceSegment, this.concatFailure);
            super.concat(targetHandle, offset, sourceSegment);
        }

        private void maybeThrow(String segmentName, Function<String, IntentionalException> exceptionFunction) {
            IntentionalException toThrow;
            if (exceptionFunction != null && (toThrow = exceptionFunction.apply(segmentName)) != null) {
                throw toThrow;
            }
        }
    }

    //endregion
}
