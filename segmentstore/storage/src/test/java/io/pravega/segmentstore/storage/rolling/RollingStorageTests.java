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

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RollingStorage class.
 */
public class RollingStorageTests extends StorageTestBase {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);

    /**
     * Tests the ability to roll over Segments.
     */
    @Test
    public void testRolling() throws Exception {
        final String segmentName = "RollingSegment";
        final int smallWriteLength = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 0.24);
        final int largeWriteLength = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 1.8);
        final int appendCount = APPENDS_PER_SEGMENT * 2;
        final Random rnd = new Random(0);

        // Write small and large writes, alternatively.
        @Cleanup
        val s = createRollingStorage();
        s.initialize(1);
        s.create(segmentName);
        val writeHandle = s.openWrite(segmentName);
        val readHandle = s.openRead(segmentName); // Open now, before writing, so we force a refresh.
        int offset = 0;
        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
        for (int i = 0; i < appendCount; i++) {
            byte[] appendData = new byte[i % 2 == 0 ? smallWriteLength : largeWriteLength];
            rnd.nextBytes(appendData);
            s.write(writeHandle, offset, new ByteArrayInputStream(appendData), appendData.length);
            offset += appendData.length;
            writeStream.write(appendData);
        }

        // Check that no file has exceeded its maximum length.
        byte[] writtenData = writeStream.toByteArray();
        Assert.assertEquals("Unexpected segment length.", writtenData.length, s.getStreamSegmentInfo(segmentName).getLength());
        val baseStorage = s.getBaseStorage();
        int checkedLength = 0;
        while (checkedLength < writtenData.length) {
            String subSegmentName = StreamSegmentNameUtils.getSubSegmentName(segmentName, checkedLength);
            Assert.assertTrue("Inexistent SubSegment: " + subSegmentName, baseStorage.exists(subSegmentName));
            val subSegmentInfo = baseStorage.getStreamSegmentInfo(subSegmentName);
            int expectedLength = (int) Math.min(DEFAULT_ROLLING_POLICY.getMaxLength(), writtenData.length - checkedLength);
            Assert.assertEquals("Unexpected SubSegment length for: " + subSegmentName, expectedLength, subSegmentInfo.getLength());
            checkedLength += expectedLength;

            if (checkedLength < writtenData.length) {
                Assert.assertTrue("Expected SubSegment to be sealed: " + subSegmentName, subSegmentInfo.isSealed());
            }
        }

        // Check reads
        byte[] readBuffer = new byte[writtenData.length];
        int bytesRead = s.read(readHandle, 0, readBuffer, 0, readBuffer.length);
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        Assert.assertArrayEquals("Unexpected data read back.", writtenData, readBuffer);
    }

    /**
     * Tests the case when Delete worked partially (only some SubSegments were deleted, or all SubSegments were deleted
     * but the Header still exists).
     */
    @Test
    public void testDeleteFailure() {

    }

    /**
     * Tests the ability to use native concat for those cases when it's appropriate.
     */
    @Test
    public void testConcatNatively() {

    }

    /**
     * Tests the ability to concat using the header file for those cases when native concat cannot be used.
     */
    @Test
    public void testConcatHeader() {

    }

    /**
     * Tests the ability to handle partially executed concat operations (for header concat only, which is a two-step process).
     */
    @Test
    public void testConcatFailure() {

    }

    /**
     * Tests the ability to truncate Segments.
     */
    @Test
    public void testTruncate() {
        // Check writes
        // Check reads (even with separate handles)
        // Check concats (that they don't work if source is truncated)
        // Check interrupted truncates.
    }

    @Override
    public void testFencing() throws Exception {
        // Fencing is left up to the underlying Storage implementation to handle. There's nothing to test here.
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new RollingStorage(new InMemoryStorage(), DEFAULT_ROLLING_POLICY), executorService());
    }

    private RollingStorage createRollingStorage() {
        return new RollingStorage(new InMemoryStorage(), DEFAULT_ROLLING_POLICY);
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        val headerName = StreamSegmentNameUtils.getHeaderSegmentName(segmentName);
        val headerHandle = InMemoryStorage.newHandle(headerName, readOnly);
        return new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());
    }
}
