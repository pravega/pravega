/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.rolling;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.SyncStorage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for testing any Storage implementation that has a layer of RollingStorage.
 */
public abstract class RollingStorageTestBase extends StorageTestBase {
    protected static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);

    @Override
    public void testFencing() throws Exception {
        // Fencing is left up to the underlying Storage implementation to handle. There's nothing to test here.
    }

    /**
     * Tests a scenario that would concatenate various segments successively into an initially empty segment while not
     * producing an excessive number of chunks. The initial concat will use header merge since the segment has no chunks,
     * but successive concats should unseal that last chunk and concat to it using the native method.
     * <p>
     * NOTE: this could be moved down into RollingStorageTests.java, however it being here ensures that unseal() is being
     * exercised in all classes that derive from this, which is all of the Storage implementations.
     *
     * @throws Exception If one occurred.
     */
    @Test
    public void testSuccessiveConcats() throws Exception {
        final String segmentName = "Segment";
        final int writeLength = 21;
        final int concatCount = 10;

        @Cleanup
        val s = createStorage();
        s.initialize(1);

        // Create Target Segment with infinite rolling. Do not write anything to it yet.
        val writeHandle = s.create(segmentName, SegmentRollingPolicy.NO_ROLLING, TIMEOUT)
                .thenCompose(v -> s.openWrite(segmentName)).join();

        final Random rnd = new Random(0);
        byte[] writeBuffer = new byte[writeLength];
        val writeStream = new ByteArrayOutputStream();
        for (int i = 0; i < concatCount; i++) {
            // Create a source segment, write a little bit to it, then seal & merge it.
            String sourceSegment = segmentName + "_Source_" + i;
            val sourceHandle = s.create(sourceSegment, TIMEOUT).thenCompose(v -> s.openWrite(sourceSegment)).join();
            rnd.nextBytes(writeBuffer);
            s.write(sourceHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
            s.seal(sourceHandle, TIMEOUT).join();
            s.concat(writeHandle, writeStream.size(), sourceSegment, TIMEOUT).join();
            writeStream.write(writeBuffer);
        }

        // Write directly to the target segment - this ensures that writes themselves won't create a new chunk if the
        // write can still fit into the last chunk.
        rnd.nextBytes(writeBuffer);
        s.write(writeHandle, writeStream.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        writeStream.write(writeBuffer);

        // Get a read handle, which will also fetch the number of chunks for us.
        val readHandle = (RollingSegmentHandle) s.openRead(segmentName).join();
        Assert.assertEquals("Unexpected number of chunks created.", 1, readHandle.chunks().size());
        val writtenData = writeStream.toByteArray();
        byte[] readBuffer = new byte[writtenData.length];
        int bytesRead = s.read(readHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        Assert.assertArrayEquals("Unexpected data read back.", writtenData, readBuffer);
    }

    @Test
    public void testWriteAfterHeaderMerge() throws Exception {
        final String segmentName = "Segment";
        final int writeLength = 21;

        @Cleanup
        val s = createStorage();
        s.initialize(1);

        // Create Target Segment with infinite rolling. Do not write anything to it yet.
        val writeHandle = s.create(segmentName, SegmentRollingPolicy.NO_ROLLING, TIMEOUT)
                .thenCompose(v -> s.openWrite(segmentName)).join();

        final Random rnd = new Random(0);
        byte[] writeBuffer = new byte[writeLength];
        val writeStream = new ByteArrayOutputStream();

        // Create a source segment, write a little bit to it, then seal & merge it.
        String sourceSegment = segmentName + "_Source";
        val sourceHandle = s.create(sourceSegment, TIMEOUT).thenCompose(v -> s.openWrite(sourceSegment)).join();
        rnd.nextBytes(writeBuffer);
        s.write(sourceHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        s.seal(sourceHandle, TIMEOUT).join();
        s.concat(writeHandle, writeStream.size(), sourceSegment, TIMEOUT).join();
        writeStream.write(writeBuffer);

        // Write directly to the target segment.
        rnd.nextBytes(writeBuffer);
        s.write(writeHandle, writeStream.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        writeStream.write(writeBuffer);

        // Get a read handle, which will also fetch the number of chunks for us.
        val readHandle = (RollingSegmentHandle) s.openRead(segmentName).join();
        Assert.assertEquals("Unexpected number of chunks created.", 1, readHandle.chunks().size());
    }

    @Override
    protected void createSegment(String segmentName, Storage storage) {
        storage.create(segmentName, new SegmentRollingPolicy(getSegmentRollingSize()), null).join();
    }

    protected Storage wrap(SyncStorage storage) {
        return new AsyncStorageWrapper(new RollingStorage(storage, new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());
    }

    protected long getSegmentRollingSize() {
        return DEFAULT_ROLLING_SIZE;
    }
}
