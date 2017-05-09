/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage;

import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentExistsException;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Base class for testing any implementation of the Storage interface.
 */
public abstract class StorageTestBase extends ThreadPooledTestSuite {
    //region General Test arguments

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);
    protected static final long DEFAULT_EPOCH = 1;
    protected static final int APPENDS_PER_SEGMENT = 10;
    private static final int SEGMENT_COUNT = 4;

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    //endregion

    //region Tests

    /**
     * Tests the create() method.
     */
    @Test
    public void testCreate() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, null).join();
            assertThrows("create() did not throw for existing StreamSegment.",
                    s.create(segmentName, null),
                    ex -> ex instanceof StreamSegmentExistsException);
        }
    }

    /**
     * Tests the openRead() and openWrite() methods.
     */
    @Test
    public void testOpen() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Segment does not exist.
            assertThrows("openWrite() did not throw for non-existent StreamSegment.",
                    s.openWrite(segmentName),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertThrows("openRead() did not throw for non-existent StreamSegment.",
                    s.openRead(segmentName),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the write() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s.create(segmentName, TIMEOUT).join();

            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createHandle(segmentName + "_1", false, DEFAULT_EPOCH), 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (smaller).",
                    () -> s.write(writeHandle, finalOffset - 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the read() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testRead() throws Exception {
        final String context = "Read";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Check invalid segment name.
            assertThrows("read() did not throw for invalid segment name.",
                    () -> s.read(createHandle("foo_read_1", true, DEFAULT_EPOCH), 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

            // Do some reading.
            for (Entry<String, ByteArrayOutputStream> entry : appendData.entrySet()) {
                String segmentName = entry.getKey();
                val readHandle = s.openRead(segmentName).join();
                byte[] expectedData = entry.getValue().toByteArray();

                for (int offset = 0; offset < expectedData.length / 2; offset++) {
                    int length = expectedData.length - 2 * offset;
                    byte[] readBuffer = new byte[length];
                    int bytesRead = s.read(readHandle, offset, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                    Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                            length, bytesRead);
                    AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                            expectedData, offset, readBuffer, 0, bytesRead);
                }
            }

            // Test bad parameters.
            val testSegment = getSegmentName(0, context);
            val testSegmentHandle = s.openRead(testSegment).join();
            byte[] testReadBuffer = new byte[10];
            assertThrows("read() allowed reading with negative read offset.",
                    () -> s.read(testSegmentHandle, -1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with offset beyond Segment length.",
                    () -> s.read(testSegmentHandle, s.getStreamSegmentInfo(testSegment, TIMEOUT).join().getLength() + 1,
                            testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with negative read buffer offset.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, -1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with invalid read buffer length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with invalid read length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 0, testReadBuffer.length + 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(s.openWrite(testSegment).join(), TIMEOUT).join();
            assertThrows("read() did not throw for a deleted StreamSegment.",
                    () -> s.read(testSegmentHandle, 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the seal() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testSeal() throws Exception {
        final String context = "Seal";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Check segment not exists.
            assertThrows("seal() did not throw for non-existent segment name.",
                    () -> s.seal(createHandle("foo", false, DEFAULT_EPOCH), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);
            int deleteCount = 0;
            for (String segmentName : appendData.keySet()) {
                val readHandle = s.openRead(segmentName).join();
                assertThrows("seal() did not throw for read-only handle.",
                        () -> s.seal(readHandle, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);

                val writeHandle = s.openWrite(segmentName).join();
                s.seal(writeHandle, TIMEOUT).join();

                //Seal is idempotent. Resealing an already sealed segment should work.
                s.seal(writeHandle, TIMEOUT).join();
                assertThrows("write() did not throw for a sealed StreamSegment.",
                        () -> s.write(writeHandle, s.getStreamSegmentInfo(segmentName, TIMEOUT).
                                join().getLength(), new ByteArrayInputStream("g".getBytes()), 1, TIMEOUT),
                        ex -> ex instanceof StreamSegmentSealedException);

                // Check post-delete seal. Half of the segments use the existing handle, and half will re-acquire it.
                // We want to reacquire it because OpenWrite will return a read-only handle for sealed segments.
                boolean reacquireHandle = (deleteCount++) % 2 == 0;
                SegmentHandle deleteHandle = writeHandle;
                if (reacquireHandle) {
                    deleteHandle = s.openWrite(segmentName).join();
                }

                s.delete(deleteHandle, TIMEOUT).join();
                assertThrows("seal() did not throw for a deleted StreamSegment.",
                        () -> s.seal(writeHandle, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            }
        }
    }

    /**
     * Tests the concat() method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testConcat() throws Exception {
        final String context = "Concat";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

            // Check invalid segment name.
            val firstSegmentName = getSegmentName(0, context);
            val firstSegmentHandle = s.openWrite(firstSegmentName).join();
            AtomicLong firstSegmentLength = new AtomicLong(s.getStreamSegmentInfo(firstSegmentName,
                    TIMEOUT).join().getLength());
            assertThrows("concat() did not throw for non-existent target segment name.",
                    () -> s.concat(createHandle("foo1", false, DEFAULT_EPOCH), 0, firstSegmentName, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertThrows("concat() did not throw for invalid source StreamSegment name.",
                    () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), "foo2", TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ArrayList<String> concatOrder = new ArrayList<>();
            concatOrder.add(firstSegmentName);
            for (String sourceSegment : appendData.keySet()) {
                if (sourceSegment.equals(firstSegmentName)) {
                    // FirstSegment is where we'll be concatenating to.
                    continue;
                }

                assertThrows("Concat allowed when source segment is not sealed.",
                        () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT),
                        ex -> ex instanceof IllegalStateException);

                // Seal the source segment and then re-try the concat
                val sourceWriteHandle = s.openWrite(sourceSegment).join();
                s.seal(sourceWriteHandle, TIMEOUT).join();
                SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                SegmentProperties sourceProps = s.getStreamSegmentInfo(sourceSegment, TIMEOUT).join();

                s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT).join();
                concatOrder.add(sourceSegment);
                SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                Assert.assertFalse("concat() did not delete source segment", s.exists(sourceSegment, TIMEOUT).join());

                // Only check lengths here; we'll check the contents at the end.
                Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.",
                        preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
                firstSegmentLength.set(postConcatTargetProps.getLength());
            }

            // Check the contents of the first StreamSegment. We already validated that the length is correct.
            SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
            byte[] readBuffer = new byte[(int) segmentProperties.getLength()];

            // Read the entire StreamSegment.
            int bytesRead = s.read(firstSegmentHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);

            // Check, concat-by-concat, that the final data is correct.
            int offset = 0;
            for (String segmentName : concatOrder) {
                byte[] concatData = appendData.get(segmentName).toByteArray();
                AssertExtensions.assertArrayEquals("Unexpected concat data.", concatData, 0, readBuffer, offset,
                        concatData.length);
                offset += concatData.length;
            }

            Assert.assertEquals("Concat included more bytes than expected.", offset, readBuffer.length);
        }
    }

    /**
     * Verifies that the Storage implementation enforces fencing: if a segment owner changes, no operation is allowed
     * on a segment until open() is called on it.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public abstract void testFencing() throws Exception;

    private String getSegmentName(int id, String context) {
        return String.format("%s_%s", context, id);
    }

    private HashMap<String, ByteArrayOutputStream> populate(Storage s, String context) throws Exception {
        HashMap<String, ByteArrayOutputStream> appendData = new HashMap<>();

        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            String segmentName = getSegmentName(segmentId, context);

            s.create(segmentName, TIMEOUT).join();
            val writeHandle = s.openWrite(segmentName).join();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            appendData.put(segmentName, writeStream);

            long offset = 0;
            for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                writeStream.write(writeData);
                offset += writeData.length;
            }
        }
        return appendData;
    }

    //endregion

    //region Abstract methods

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    protected abstract Storage createStorage();

    /**
     * Creates a new handle for the given Segment, regardless of whether the Segment exists or not.
     *
     * @param segmentName The name of the segment.
     * @param readOnly    Whether this is a read-only handle or not.
     * @param epoch       Epoch.
     * @return The handle.
     */
    protected abstract SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch);

    //endregion
}