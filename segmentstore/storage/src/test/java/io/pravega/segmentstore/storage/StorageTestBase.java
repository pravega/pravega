/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.common.MathHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertSuppliedFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Base class for testing any implementation of the Storage interface.
 */
public abstract class StorageTestBase extends ThreadPooledTestSuite {
    //region General Test arguments

    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    protected static final long DEFAULT_EPOCH = 1;
    protected static final int APPENDS_PER_SEGMENT = 10;
    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
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
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());
            assertThrows("create() did not throw for existing StreamSegment.",
                    () -> createSegment(segmentName, s),
                    ex -> ex instanceof StreamSegmentExistsException);

            // Delete and make sure it can be recreated.
            s.openWrite(segmentName).thenCompose(handle -> s.delete(handle, null)).join();
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());
        }
    }

    /**
     * Tests the delete() method.
     */
    @Test
    public void testDelete() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            //Delete the segment.
            s.openWrite(segmentName).thenCompose(handle -> s.delete(handle, null)).join();
            Assert.assertFalse("Expected the segment to not exist.", s.exists(segmentName, null).join());
            assertThrows("getStreamSegmentInfo() did not throw for deleted StreamSegment.",
                    () -> s.getStreamSegmentInfo(segmentName, null).join(),
                    ex -> ex instanceof StreamSegmentNotExistsException);
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
            assertFutureThrows("openWrite() did not throw for non-existent StreamSegment.",
                    s.openWrite(segmentName),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertFutureThrows("openRead() did not throw for non-existent StreamSegment.",
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
        int appendCount = 10;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertSuppliedFutureThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertSuppliedFutureThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createInexistentSegmentHandle(s, false), 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format(APPEND_FORMAT, segmentName, j).getBytes();

                // We intentionally add some garbage at the end of the dataStream to verify that write() takes into account
                // the value of the "length" argument.
                val dataStream = new SequenceInputStream(new ByteArrayInputStream(writeData), new ByteArrayInputStream(new byte[100]));
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertSuppliedFutureThrows("write() did not throw bad offset write (smaller).",
                    () -> s.write(writeHandle, finalOffset - 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            assertSuppliedFutureThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertSuppliedFutureThrows("write() did not throw for a deleted StreamSegment.",
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
            assertSuppliedFutureThrows("read() did not throw for invalid segment name.",
                    () -> s.read(createInexistentSegmentHandle(s, true), 0, new byte[1], 0, 1, TIMEOUT),
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
            assertSuppliedFutureThrows("read() allowed reading with negative read offset.",
                    () -> s.read(testSegmentHandle, -1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with offset beyond Segment length.",
                    () -> s.read(testSegmentHandle, s.getStreamSegmentInfo(testSegment, TIMEOUT).join().getLength() + 1,
                            testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with negative read buffer offset.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, -1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with invalid read buffer length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with invalid read length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 0, testReadBuffer.length + 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(s.openWrite(testSegment).join(), TIMEOUT).join();
            assertSuppliedFutureThrows("read() did not throw for a deleted StreamSegment.",
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
            assertSuppliedFutureThrows("seal() did not throw for non-existent segment name.",
                    () -> s.seal(createInexistentSegmentHandle(s, false), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);
            int deleteCount = 0;
            for (String segmentName : appendData.keySet()) {
                val readHandle = s.openRead(segmentName).join();
                assertSuppliedFutureThrows("seal() did not throw for read-only handle.",
                        () -> s.seal(readHandle, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);

                val writeHandle = s.openWrite(segmentName).join();
                s.seal(writeHandle, TIMEOUT).join();

                //Seal is idempotent. Resealing an already sealed segment should work.
                s.seal(writeHandle, TIMEOUT).join();
                assertSuppliedFutureThrows("write() did not throw for a sealed StreamSegment.",
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
                assertSuppliedFutureThrows("seal() did not throw for a deleted StreamSegment.",
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
            val sealedSegmentName = "SealedSegment";
            createSegment(sealedSegmentName, s);
            val sealedSegmentHandle = s.openWrite(sealedSegmentName).join();
            s.write(sealedSegmentHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
            s.seal(sealedSegmentHandle, TIMEOUT).join();
            AtomicLong firstSegmentLength = new AtomicLong(s.getStreamSegmentInfo(firstSegmentName,
                    TIMEOUT).join().getLength());
            assertSuppliedFutureThrows("concat() did not throw for non-existent target segment name.",
                    () -> s.concat(createInexistentSegmentHandle(s, false), 0, sealedSegmentName, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertSuppliedFutureThrows("concat() did not throw for invalid source StreamSegment name.",
                    () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), "foo2", TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ArrayList<String> concatOrder = new ArrayList<>();
            concatOrder.add(firstSegmentName);
            for (String sourceSegment : appendData.keySet()) {
                if (sourceSegment.equals(firstSegmentName)) {
                    // FirstSegment is where we'll be concatenating to.
                    continue;
                }

                assertSuppliedFutureThrows("Concat allowed when source segment is not sealed.",
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
        byte[] extraData = new byte[1024];
        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            String segmentName = getSegmentName(segmentId, context);
            createSegment(segmentName, s);
            val writeHandle = s.openWrite(segmentName).join();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            appendData.put(segmentName, writeStream);

            long offset = 0;
            for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
                byte[] writeData = String.format(APPEND_FORMAT, segmentName, j).getBytes();

                // Append some garbage at the end to make sure we only write as much as instructed, and not the whole InputStream.
                val dataStream = new SequenceInputStream(new ByteArrayInputStream(writeData), new ByteArrayInputStream(extraData));
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                writeStream.write(writeData);
                offset += writeData.length;
            }
        }
        return appendData;
    }

    //endregion

    //region Protected utility methods containing common code.

    protected void verifyReadOnlyOperationsSucceed(SegmentHandle handle, Storage storage) {
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    protected void verifyWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat";
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        storage.concat(handle, si.getLength() + data.length, concatHandle.getSegmentName(), TIMEOUT).join();
    }

    protected void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = "hello".getBytes();
        AssertExtensions.assertSuppliedFutureThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat";
        createSegment(concatName, storage);
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength(), concatHandle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    protected void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    protected void verifyFinalWriteOperationsFail(SegmentHandle handle, Storage storage) {
        AssertExtensions.assertSuppliedFutureThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertSuppliedFutureThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    protected SegmentHandle createInexistentSegmentHandle(Storage s, boolean readOnly) {
        Random rnd = RandomFactory.create();
        String segmentName = "Inexistent_" + MathHelpers.abs(rnd.nextInt());
        createSegment(segmentName, s);
        return (readOnly ? s.openRead(segmentName) : s.openWrite(segmentName))
                .thenCompose(handle -> s.openWrite(segmentName)
                        .thenCompose(h2 -> s.delete(h2, TIMEOUT))
                        .thenApply(v -> handle))
                .join();
    }

    protected void createSegment(String name, Storage s) {
        s.create(name, null).join();
    }

    //endregion

    //region Abstract methods

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    protected abstract Storage createStorage();

    //endregion
}