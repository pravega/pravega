/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.emc.pravega.testcommon.AssertExtensions.assertThrows;

/**
 * Base class for testing any implementation of the Storage interface.
 */
public abstract class StorageTestBase {
    //region General Test arguments

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SEGMENT_COUNT = 4;
    private static final int APPENDS_PER_SEGMENT = 10;

    //endregion

    //region Tests

    /**
     * Tests the open() method.
     */
    @Test
    public void testOpen() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            // Segment does not exist.
            assertThrows("open() did not throw for non-existent StreamSegment.",
                    s.open(segmentName),
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
            val handle = s.create(segmentName, TIMEOUT).join();

            // Invalid name
            assertThrows(
                    "write() did not throw for invalid handle.",
                    () -> s.write(segmentName + "invalid", 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(segmentName, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertThrows("write() did not throw bad offset write (smaller).",
                    () -> s.write(segmentName, finalOffset - 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            assertThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(segmentName, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(segmentName, TIMEOUT).join();
            assertThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(segmentName, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
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
            // Check invalid handle.
            assertThrows("read() did not throw for invalid handle.",
                    () -> s.read("foo_read_1", 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

            // Do some reading.
            for (String segmentName : appendData.keySet()) {
                val handle = s.open(segmentName).join();
                byte[] expectedData = appendData.get(segmentName).toByteArray();

                for (int offset = 0; offset < expectedData.length / 2; offset++) {
                    int length = expectedData.length - 2 * offset;
                    byte[] readBuffer = new byte[length];
                    int bytesRead = s.read(segmentName, offset, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                    Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset), length, bytesRead);
                    AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset), expectedData, offset, readBuffer, 0, bytesRead);
                }
            }

            // Test bad parameters.
            val testSegment = getSegmentName(0, context);
            s.open(testSegment).join();
            byte[] testReadBuffer = new byte[10];
            assertThrows("read() allowed reading with negative read offset.",
                    () -> s.read(getSegmentName(0, context), -1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with offset beyond Segment length.",
                    () -> s.read(testSegment, s.getStreamSegmentInfo(testSegment, TIMEOUT).join().getLength() + 1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with negative read buffer offset.",
                    () -> s.read(testSegment, 0, testReadBuffer, -1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with invalid read buffer length.",
                    () -> s.read(testSegment, 0, testReadBuffer, 1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertThrows("read() allowed reading with invalid read length.",
                    () -> s.read(testSegment, 0, testReadBuffer, 0, testReadBuffer.length + 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(testSegment, TIMEOUT).join();
            assertThrows("read() did not throw for a deleted StreamSegment.",
                    () -> s.read(testSegment, 0, new byte[1], 0, 1, TIMEOUT),
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
            // Check invalid handle.
            assertThrows("seal() did not throw for invalid handle.",
                    () -> s.seal(createInvalidHandle("foo"), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);
            for (String segmentName : appendData.keySet()) {
                val handle = s.open(segmentName).join();
                val segmentInfo = s.seal(segmentName, TIMEOUT).join();
                Assert.assertTrue("seal() did not return a segmentInfo with isSealed == true", segmentInfo.isSealed());

                //Seal is reentrant. Resealing an already sealed segment should work
                val segmentInfo1 = s.seal(segmentName, TIMEOUT).join();
                Assert.assertTrue("seal() is reentrant returns with isSealed == true", segmentInfo1.isSealed());

                /*This works with actual HDFS but does not work with the simulation. Will uncomment it when
                // we fix mini hdfs
                assertThrows("write() did not throw for a sealed StreamSegment.",
                        () -> s.write(segmentName, s.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength(), new ByteArrayInputStream("g".getBytes()), 1, TIMEOUT),
                        ex -> ex instanceof StreamSegmentSealedException);
                 */

                // Check post-delete seal.
                s.delete(segmentName, TIMEOUT).join();
                assertThrows("seal() did not throw for a deleted StreamSegment.",
                        () -> s.seal(segmentName, TIMEOUT),
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
            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

            // Check invalid handle.
            val firstSegmentHandle = getSegmentName(0, context);
            s.open(firstSegmentHandle).join();
            AtomicLong firstSegmentLength = new AtomicLong(s.getStreamSegmentInfo(firstSegmentHandle, TIMEOUT).join().getLength());
            assertThrows("concat() did not throw invalid target StreamSegment handle.",
                    () -> s.concat(createInvalidHandle("foo1"), 0, firstSegmentHandle, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertThrows("concat() did not throw for invalid source StreamSegment handle.",
                    () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), createInvalidHandle("foo2"), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ArrayList<String> concatOrder = new ArrayList<>();
            concatOrder.add(firstSegmentHandle);
            for (String sourceSegment : appendData.keySet()) {
                if (sourceSegment.equals(firstSegmentHandle)) {
                    // FirstSegment is where we'll be concatenating to.
                    continue;
                }

                val sourceHandle = sourceSegment;
                s.open(sourceSegment).join();
                /* Sealing is a higher level construct. HDFS allows concat where the source can be written to as well.
                assertThrows("Concat allowed when source segment is not sealed.",
                        () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceHandle, TIMEOUT),
                        ex -> ex instanceof IllegalStateException);
                */
                // Seal the source segment and then re-try the concat
                s.seal(sourceHandle, TIMEOUT).join();
                SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentHandle, TIMEOUT).join();
                SegmentProperties sourceProps = s.getStreamSegmentInfo(sourceHandle, TIMEOUT).join();

                s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceHandle, TIMEOUT).join();
                concatOrder.add(sourceSegment);
                SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentHandle, TIMEOUT).join();
                Assert.assertFalse("concat() did not delete source segment", s.exists(sourceHandle, TIMEOUT).join());

                // Only check lengths here; we'll check the contents at the end.
                Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.", preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
                firstSegmentLength.set(postConcatTargetProps.getLength());
            }

            // Check the contents of the first StreamSegment. We already validated that the length is correct.
            SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentHandle, TIMEOUT).join();
            byte[] readBuffer = new byte[(int) segmentProperties.getLength()];

            // Read the entire StreamSegment.
            int bytesRead = s.read(firstSegmentHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);

            // Check, concat-by-concat, that the final data is correct.
            int offset = 0;
            for (String segmentName : concatOrder) {
                byte[] concatData = appendData.get(segmentName).toByteArray();
                AssertExtensions.assertArrayEquals("Unexpected concat data.", concatData, 0, readBuffer, offset, concatData.length);
                offset += concatData.length;
            }

            Assert.assertEquals("Concat included more bytes than expected.", offset, readBuffer.length);
        }
    }

    private String getSegmentName(int id, String context) {
        return String.format("%s_%s", context, id);
    }

    private HashMap<String, ByteArrayOutputStream> populate(Storage s, String context) throws Exception {
        HashMap<String, ByteArrayOutputStream> appendData = new HashMap<>();

        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            String segmentName = getSegmentName(segmentId, context);

            val handle = s.create(segmentName, TIMEOUT).join();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            appendData.put(segmentName, writeStream);

            long offset = 0;
            for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(segmentName, offset, dataStream, writeData.length, TIMEOUT).join();
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
     * Creates a SegmentHandle that is known to be bad invalid.
     *
     * @param segmentName The name of the segment to create a handle for.
     */
    protected abstract String createInvalidHandle(String segmentName);

    //endregion
}