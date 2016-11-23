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

package com.emc.pravega.service.storage.impl.hdfstest;

import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorage;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;

/**
 * This file contains unit tests for InMemoryStorage.
 */
public class HDFSStorageTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SEGMENT_COUNT = 10;
    private static final int APPENDS_PER_SEGMENT = 10;

    /**
     * Tests the write() method.
     *
     * @throws Exception asserts and exceptions thrown by the test run.
     */
    @Ignore
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            // Check pre-create write.
            AssertExtensions.assertThrows("write() did not throw for non-existent StreamSegment.",
                    s.write(segmentName, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            s.create(segmentName, TIMEOUT).join();

            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(segmentName, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            AssertExtensions.assertThrows("write() did not throw bad offset write (smaller).",
                    s.write(segmentName, offset - 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            AssertExtensions.assertThrows("write() did not throw bad offset write (larger).",
                    s.write(segmentName, offset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(segmentName, TIMEOUT).join();
            AssertExtensions.assertThrows("write() did not throw for a deleted StreamSegment.",
                    s.write(segmentName, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the read() method.
     *
     * @throws Exception asserts and exceptions thrown by the test run.
     */
    @Ignore
    @Test
    public void testRead() throws Exception {
        try (Storage s = createStorage()) {
            // Check pre-create read.
            AssertExtensions.assertThrows("read() did not throw for non-existent StreamSegment.",
                    s.read("foo", 0, new byte[1], 0, 1, TIMEOUT), ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s);

            // Do some reading.
            for (String segmentName : appendData.keySet()) {
                byte[] expectedData = appendData.get(segmentName).toByteArray();

                for (int offset = 0; offset < expectedData.length / 2; offset++) {
                    int length = expectedData.length - 2 * offset;
                    byte[] readBuffer = new byte[length];
                    int bytesRead = s.read(segmentName, offset, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                    Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                            length, bytesRead);
                    AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                            expectedData, offset, readBuffer, 0, bytesRead);
                }
            }

            // Test bad parameters.
            String testSegmentName = getSegmentName(0);
            byte[] testReadBuffer = new byte[10];
            AssertExtensions.assertThrows("read() allowed reading with negative read offset.",
                    s.read(testSegmentName, -1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows("read() allowed reading with offset beyond Segment length.",
                    s.read(testSegmentName, s.getStreamSegmentInfo(testSegmentName, TIMEOUT).join().getLength() + 1,
                            testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows("read() allowed reading with negative read buffer offset.",
                    s.read(testSegmentName, 0, testReadBuffer, -1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows("read() allowed reading with invalid read buffer length.",
                    s.read(testSegmentName, 0, testReadBuffer, 1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows("read() allowed reading with invalid read length.",
                    s.read(testSegmentName, 0, testReadBuffer, 0, testReadBuffer.length + 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(testSegmentName, TIMEOUT).join();
            AssertExtensions.assertThrows("read() did not throw for a deleted StreamSegment.",
                    s.read(testSegmentName, 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the seal() method.
     *
     * @throws Exception asserts and exceptions thrown by the test run.
     */
    @Ignore
    @Test
    public void testSeal() throws Exception {
        try (Storage s = createStorage()) {
            // Check pre-create seal.
            AssertExtensions.assertThrows("seal() did not throw for non-existent StreamSegment.",
                    s.seal("foo", TIMEOUT), ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s);
            for (String segmentName : appendData.keySet()) {
                s.seal(segmentName, TIMEOUT).join();
                AssertExtensions.assertThrows("seal() did not throw for an already sealed StreamSegment.",
                        s.seal(segmentName, TIMEOUT), ex -> ex instanceof StreamSegmentSealedException);

                AssertExtensions.assertThrows("write() did not throw for a sealed StreamSegment.",
                        s.write(segmentName, s.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength(),
                                new ByteArrayInputStream("g".getBytes()), 1, TIMEOUT),
                        ex -> ex instanceof StreamSegmentSealedException);

                // Check post-delete seal.
                s.delete(segmentName, TIMEOUT).join();
                AssertExtensions.assertThrows("seal() did not throw for a deleted StreamSegment.",
                        s.seal(segmentName, TIMEOUT), ex -> ex instanceof StreamSegmentNotExistsException);
            }
        }
    }

    /**
     * Tests the concat() method.
     *
     * @throws Exception asserts and exceptions thrown by the test run.
     */
    @Ignore
    @Test
    public void testConcat() throws Exception {
        try (Storage s = createStorage()) {
            HashMap<String, ByteArrayOutputStream> appendData = populate(s);

            // Check pre-create concat.
            String firstSegmentName = getSegmentName(0);
            AssertExtensions.assertThrows("concat() did not throw for non-existent target StreamSegment.",
                    s.concat("foo1", 0, firstSegmentName, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            AssertExtensions.assertThrows("concat() did not throw for non-existent source StreamSegment.",
                    s.concat(firstSegmentName, 0, "foo2", TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ArrayList<String> concatOrder = new ArrayList<>();
            concatOrder.add(firstSegmentName);
            for (String segmentName : appendData.keySet()) {
                if (segmentName.equals(firstSegmentName)) {
                    // FirstSegment is where we'll be concatenating to.
                    continue;
                }

                AssertExtensions.assertThrows("Concat allowed when source segment is not sealed.",
                        () -> s.concat(firstSegmentName, 0, segmentName, TIMEOUT),
                        ex -> ex instanceof IllegalStateException);

                // Seal the source segment and then re-try the concat
                s.seal(segmentName, TIMEOUT).join();
                SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                SegmentProperties sourceProps = s.getStreamSegmentInfo(segmentName, TIMEOUT).join();

                s.concat(firstSegmentName, 0, segmentName, TIMEOUT).join();
                concatOrder.add(segmentName);
                SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
                AssertExtensions.assertThrows("concat() did not delete source segment",
                        s.getStreamSegmentInfo(segmentName, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                // Only check lengths here; we'll check the contents at the end.
                Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.",
                        preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
            }

            // Check the contents of the first StreamSegment. We already validated that the length is correct.
            SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
            byte[] readBuffer = new byte[(int) segmentProperties.getLength()];

            // Read the entire StreamSegment.
            int bytesRead = s.read(firstSegmentName, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
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

    private String getSegmentName(int id) {
        return Integer.toString(id);
    }

    private HashMap<String, ByteArrayOutputStream> populate(Storage s) throws Exception {
        HashMap<String, ByteArrayOutputStream> appendData = new HashMap<>();

        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            String segmentName = getSegmentName(segmentId);

            s.create(segmentName, TIMEOUT).join();
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

    private Storage createStorage() {
        Properties prop = new Properties();
        prop.setProperty("hdfs.fs.default.name", "localhost:9000");
        prop.setProperty("hdfs.hdfsroot", "");
        prop.setProperty("hdfs.pravegaid", "0");
        prop.setProperty("hdfs.replication", "1");
        prop.setProperty("hdfs.blocksize", "1048576");
        HDFSStorageConfig config = new HDFSStorageConfig(prop);
        return new HDFSStorage(config, ForkJoinPool.commonPool());
    }
}
