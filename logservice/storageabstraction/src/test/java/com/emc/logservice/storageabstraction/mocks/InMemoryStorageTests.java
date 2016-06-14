package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.contracts.*;
import com.emc.logservice.storageabstraction.BadOffsetException;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Unit tests for InMemoryStorage
 */
public class InMemoryStorageTests {
    private static final Duration Timeout = Duration.ofSeconds(30);

    /**
     * Tests the write() method.
     */
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo";
        int appendCount = 100;

        try (Storage s = createStorage()) {
            // Check pre-create write.
            AssertExtensions.assertThrows(
                    "write() did not throw for non-existent StreamSegment.",
                    s.write(segmentName, 0, new ByteArrayInputStream(new byte[1]), 1, Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            s.create(segmentName, Timeout).join();

            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(segmentName, offset, dataStream, writeData.length, Timeout).join();
                offset += writeData.length;
            }

            // Check bad offset.
            AssertExtensions.assertThrows(
                    "write() did not throw bad offset write (smaller).",
                    s.write(segmentName, offset - 1, new ByteArrayInputStream("h".getBytes()), 1, Timeout),
                    ex -> ex instanceof BadOffsetException);

            AssertExtensions.assertThrows(
                    "write() did not throw bad offset write (larger).",
                    s.write(segmentName, offset + 1, new ByteArrayInputStream("h".getBytes()), 1, Timeout),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(segmentName, Timeout).join();
            AssertExtensions.assertThrows(
                    "write() did not throw for a deleted StreamSegment.",
                    s.write(segmentName, 0, new ByteArrayInputStream(new byte[1]), 1, Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the read() method.
     */
    @Test
    public void testRead() throws Exception {
        try (Storage s = createStorage()) {
            // Check pre-create read.
            AssertExtensions.assertThrows(
                    "read() did not throw for non-existent StreamSegment.",
                    s.read("foo", 0, new byte[1], 0, 1, Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, 10, 10);

            // Do some reading.
            for (String segmentName : appendData.keySet()) {
                byte[] expectedData = appendData.get(segmentName).toByteArray();

                for (int offset = 0; offset < expectedData.length / 2; offset++) {
                    int length = expectedData.length - 2 * offset;
                    byte[] readBuffer = new byte[length];
                    int bytesRead = s.read(segmentName, offset, readBuffer, 0, readBuffer.length, Timeout).join();
                    Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset), length, bytesRead);
                    AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset), expectedData, offset, readBuffer, 0, bytesRead);
                }
            }

            // Test bad parameters.
            String testSegmentName = getSegmentName(0);
            byte[] testReadBuffer = new byte[10];
            AssertExtensions.assertThrows(
                    "read() allowed reading with negative read offset.",
                    s.read(testSegmentName, -1, testReadBuffer, 0, testReadBuffer.length, Timeout),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows(
                    "read() allowed reading with offset beyond Segment length.",
                    s.read(testSegmentName, s.getStreamSegmentInfo(testSegmentName, Timeout).join().getLength() + 1, testReadBuffer, 0, testReadBuffer.length, Timeout),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows(
                    "read() allowed reading with negative read buffer offset.",
                    s.read(testSegmentName, 0, testReadBuffer, -1, testReadBuffer.length, Timeout),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows(
                    "read() allowed reading with invalid read buffer length.",
                    s.read(testSegmentName, 0, testReadBuffer, 1, testReadBuffer.length, Timeout),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            AssertExtensions.assertThrows(
                    "read() allowed reading with invalid read length.",
                    s.read(testSegmentName, 0, testReadBuffer, 0, testReadBuffer.length + 1, Timeout),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(testSegmentName, Timeout).join();
            AssertExtensions.assertThrows(
                    "read() did not throw for a deleted StreamSegment.",
                    s.read(testSegmentName, 0, new byte[1], 0, 1, Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the seal() method.
     */
    @Test
    public void testSeal() throws Exception {
        try (Storage s = createStorage()) {
            // Check pre-create seal.
            AssertExtensions.assertThrows(
                    "seal() did not throw for non-existent StreamSegment.",
                    s.seal("foo", Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, 10, 10);
            for (String segmentName : appendData.keySet()) {
                s.seal(segmentName, Timeout).join();
                AssertExtensions.assertThrows(
                        "seal() did not throw for an already sealed StreamSegment.",
                        s.seal(segmentName, Timeout),
                        ex -> ex instanceof StreamSegmentSealedException);

                AssertExtensions.assertThrows(
                        "write() did not throw for a sealed StreamSegment.",
                        s.write(segmentName, s.getStreamSegmentInfo(segmentName, Timeout).join().getLength(), new ByteArrayInputStream("g".getBytes()), 1, Timeout),
                        ex -> ex instanceof StreamSegmentSealedException);

                // Check post-delete seal.
                s.delete(segmentName, Timeout).join();
                AssertExtensions.assertThrows(
                        "seal() did not throw for a deleted StreamSegment.",
                        s.seal(segmentName, Timeout),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            }
        }
    }

    /**
     * Tests the concat() method.
     */
    @Test
    public void testConcat() throws Exception {
        try (Storage s = createStorage()) {
            HashMap<String, ByteArrayOutputStream> appendData = populate(s, 10, 10);

            // Check pre-create concat.
            String firstSegmentName = getSegmentName(0);
            AssertExtensions.assertThrows(
                    "concat() did not throw for non-existent target StreamSegment.",
                    s.concat("foo1", firstSegmentName, Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            AssertExtensions.assertThrows(
                    "concat() did not throw for non-existent source StreamSegment.",
                    s.concat(firstSegmentName, "foo2", Timeout),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ArrayList<String> concatOrder = new ArrayList<>();
            concatOrder.add(firstSegmentName);
            for (String segmentName : appendData.keySet()) {
                if (segmentName.equals(firstSegmentName)) {
                    // FirstSegment is where we'll be concat-ting to.
                    continue;
                }

                SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, Timeout).join();
                SegmentProperties sourceProps = s.getStreamSegmentInfo(segmentName, Timeout).join();

                s.concat(firstSegmentName, segmentName, Timeout).join();
                concatOrder.add(segmentName);
                SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, Timeout).join();
                AssertExtensions.assertThrows(
                        "concat() did not delete source segment",
                        s.getStreamSegmentInfo(segmentName, Timeout),
                        ex -> ex instanceof StreamSegmentNotExistsException);

                // Only check lengths here; we'll check the contents at the end.
                Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.", preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
            }

            // Check the contents of the first StreamSegment. We already validated that the length is correct.
            SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentName, Timeout).join();
            byte[] readbuffer = new byte[(int) segmentProperties.getLength()];

            // Read the entire StreamSegment.
            int bytesRead = s.read(firstSegmentName, 0, readbuffer, 0, readbuffer.length, Timeout).join();
            Assert.assertEquals("Unexpected number of bytes read.", readbuffer.length, bytesRead);

            // Check, concat-by-concat, that the final data is correct.
            int offset = 0;
            for (String segmentName : concatOrder) {
                byte[] concatData = appendData.get(segmentName).toByteArray();
                AssertExtensions.assertArrayEquals("Unexpected concat data.", concatData, 0, readbuffer, offset, concatData.length);
                offset += concatData.length;
            }

            Assert.assertEquals("Concat included more bytes than expected.", offset, readbuffer.length);
        }
    }

    private String getSegmentName(int id) {
        return Integer.toString(id);
    }

    private HashMap<String, ByteArrayOutputStream> populate(Storage s, int segmentCount, int appendsPerSegment) throws Exception {
        HashMap<String, ByteArrayOutputStream> appendData = new HashMap<>();

        for (int segmentId = 0; segmentId < segmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId);

            s.create(segmentName, Timeout).join();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            appendData.put(segmentName, writeStream);

            long offset = 0;
            for (int j = 0; j < appendsPerSegment; j++) {
                byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
                ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
                s.write(segmentName, offset, dataStream, writeData.length, Timeout).join();
                writeStream.write(writeData);
                offset += writeData.length;
            }
        }
        return appendData;
    }

    protected Storage createStorage() {
        return new InMemoryStorage();
    }
}
