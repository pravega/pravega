/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the StreamSegmentStorageReader class.
 */
public class StreamSegmentStorageReaderTests extends ThreadPooledTestSuite {
    private static final String SEGMENT_NAME = "Segment";
    private static final int SEGMENT_APPEND_COUNT = 10;
    private static final int SEGMENT_LENGTH = SEGMENT_APPEND_COUNT * 1024;
    private static final int SEGMENT_APPEND_SIZE = SEGMENT_LENGTH / SEGMENT_APPEND_COUNT;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the read() method on a non-truncated, non-sealed segment.
     */
    @Test
    public void testNormalRead() throws Exception {
        @Cleanup
        val s = createStorage();
        val writtenData = populate(s);
        val si = StreamSegmentInformation.builder().name(SEGMENT_NAME).length(writtenData.length).startOffset(0).sealed(false).build();

        // We try to read 1 byte beyond the end of the segment to verify the reading ends correctly.
        val readResult = StreamSegmentStorageReader.read(si, 0, writtenData.length + 1, SEGMENT_APPEND_SIZE - 1, s);
        verifyReadResult(readResult, si, 0, writtenData.length, writtenData);
    }

    /**
     * Tests the read() method on a truncated segment.
     */
    @Test
    public void testTruncatedSegment() throws Exception {
        @Cleanup
        val s = createStorage();
        val writtenData = populate(s);
        val si = StreamSegmentInformation.builder().name(SEGMENT_NAME).length(writtenData.length).startOffset(SEGMENT_APPEND_SIZE)
                .sealed(false).build();

        // 1. Read a truncated offset.
        val truncatedResult = StreamSegmentStorageReader.read(si, 0, writtenData.length + 1, SEGMENT_APPEND_SIZE - 1, s);
        verifyReadResult(truncatedResult, si, 0, 0, writtenData);

        // 2. Read a truncated offset.
        val nonTruncatedResult = StreamSegmentStorageReader.read(si, SEGMENT_APPEND_SIZE, writtenData.length + 1, SEGMENT_APPEND_SIZE - 1, s);
        verifyReadResult(nonTruncatedResult, si, SEGMENT_APPEND_SIZE, writtenData.length - SEGMENT_APPEND_SIZE, writtenData);
    }

    /**
     * Tests the read() method on a Sealed Segment.
     */
    @Test
    public void testSealedSegment() throws Exception {
        @Cleanup
        val s = createStorage();
        val writtenData = populate(s);
        val si = StreamSegmentInformation.builder().name(SEGMENT_NAME).length(writtenData.length).startOffset(0).sealed(true).build();

        // We try to read 1 byte beyond the end of the segment to verify the reading ends correctly.
        val readResult = StreamSegmentStorageReader.read(si, 0, writtenData.length + 1, SEGMENT_APPEND_SIZE - 1, s);
        verifyReadResult(readResult, si, 0, writtenData.length, writtenData);
    }

    /**
     * Tests the read() method when errors are present.
     */
    @Test
    public void testReadWithErrors() throws Exception {
        @Cleanup
        val s = createStorage();

        // 1. Segment does not exist.
        val si1 = StreamSegmentInformation.builder().name(SEGMENT_NAME).length(SEGMENT_LENGTH).startOffset(0).sealed(true).build();
        val rr1 = StreamSegmentStorageReader.read(si1, 0, SEGMENT_LENGTH, SEGMENT_APPEND_SIZE - 1, s);
        val firstEntry1 = rr1.next();
        Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage, firstEntry1.getType());
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected exception when Segment does not exist initially.",
                () -> {
                    firstEntry1.requestContent(TIMEOUT);
                    return firstEntry1.getContent();
                },
                ex -> ex instanceof StreamSegmentNotExistsException);

        populate(s);

        // 2. Segment exists initially, but is deleted while reading.
        val si2 = StreamSegmentInformation.builder().name(SEGMENT_NAME).length(SEGMENT_LENGTH).startOffset(0).sealed(true).build();
        val rr2 = StreamSegmentStorageReader.read(si2, 0, SEGMENT_LENGTH, SEGMENT_APPEND_SIZE - 1, s);

        // Skip over the first entry.
        val firstEntry2 = rr2.next();
        firstEntry2.requestContent(TIMEOUT);
        firstEntry2.getContent().join();

        // Delete the Segment, then attempt to read again.
        s.delete(s.openWrite(SEGMENT_NAME).join(), TIMEOUT).join();
        val secondEntry = rr2.next();
        Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage, secondEntry.getType());
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected exception when Segment was deleted while reading.",
                () -> {
                    secondEntry.requestContent(TIMEOUT);
                    return secondEntry.getContent();
                },
                ex -> ex instanceof StreamSegmentNotExistsException);

    }

    public static void verifyReadResult(ReadResult readResult, SegmentProperties si, int expectedStartOffset,
                                        int expectedLength, byte[] writtenData) throws Exception {
        Assert.assertEquals("Unexpected ReadResult.StartOffset.", expectedStartOffset, readResult.getStreamSegmentStartOffset());
        while (readResult.hasNext()) {
            val entry = readResult.next();
            if (entry.getStreamSegmentOffset() < si.getStartOffset()) {
                Assert.assertEquals("Expected a truncated ReadResultEntry.", ReadResultEntryType.Truncated, entry.getType());
                Assert.assertFalse("Not expecting ReadResult to have any other entries.", readResult.hasNext());
                break;
            } else if (entry.getStreamSegmentOffset() == si.getLength()) {
                Assert.assertEquals("Expected an EndOfSegment ReadResultEntry.", ReadResultEntryType.EndOfStreamSegment, entry.getType());
                Assert.assertFalse("Not expecting ReadResult to have any other entries.", readResult.hasNext());
                break;
            } else {
                Assert.assertEquals("Expected a Storage ReadResultEntry.", ReadResultEntryType.Storage, entry.getType());
                entry.requestContent(TIMEOUT);
                val contents = entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                AssertExtensions.assertGreaterThanOrEqual("Empty read entry contents.", 0, contents.getLength());
                val readData = contents.getCopy();
                AssertExtensions.assertArrayEquals("Unexpected data read back.", writtenData, (int) entry.getStreamSegmentOffset(),
                        readData, 0, readData.length);
            }
        }

        Assert.assertEquals("Unexpected consumed length.", expectedLength, readResult.getConsumedLength());
    }

    private byte[] populate(Storage s) {
        byte[] data = new byte[SEGMENT_LENGTH];
        val rnd = new Random(0);
        rnd.nextBytes(data);
        val handle = s.create(SEGMENT_NAME, TIMEOUT)
                .thenCompose(si -> s.openWrite(SEGMENT_NAME)).join();
        final int appendSize = data.length / SEGMENT_APPEND_COUNT;
        int offset = 0;
        for (int i = 0; i < appendSize; i++) {
            int writeLength = Math.min(appendSize, data.length - offset);
            s.write(handle, offset, new ByteArrayInputStream(data, offset, writeLength), writeLength, TIMEOUT).join();
            offset += writeLength;
        }

        return data;
    }

    private Storage createStorage() {
        val factory = new InMemoryStorageFactory(executorService());
        return factory.createStorageAdapter();
    }
}
