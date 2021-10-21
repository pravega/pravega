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
package io.pravega.segmentstore.server.logs;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.AssertExtensions;
import java.util.List;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the DataFrame class.
 */
public class DataFrameTests {
    private static final int ENTRY_HEADER_SIZE = 5; // This is a copy of DataFrame.EntryHeader.HeaderSize, but that's not accessible from here.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the ability to append a set of records to a DataFrame, serialize it, deserialize it, and then read those
     * records back.
     */
    @Test
    public void testSerialization() throws Exception {
        int maxFrameSize = 2 * 1024 * 1024;
        int maxRecordCount = 4500;
        int minRecordSize = 0;
        int maxRecordSize = 1024;
        List<ByteArraySegment> allRecords = DataFrameTestHelpers.generateRecords(maxRecordCount, minRecordSize, maxRecordSize, ByteArraySegment::new);

        // Append some records.
        DataFrame writeFrame = DataFrame.ofSize(maxFrameSize);
        int recordsAppended = appendRecords(allRecords, writeFrame);
        AssertExtensions.assertGreaterThan("Did not append enough records. Test may not be valid.", allRecords.size() / 2, recordsAppended);
        writeFrame.seal();

        val frameData = writeFrame.getData();
        Assert.assertEquals("Unexpected length from getData().", writeFrame.getLength(), frameData.getLength());

        // Read them back, by deserializing the frame.
        val contents = DataFrame.read(frameData.getReader(), frameData.getLength(), writeFrame.getAddress());
        DataFrameTestHelpers.checkReadRecords(contents, allRecords, b -> b);
    }

    /**
     * Tests the ability to Start/End/Discard an entry.
     */
    @Test
    public void testStartEndDiscardEntry() {
        int dataFrameSize = 1000;
        DataFrame df = DataFrame.ofSize(dataFrameSize);
        AssertExtensions.assertThrows(
                "append(byte) worked even though no entry started.",
                () -> df.append((byte) 1),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "append(ByteArraySegment) worked even though no entry started.",
                () -> df.append(new ByteArraySegment(new byte[1]).getBufferViewReader()),
                ex -> ex instanceof IllegalStateException);

        // Start a new entry.
        boolean started = df.startNewEntry(true);
        Assert.assertTrue("Unable to start a new entry in a blank frame.", started);

        // Append some data until we reach the end.
        int bytesAppended = 0;
        while (df.append((byte) 1) > 0) {
            bytesAppended++;
        }

        // This is how many bytes we have available for writing (add something for the EntryHeader as well).
        int usableFrameLength = bytesAppended + ENTRY_HEADER_SIZE;

        // Discard everything we have so far, so our frame should revert back to an empty one.
        df.discardEntry();

        // Start a new entry, and write about 1/3 of the usable space.
        started = df.startNewEntry(true);
        Assert.assertTrue("Unable to start a new entry in a blank frame.", started);
        bytesAppended = 0;
        for (int i = 0; i < usableFrameLength / 3; i++) {
            if (df.append((byte) 1) == 0) {
                Assert.fail("Unable to append data even though we haven't filled out the frame.");
            }

            bytesAppended++;
        }

        // End the record using endEntry.
        boolean spaceAvailable = df.endEntry(true);
        Assert.assertTrue("endEntry returned false even though we did not fill up the frame.", spaceAvailable);

        // Start a new entry, and write about 1/3 of the usable space.
        started = df.startNewEntry(true);
        Assert.assertTrue("Unable to start a new entry in a non-full frame.", started);
        for (int i = 0; i < usableFrameLength / 3; i++) {
            if (df.append((byte) 1) == 0) {
                Assert.fail("Unable to append data even though we haven't filled out the frame.");
            }

            bytesAppended++;
        }

        // Start a new entry (and purposefully don't close the old one - it will auto-close), and write until the end.
        started = df.startNewEntry(true);
        Assert.assertTrue("Unable to start a new entry in a non-full frame.", started);
        while (df.append((byte) 1) > 0) {
            bytesAppended++;
        }

        spaceAvailable = df.endEntry(true);
        Assert.assertFalse("endEntry returned true even though we filled up the frame.", spaceAvailable);

        started = df.startNewEntry(true);
        Assert.assertFalse("Able to start a new entry in a full frame.", started);

        // Verify we were able to write the expected number of bytes. Each entry uses 'ENTRY_HEADER_SIZE' bytes for its header,
        // and we have 3 entries.
        Assert.assertEquals("Unexpected number of bytes appended.", usableFrameLength - 3 * ENTRY_HEADER_SIZE, bytesAppended);
    }

    /**
     * Test getFrameSequence() and getPreviousSequence().
     */
    @Test
    public void testFrameSequence() {
        long newSequence = 67890;
        int dataFrameSize = 1000;
        DataFrame df = DataFrame.ofSize(dataFrameSize);

        LogAddress a = new LogAddress(newSequence) {
        };

        df.setAddress(a);
        Assert.assertEquals("Unexpected value for getFrameSequence().", newSequence, df.getAddress().getSequence());
    }

    private int appendRecords(List<ByteArraySegment> allRecords, DataFrame dataFrame) {
        int fullRecordsAppended = 0;
        boolean filledUpFrame = false;
        for (ByteArraySegment record : allRecords) {
            // Append the first half of the record as one DataFrame Entry.
            dataFrame.startNewEntry(true); // true - this is the first entry for the record.
            int firstHalfLength = record.getLength() / 2;
            int bytesAppended = dataFrame.append(record.slice(0, firstHalfLength).getBufferViewReader());
            dataFrame.endEntry(false); // false - we did not finish the record.
            if (bytesAppended < firstHalfLength) {
                // We filled out the frame.
                filledUpFrame = true;
                break;
            }

            // Append the second half of the record as one DataFrame Entry.
            dataFrame.startNewEntry(false); // false - this is not the first entry for the record.
            int secondHalfLength = record.getLength() - firstHalfLength;
            bytesAppended = dataFrame.append(record.slice(firstHalfLength, secondHalfLength).getBufferViewReader());
            fullRecordsAppended += bytesAppended;
            if (bytesAppended < secondHalfLength) {
                // We filled out the frame.
                dataFrame.endEntry(false); // false - we did not finish the record.
                filledUpFrame = true;
                break;
            }

            dataFrame.endEntry(true); // true - we finished the record.
            fullRecordsAppended++;
        }

        Assert.assertTrue("We did not fill up the DataFrame. This test may not exercise all of the features of DataFrame.", filledUpFrame);

        return fullRecordsAppended;
    }
}
