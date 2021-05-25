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

import com.google.common.base.Preconditions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.AssertExtensions;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Consumer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the DataFrameInputStream class.
 */
public class DataFrameInputStreamTests {
    private static final int FRAME_SIZE = 512;
    private static final int RECORD_SIZE = (int) (FRAME_SIZE * 0.75);
    private static final int RECORD_COUNT = 100;
    private static final String TRACE_ID = "Trace";

    /**
     * Tests a general read case when the data is in ideal condition.
     */
    @Test
    public void testReadsNoFailure() throws Exception {
        val items = generateData();
        try (val reader = toReader(toDataFrames(items));
             val inputStream = new DataFrameInputStream(reader, TRACE_ID)) {
            check(items, inputStream, Collections.emptySet());
        }
    }

    /**
     * Tests a general read case while reading records only partially (verifies that endRecord() skips over to the next one).
     */
    @Test
    public void testReadsIncompleteRecords() throws Exception {
        val items = generateData();
        try (val reader = toReader(toDataFrames(items));
             val inputStream = new DataFrameInputStream(reader, TRACE_ID)) {
            int readLength = 0;
            for (val item : items) {
                readLength = (readLength + 1) % RECORD_SIZE;
                inputStream.beginRecord();
                val readData = StreamHelpers.readAll(inputStream, readLength);
                Assert.assertEquals("Unexpected read length.", readLength, readData.length);
                AssertExtensions.assertArrayEquals("Unexpected data.", item.data, 0, readData, 0, readData.length);
                inputStream.endRecord();
            }

            Assert.assertFalse("beginRecord() returned true when reaching the end.", inputStream.beginRecord());
            Assert.assertEquals("read() allowed reading beyond the end of the DataFrameInputStream.", -1, inputStream.read());
            Assert.assertTrue("Expected InputStream to be closed.", inputStream.isClosed());
        }
    }

    /**
     * Tests the case when the DataFrameInputStream begins with a partial record at the beginning (i.e., after a truncation).
     */
    @Test
    public void testSkipPartialRecordBeginning() throws Exception {
        val items = generateData();
        val dataFrames = toDataFrames(items);
        LogItem removedFrame = dataFrames.remove(0);
        try (val reader = toReader(dataFrames, 0, dataFrames.size() - 1);
             val inputStream = new DataFrameInputStream(reader, TRACE_ID)) {
            // We expect the first two records to be dropped since they are either entirely (#0) or partially (#1) in the
            // first frame, which we skip.
            val expectedMissing = getExpectedMissingItemIndices(items, removedFrame);
            check(items, inputStream, expectedMissing);
        }
    }

    /**
     * Tests the case when the DataFrameInputStream has a partial record in the middle (failed serialization).
     */
    @Test
    public void testPartialRecordMiddle() throws Exception {
        val items = generateData();
        val dataFrames = toDataFrames(items);

        // We need to remove a DataFrame that ends with an entire record; otherwise if we have a partial record in the
        // middle we'll end up interpreting that as a corruption.
        LogItem removedFrame = null;
        for (int i = 1; i < items.size(); i++) {
            val item = items.get(i);
            val prevItem = items.get(i - 1);
            if (item.dataFrames.size() == 1 && item.address.getSequence() != prevItem.address.getSequence()) {
                // We found it.
                removedFrame = dataFrames.remove((int) prevItem.address.getSequence());
                break;
            }
        }

        Assert.assertNotNull("Unable to locate a frame worthy of removal.", removedFrame);
        try (val reader = toReader(dataFrames, 0, dataFrames.size() - 1);
             val inputStream = new DataFrameInputStream(reader, TRACE_ID)) {
            // We expect the first two records to be dropped since they are either entirely (#0) or partially (#1) in the
            // first frame, which we skip.
            val expectedMissing = getExpectedMissingItemIndices(items, removedFrame);
            check(items, inputStream, expectedMissing);
        }
    }

    /**
     * Tests the case when the DataFrameInputStream has a partial record at the end.
     */
    @Test
    public void testPartialRecordEnd() throws Exception {
        val items = generateData();
        val dataFrames = toDataFrames(items);
        try (val reader = toReader(dataFrames, 0, dataFrames.size() - 2);
             val inputStream = new DataFrameInputStream(reader, TRACE_ID)) {
            // We expect the first two records to be dropped since they are either entirely (#0) or partially (#1) in the
            // first frame, which we skip.
            val expectedMissing = getExpectedMissingItemIndices(items, dataFrames.get(dataFrames.size() - 1));
            check(items, inputStream, expectedMissing);
        }
    }

    private void check(ArrayList<TestItem> items, DataFrameInputStream inputStream, Collection<Integer> expectedMissingIndices) throws Exception {
        boolean missingLastItem = false; // Whether the last item was missing.
        boolean multiSpan = false; // Whether the first item in a group of consecutive missing items spanned multiple frames.
        for (int i = 0; i < items.size(); i++) {
            val expectedData = items.get(i);
            if (expectedMissingIndices.contains(i)) {
                if (!missingLastItem && expectedData.dataFrames.size() > 1) {
                    multiSpan = true;
                }
                missingLastItem = true;
                continue;
            }

            if (missingLastItem && multiSpan) {
                // We have a partial record somewhere. Verify we are properly notified of it.
                val br = inputStream.beginRecord();
                Assert.assertTrue("Unexpected value for beginRecord() for record " + i, br);
                AssertExtensions.assertThrows(
                        "Able to read an entire partial record in the middle.",
                        () -> StreamHelpers.readAll(inputStream, RECORD_SIZE),
                        ex -> ex instanceof DataFrameInputStream.RecordResetException);
                missingLastItem = false;
                multiSpan = false;

                // Verify that we cannot read or skip anymore (until we call beginRecord() again).
                AssertExtensions.assertThrows(
                        "Able to read(byte) after processing partial record.",
                        inputStream::read,
                        ex -> ex instanceof IllegalStateException);
                AssertExtensions.assertThrows(
                        "Able to read(byte[]) after processing partial record.",
                        () -> {
                            int ignored = inputStream.read(new byte[1], 0, 1);
                        },
                        ex -> ex instanceof IllegalStateException);
                AssertExtensions.assertThrows(
                        "Able to skip after processing partial record.",
                        () -> {
                            long ignored = inputStream.skip(1);
                        },
                        ex -> ex instanceof IllegalStateException);
            }

            val br = inputStream.beginRecord();
            Assert.assertTrue("Unexpected value for beginRecord() for record " + i, br);
            val readData = StreamHelpers.readAll(inputStream, RECORD_SIZE);
            Assert.assertEquals("Unexpected length for record " + i, expectedData.data.length, readData.length);
            Assert.assertArrayEquals("Unexpected data for record " + i, expectedData.data, readData);
            val recordInfo = inputStream.endRecord();
            Assert.assertEquals("Unexpected address for record " + i,
                    expectedData.address.getSequence(), recordInfo.getLastUsedDataFrameAddress().getSequence());
        }

        if (missingLastItem && multiSpan) {
            Assert.assertTrue("beginRecord() returned false when a partial record was expected.", inputStream.beginRecord());
            AssertExtensions.assertThrows(
                    "Able to read an entire partial record at the end.",
                    () -> StreamHelpers.readAll(inputStream, RECORD_SIZE),
                    ex -> ex instanceof DataFrameInputStream.NoMoreRecordsException);
        } else {
            Assert.assertFalse("beginRecord() returned true when reaching the end.", inputStream.beginRecord());
        }

        Assert.assertEquals("read() allowed reading beyond the end of the DataFrameInputStream.", -1, inputStream.read());
        Assert.assertTrue("Expected InputStream to be closed.", inputStream.isClosed());
    }

    private HashSet<Integer> getExpectedMissingItemIndices(ArrayList<TestItem> items, LogItem missingDataFrame) {
        val result = new HashSet<Integer>();
        for (int i = 0; i < items.size(); i++) {
            val ti = items.get(i);
            if (ti.dataFrames.contains(missingDataFrame.address.getSequence())) {
                result.add(i);
            }
        }

        return result;
    }

    private ArrayList<TestItem> generateData() throws Exception {
        val result = new ArrayList<TestItem>(RECORD_COUNT);
        for (int i = 0; i < RECORD_COUNT; i++) {
            byte[] recordData = new byte[RECORD_SIZE];
            for (int j = 0; j < recordData.length; j++) {
                recordData[j] = (byte) ((i + j) % Byte.MAX_VALUE);
            }
            result.add(new TestItem(recordData));
        }
        return result;
    }

    private ArrayList<LogItem> toDataFrames(ArrayList<TestItem> items) throws Exception {
        val result = new ArrayList<LogItem>(RECORD_COUNT);
        Consumer<DataFrame> addCallback = df -> result.add(new LogItem(df.getData().getReader(), df.getData().getLength(), new TestLogAddress(result.size())));
        try (val dos = new DataFrameOutputStream(FRAME_SIZE, addCallback)) {
            for (TestItem item : items) {
                dos.startNewRecord();
                long beginSequence = result.size();
                dos.write(item.data);
                dos.endRecord();
                item.address = new TestLogAddress(result.size());
                for (long s = beginSequence; s <= item.address.getSequence(); s++) {
                    item.dataFrames.add(s);
                }
            }
            dos.flush();
        }
        return result;
    }

    private CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> toReader(ArrayList<LogItem> dataFrames) {
        return toReader(dataFrames, 0, dataFrames.size() - 1);
    }

    private CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> toReader(ArrayList<LogItem> dataFrames, int startIndex, int endIndex) {
        return new FrameIterator(dataFrames, startIndex, endIndex);
    }

    private static class FrameIterator implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        private final ArrayList<LogItem> items;
        private final int endIndex;
        private int pos = 0;

        FrameIterator(ArrayList<LogItem> dataFrames, int startIndex, int endIndex) {
            this.items = Preconditions.checkNotNull(dataFrames, "dataFrames");
            Preconditions.checkElementIndex(startIndex, dataFrames.size());
            this.endIndex = Preconditions.checkElementIndex(endIndex, dataFrames.size());
            Preconditions.checkArgument(startIndex <= endIndex);
            this.pos = startIndex;
        }

        @Override
        public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
            if (this.pos > this.endIndex) {
                return null;
            }

            return this.items.get(this.pos++);
        }

        @Override
        public void close() {
            this.pos = Integer.MAX_VALUE;
        }
    }

    @RequiredArgsConstructor
    private static class TestItem {
        private final HashSet<Long> dataFrames = new HashSet<>();
        private final byte[] data;
        private LogAddress address;
    }

    @Data
    private static class LogItem implements DurableDataLog.ReadItem {
        private final InputStream payload;
        private final int length;
        private final LogAddress address;
    }
}
