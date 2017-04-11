/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.shared.testcommon.AssertExtensions;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

/**
 * Helper methods for testing DataFrames.
 */
class DataFrameTestHelpers {
    /**
     * Generates a collection of TestLogItems.
     */
    static ArrayList<TestLogItem> generateLogItems(int count, int minSize, int maxSize, int startSeqNo) {
        ArrayList<byte[]> rawRecords = DataFrameTestHelpers.generateRecords(count, minSize, maxSize);
        ArrayList<TestLogItem> result = new ArrayList<>(rawRecords.size());
        for (int i = 0; i < count; i++) {
            result.add(new TestLogItem(startSeqNo + i, rawRecords.get(i)));
        }

        return result;
    }

    /**
     * Generates a collection of byte[] records.
     */
    static ArrayList<byte[]> generateRecords(int count, int minSize, int maxSize) {
        return generateRecords(count, minSize, maxSize, r -> r);
    }

    /**
     * Generates a collection of records and converts them to a custom data type (i.e. ByteArraySegment).
     */
    static <T> ArrayList<T> generateRecords(int count, int minSize, int maxSize, Function<byte[], T> recordConverter) {
        assert minSize <= maxSize;
        Random random = new Random(0);
        ArrayList<T> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int size = (maxSize != minSize ? random.nextInt(maxSize - minSize) : 0) + minSize;
            byte[] record = new byte[size];
            random.nextBytes(record);
            result.add(recordConverter.apply(record));
        }

        return result;
    }

    /**
     * Checks that the given data frame contains the given collection of records.
     */
    static <T> void checkReadRecords(Collection<DataFrame> dataFrames, List<T> records, Function<T, ByteArraySegment> recordConverter) throws Exception {
        checkReadRecords(dataFrames, records, new HashSet<>(), recordConverter);
    }

    /**
     * Checks that the given data frame contains the given collection of records.
     */
    static <T> void checkReadRecords(DataFrame dataFrame, List<T> records, Function<T, ByteArraySegment> recordConverter) throws Exception {
        ArrayList<DataFrame> frames = new ArrayList<>();
        frames.add(dataFrame);
        checkReadRecords(frames, records, recordConverter);
    }

    /**
     * Checks that the given collection of DataFrames contain the given collection of records.
     */
    static <T> void checkReadRecords(Collection<DataFrame> dataFrames, List<T> records, Collection<Integer> knownBadRecordIndices, Function<T, ByteArraySegment> recordConverter) throws Exception {
        ReadState state = new ReadState(records.size(), knownBadRecordIndices);

        for (DataFrame dataFrame : dataFrames) {
            CloseableIterator<DataFrame.DataFrameEntry, SerializationException> reader = dataFrame.getEntries();
            DataFrame.DataFrameEntry entry;
            boolean isLastEntryInFrame = true;
            while ((entry = reader.getNext()) != null) {

                // General DataFrameEntry validation.
                Assert.assertNotNull("Received a null entry even though hasNext() returned true." + state.getPosition(), entry);
                Assert.assertEquals(
                        "Unexpected value returned by getDataFrameAddress(). " + state.getPosition(),
                        dataFrame.getAddress(),
                        entry.getDataFrameAddress());

                if (entry.isFirstRecordEntry()) {
                    state.clearCurrentRecordEntries();
                }

                isLastEntryInFrame = entry.isLastEntryInDataFrame();

                // Record the current entry
                state.getCurrentRecordEntries().add(entry);
                if (entry.isLastRecordEntry()) {
                    // We have reached the LastEntry for a Record. We are now ready to compare it to the current record.
                    Assert.assertFalse("Unexpected entry with isLastRecordEntry flag (when current record is bad).", state.isCurrentRecordBad());

                    int nextGoodRecordIndex = state.getNextGoodRecordIndex();
                    AssertExtensions.assertGreaterThan("No more valid records to compare to.", nextGoodRecordIndex, records.size());
                    ByteArraySegment currentRecord = recordConverter.apply(records.get(nextGoodRecordIndex));
                    AssertExtensions.assertLessThanOrEqual(
                            "Accumulated entries have more bytes than the current record has." + state.getPosition(),
                            currentRecord.getLength(),
                            state.getCurrentRecordEntriesSize());

                    int recordOffset = 0;
                    for (DataFrame.DataFrameEntry recordEntry : state.getCurrentRecordEntries()) {
                        ByteArraySegment entryData = recordEntry.getData();
                        for (int i = 0; i < entryData.getLength(); i++) {
                            if (currentRecord.get(recordOffset) != entryData.get(i)) {
                                Assert.fail(String.format("Unexpected entry contents. FrameIndex = %d, RecordIndex = %d, EntryNumberInRecord = %d.", state.getFrameIndex(), state.getNextGoodRecordIndex(), i));
                            }

                            recordOffset++;
                        }
                    }

                    Assert.assertEquals("isLastRecordEntry() indicates true but there are bytes remaining to be read in the record.", currentRecord.getLength(), recordOffset);
                    state.clearCurrentRecordEntries();
                }
            }

            // Verify the accuracy of isLastEntryInDataFrame() - if it's true, then hasNext() should be false.
            Assert.assertEquals(
                    "Unexpected value for isLastEntryInDataFrame()." + state.getPosition(),
                    reader.getNext() == null,
                    isLastEntryInFrame);

            state.moveToNextFrame();
        }

        //Assert.assertEquals("Reached the end of the DataFrames but there are still records to be expected.", records.size(), state.getNextGoodRecordIndex());
    }

    //region ReadState

    private static class ReadState {
        private final Collection<Integer> knownBadRecordIndices;
        private final ArrayList<DataFrame.DataFrameEntry> currentRecordEntries;
        private final int recordCount;
        private int recordIndex;
        private int currentRecordEntriesSize;
        private int frameIndex;

        ReadState(int recordCount, Collection<Integer> knownBadRecordIndices) {
            this.recordCount = recordCount;
            this.knownBadRecordIndices = knownBadRecordIndices;
            this.currentRecordEntries = new ArrayList<>();
            this.recordIndex = -1;
            this.currentRecordEntriesSize = 0;
            this.frameIndex = 0;
        }

        int getNextGoodRecordIndex() {
            do {
                this.recordIndex++;
            } while (this.recordIndex < this.recordCount && this.knownBadRecordIndices.contains(this.recordIndex));

            return this.recordIndex;
        }

        boolean isCurrentRecordBad() {
            return this.knownBadRecordIndices.contains(this.recordIndex);
        }

        List<DataFrame.DataFrameEntry> getCurrentRecordEntries() {
            return this.currentRecordEntries;
        }

        int getCurrentRecordEntriesSize() {
            return this.currentRecordEntriesSize;
        }

        int getFrameIndex() {
            return this.frameIndex;
        }

        void clearCurrentRecordEntries() {
            this.currentRecordEntries.clear();
            this.currentRecordEntriesSize = 0;
        }

        void moveToNextFrame() {
            this.frameIndex++;
        }

        String getPosition() {
            return String.format(" FrameIndex = %d, RecordIndex = %d, EntryIndexInRecord = %d.", this.frameIndex, this.recordIndex, this.currentRecordEntries.size());
        }
    }

    //endregion
}
