package com.emc.logservice.server.logs;

import com.emc.logservice.common.ByteArraySegment;
import com.emc.logservice.common.IteratorWithException;
import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

/**
 * Created by andrei on 6/15/16.
 */
class DataFrameTestHelpers {
    /**
     * Generates a collection of byte[] records.
     *
     * @param count
     * @param maxSize
     * @return
     */
    public static ArrayList<byte[]> generateRecords(int count, int maxSize) {
        return generateRecords(count, maxSize, r -> r);
    }

    /**
     * Generates a collection of records and converts them to a custom data type (i.e. ByteArraySegment).
     *
     * @param count
     * @param maxSize
     * @param recordConverter
     * @param <T>
     * @return
     */
    public static <T> ArrayList<T> generateRecords(int count, int maxSize, Function<byte[], T> recordConverter) {
        Random random = new Random(0);
        ArrayList<T> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] record = new byte[maxSize];
            random.nextBytes(record);
            result.add(recordConverter.apply(record));
        }

        return result;
    }

    /**
     * Checks that the given data frame contains the given collection of records.
     *
     * @param dataFrames
     * @param records
     * @param recordConverter
     * @param <T>
     * @throws Exception
     */
    public static <T> void checkReadRecords(Collection<DataFrame> dataFrames, List<T> records, Function<T, ByteArraySegment> recordConverter) throws Exception {
        int recordIndex = 0;
        int offsetWithinCurrentRecord = 0;
        boolean expectingStartOfRecord = true;
        for (DataFrame dataFrame : dataFrames) {
            IteratorWithException<DataFrame.DataFrameEntry, SerializationException> reader = dataFrame.getEntries();
            while (reader.hasNext()) {
                DataFrame.DataFrameEntry entry = reader.pollNext();
                Assert.assertNotNull("Received a null entry even though hasNext() returned true.", entry);
                Assert.assertEquals("Unexpected value returned by getDataFrameSequence().", dataFrame.getFrameSequence(), entry.getDataFrameSequence());
                Assert.assertEquals("Unexpected value from isFirstRecordEntry().", expectingStartOfRecord, entry.isFirstRecordEntry());
                if (entry.isFirstRecordEntry()) {
                    Assert.assertTrue("DataFrame has more entries than expected.", recordIndex < records.size());
                    offsetWithinCurrentRecord = 0;
                    expectingStartOfRecord = false;
                }

                // Check, byte-by-byte, that what we read back is the same as what we put in.
                ByteArraySegment currentRecord = recordConverter.apply(records.get(recordIndex));
                AssertExtensions.assertGreaterThanOrEqual("Entry has more bytes than remaining in the current record.", currentRecord.getLength() - offsetWithinCurrentRecord, entry.getData().getLength());
                InputStream expectedStream = currentRecord.getReader(offsetWithinCurrentRecord, currentRecord.getLength() - offsetWithinCurrentRecord);
                InputStream actualStream = entry.getData().getReader();
                AssertExtensions.assertStreamEquals(
                        String.format("Unexpected entry contents. Record %d, RecordEntryOffset %d.", recordIndex, offsetWithinCurrentRecord),
                        expectedStream,
                        actualStream,
                        entry.getData().getLength());

                offsetWithinCurrentRecord += entry.getData().getLength();
                if (entry.isLastRecordEntry()) {
                    Assert.assertEquals(
                            String.format("isLastRecordEntry() indicates true but there are bytes remaining to be read in the record. Record %d, RecordEntryOffset %d.", recordIndex, offsetWithinCurrentRecord),
                            0,
                            currentRecord.getLength() - offsetWithinCurrentRecord);
                    recordIndex++;
                    Assert.assertEquals("Unexpected value for isLastEntryInDataFrame().", recordIndex >= records.size(), entry.isLastEntryInDataFrame());
                    expectingStartOfRecord = true;
                }
            }
        }
    }

    /**
     * Checks that the given data frame contains the given collection of records.
     *
     * @param dataFrame
     * @param records
     * @param recordConverter
     * @param <T>
     * @throws Exception
     */
    public static <T> void checkReadRecords(DataFrame dataFrame, List<T> records, Function<T, ByteArraySegment> recordConverter) throws Exception {
        ArrayList<DataFrame> frames = new ArrayList<>();
        frames.add(dataFrame);
        checkReadRecords(frames, records, recordConverter);
    }
}
