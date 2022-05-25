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

import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.function.Callbacks;
import io.pravega.common.io.SerializationException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Unit tests for DataFrameReader class.
 */
public class DataFrameReaderTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1234567;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SMALL_RECORD_MIN_SIZE = 0;
    private static final int SMALL_RECORD_MAX_SIZE = 128;
    private static final int LARGE_RECORD_MIN_SIZE = 1024;
    private static final int LARGE_RECORD_MAX_SIZE = 10240;
    private static final int FRAME_SIZE = 512;
    private static final Serializer<TestLogItem> SERIALIZER = new TestLogItem.TestLogItemSerializer();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests the happy case: DataFrameReader can read from a DataLog when the are no exceptions.
     */
    @Test
    public void testReadsNoFailure() throws Exception {
        int failEvery = 7; // Fail every X records (write-wise).

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        // Have every other 'failEvery' record fail after writing 90% of itself.
        for (int i = 0; i < records.size(); i += failEvery) {
            records.get(i).failSerializationAfterComplete(0.9, new IOException("intentional " + i));
        }

        HashSet<Integer> failedIndices = new HashSet<>();
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, Callbacks::doNothing, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, SERIALIZER, args)) {
                for (int i = 0; i < records.size(); i++) {
                    try {
                        b.append(records.get(i));
                    } catch (IOException ex) {
                        failedIndices.add(i);
                    }
                }
                b.flush();
            }

            TestSerializer logItemFactory = new TestSerializer();
            DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, logItemFactory, CONTAINER_ID);
            List<TestLogItem> readItems = readAll(reader);
            checkReadResult(records, failedIndices, readItems);
        }
    }

    /**
     * Tests the case when we begin reading from a DataFrame which begins with a partial record. That record needs to
     * be dropped (not returned). DataFrameReader should always return full records.
     */
    @Test
    public void testReadsWithPartialEntries() throws Exception {
        // This test will only work if LARGE_RECORD_MIN_SIZE > FRAME_SIZE.
        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(3, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MIN_SIZE, 0);
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            ArrayList<DataFrameBuilder.CommitArgs> commitFrames = new ArrayList<>();
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, SERIALIZER, args)) {
                for (TestLogItem r : records) {
                    b.append(r);
                }

                b.flush();
            }

            // Delete the first entry in the DataLog.
            ArrayList<Integer> failedIndices = new ArrayList<>();
            dataLog.truncate(commitFrames.get(0).getLogAddress(), TIMEOUT).join();

            // Given that each TestLogItem's length is larger than a data frame, truncating the first DataFrame will
            // invalidate the first one.
            failedIndices.add(0);

            TestSerializer logItemFactory = new TestSerializer();
            DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, logItemFactory, CONTAINER_ID);
            List<TestLogItem> readItems = readAll(reader);
            checkReadResult(records, failedIndices, readItems);
        }
    }

    /**
     * Tests the case when the DataFrameReader reads from a log and it encounters LogItem SerializationExceptions.
     */
    @Test
    public void testReadsWithDeserializationFailure() throws Exception {
        int failDeserializationEvery = 11; // Fail deserialization every X records (write-wise).

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, Callbacks::doNothing, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, SERIALIZER, args)) {
                for (TestLogItem r : records) {
                    b.append(r);
                }
            }

            ErrorInjector<SerializationException> errorInjector = new ErrorInjector<>(
                    count -> count % failDeserializationEvery == 0,
                    () -> new SerializationException("TestLogItem.deserialize intentional"));

            TestSerializer logItemFactory = new TestSerializer();
            logItemFactory.setDeserializationErrorInjector(errorInjector);
            testReadWithException(dataLog, logItemFactory, ex -> ex instanceof DataCorruptionException);
        }
    }

    /**
     * Tests the case when the DataFrameReader reads from a log and it encounters log read failures.
     * 1. Initial read failures.
     * 2. Somewhere in the middle of reading.
     */
    @Test
    public void testReadsWithDataLogFailure() throws Exception {
        int failReadSyncEvery = 3; // Fail reads synchronously every X attempts.

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, Callbacks::doNothing, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, SERIALIZER, args)) {
                for (TestLogItem r : records) {
                    b.append(r);
                }
            }

            TestSerializer logItemFactory = new TestSerializer();

            // Test 1: Initial call to getReader.
            ErrorInjector<Exception> getReaderErrorInjector = new ErrorInjector<>(
                    count -> true, // Fail every time.
                    () -> new DataLogNotAvailableException("intentional getReader exception"));
            dataLog.setReadErrorInjectors(getReaderErrorInjector, null);
            AssertExtensions.assertThrows(
                    "No exception or wrong type of exception thrown by getNext() with exception thrown by getReader().",
                    () -> new DataFrameReader<>(dataLog, logItemFactory, CONTAINER_ID),
                    ex -> Exceptions.unwrap(ex) == getReaderErrorInjector.getLastCycleException());

            // Test 2: Failures during getNext().
            ErrorInjector<Exception> readErrorInjector = new ErrorInjector<>(
                    count -> count % failReadSyncEvery == 0,
                    () -> new DataLogNotAvailableException("intentional getNext exception"));
            dataLog.setReadErrorInjectors(null, readErrorInjector);
            testReadWithException(dataLog, logItemFactory, ex -> ex == readErrorInjector.getLastCycleException());
        }
    }

    /**
     * Test DataFrameReader in case of reading a log with duplicate entries and no tolerance to repeated entries upon recovery.
     * The expected output is to throw DataCorruptionException while reading the log.
     */
    @Test
    public void testWithDuplicateLogItemsAndNoToleranceToDuplicates() {
        AssertExtensions.assertThrows(DataCorruptionException.class, () -> testWithDuplicateLogItems(0));
    }

    /**
     * Test DataFrameReader in case of reading a log with duplicate entries and tolerance to repeated entries upon recovery
     * (up to 10 elements behind). The expected output is to read the log skipping duplicate entries.
     */
    @Test
    public void testWithDuplicateLogItemsAndToleranceToDuplicates() throws Exception {
        testWithDuplicateLogItems(10);
    }

    private void testWithDuplicateLogItems(int toleratedOverlap) throws Exception {
        long startSeqNo = 0;
        int count = 100;
        int minSize = 10;
        int maxSize = 1000;
        int duplicates = 0;
        ArrayList<byte[]> rawRecords = DataFrameTestHelpers.generateRecords(count, minSize, maxSize);
        ArrayList<TestLogItem> result = new ArrayList<>(rawRecords.size());
        for (int i = 0; i < count; i++) {
            result.add(new TestLogItem(startSeqNo + i + duplicates, rawRecords.get(i)));
            // Let's add a duplicate element every 10.
            if (i % 10 == 0) {
                // Use this to cheat DataFrameBuilder, as it does not allow adding duplicate sequence numbers, but we
                // need that to test how DataFrameReader skips them when reading.
                TestLogItem duplicateLogItem = Mockito.spy(new TestLogItem(startSeqNo + i + duplicates, rawRecords.get(i)));
                duplicates++;
                Mockito.when(duplicateLogItem.getSequenceNumber()).thenReturn(startSeqNo + i + duplicates).thenCallRealMethod();
                result.add(duplicateLogItem);
            }
        }

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, Callbacks::doNothing, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, SERIALIZER, args)) {
                for (TestLogItem testLogItem : result) {
                    b.append(testLogItem);
                    b.flush();
                }
            }

            TestSerializer logItemFactory = new TestSerializer();
            @Cleanup
            DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, logItemFactory, CONTAINER_ID, toleratedOverlap);
            List<TestLogItem> readItems = readAll(reader);
            Assert.assertEquals(count, readItems.size());
            int actualSeqNumber = 0;
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(actualSeqNumber, readItems.get(i).getSequenceNumber());
                actualSeqNumber = (i % 10 == 0) ? actualSeqNumber + 2 : actualSeqNumber + 1;
            }
        }
    }

    private void testReadWithException(DurableDataLog dataLog, Serializer<TestLogItem> serializer, Predicate<Throwable> exceptionVerifier) throws Exception {
        try (DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, serializer, CONTAINER_ID)) {
            boolean encounteredException = false;
            while (true) {
                DataFrameRecord<TestLogItem> dataFrameRecord;

                try {
                    dataFrameRecord = reader.getNext();

                    // We are expecting an exception at all times (the catch block will verify the correctness of the exception thrown).
                    Assert.fail("Expected an exception but none got thrown.");
                } catch (Exception ex) {
                    Throwable realException = Exceptions.unwrap(ex);

                    //Verify we were really expecting this exception.
                    if (encounteredException) {
                        // We've already encountered a read exception. Verify we cannot read anymore.
                        Assert.assertTrue("Wrong exception type (expecting ObjectClosedException). " + realException, realException instanceof ObjectClosedException);
                        break;
                    } else {
                        // First time we see an exception. Verify it's a Data Corruption Exception.
                        boolean isValidException = exceptionVerifier.test(realException);
                        Assert.assertTrue("Wrong exception: " + realException, isValidException);
                        encounteredException = true;
                        continue; // We need to verify we cannot read anymore; we'll do that in the next loop iteration.                    }
                    }
                }

                if (dataFrameRecord == null) {
                    Assert.fail("Reached the end of the log and no exceptions were detected.");
                    break;
                }
            }
        }
    }

    private void checkReadResult(List<TestLogItem> expectedItems, Collection<Integer> knownBadIndices, List<TestLogItem> actualItems) {
        Assert.assertEquals("Unexpected number of items read.", expectedItems.size() - knownBadIndices.size(), actualItems.size());
        int actualIndex = 0;
        for (int i = 0; i < expectedItems.size(); i++) {
            if (knownBadIndices.contains(i)) {
                // Skip over the known bad item.
                continue;
            }

            TestLogItem expected = expectedItems.get(i);
            TestLogItem actual = actualItems.get(actualIndex);
            Assert.assertEquals("Unexpected Sequence Number.", expected.getSequenceNumber(), actual.getSequenceNumber());
            Assert.assertArrayEquals(String.format("Unexpected read data (Sequence Number = %d).", expected.getSequenceNumber()), expected.getData(), actual.getData());
            actualIndex++;
        }
    }

    private ArrayList<TestLogItem> readAll(DataFrameReader<TestLogItem> reader) throws Exception {
        ArrayList<TestLogItem> result = new ArrayList<>();
        long lastDataFrameSequence = -1;
        boolean expectDifferentDataFrameSequence = true;
        while (true) {
            // Fetch the next operation.
            DataFrameRecord<TestLogItem> dataFrameRecord = reader.getNext();
            if (dataFrameRecord == null) {
                // We have reached the end.
                break;
            }

            // Check the monotonicity of the DataFrameSequence. If we encountered a DataFrameRecord with the flag isLastFrameEntry,
            // then we must ensure the DataFrameSequence changes (increases).
            if (expectDifferentDataFrameSequence) {
                AssertExtensions.assertGreaterThan("Expecting a different (and larger) DataFrameSequence.", lastDataFrameSequence, dataFrameRecord.getLastUsedDataFrameAddress().getSequence());
                expectDifferentDataFrameSequence = false;
            } else {
                AssertExtensions.assertGreaterThanOrEqual("Expecting a increasing (or equal) DataFrameSequence.", lastDataFrameSequence, dataFrameRecord.getLastUsedDataFrameAddress().getSequence());
            }

            if (dataFrameRecord.isLastFrameEntry()) {
                expectDifferentDataFrameSequence = true;
            }

            result.add(dataFrameRecord.getItem());
        }

        return result;
    }
}
