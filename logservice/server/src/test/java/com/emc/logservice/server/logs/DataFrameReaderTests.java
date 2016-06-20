package com.emc.logservice.server.logs;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.server.*;
import com.emc.logservice.storageabstraction.DataLogNotAvailableException;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.ErrorInjector;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.*;

/**
 * Unit tests for DataFrameReader class.
 */
public class DataFrameReaderTests {
    private static final String ContainerId = "TestContainer";
    private static final Duration Timeout = Duration.ofSeconds(30);
    private static final int SmallRecordMinSize = 0;
    private static final int SmallRecordMaxSize = 128;
    private static final int LargeRecordMinSize = 1024;
    private static final int LargeRecordMaxSize = 10240;
    private static final int FrameSize = 512;

    /**
     * Tests the happy case: DataFrameReader can read from a DataLog when the are no exceptions.
     */
    @Test
    public void testReadsNoFailure() throws Exception {
        int failEvery = 7; // Fail every X records (write-wise).

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SmallRecordMinSize, SmallRecordMaxSize, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LargeRecordMinSize, LargeRecordMaxSize, records.size()));

        // Have every other 'failEvery' record fail after writing 90% of itself.
        for (int i = 0; i < records.size(); i += failEvery) {
            records.get(i).failSerializationAfterComplete(0.9, new IOException("intentional " + i));
        }

        HashSet<Integer> failedIndices = new HashSet<>();
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(ContainerId, FrameSize)) {
            dataLog.initialize(Timeout).join();

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
                for (int i = 0; i < records.size(); i++) {
                    try {
                        b.append(records.get(i));
                    }
                    catch (IOException ex) {
                        failedIndices.add(i);
                    }
                }
            }

            TestLogItemFactory logItemFactory = new TestLogItemFactory();
            DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, logItemFactory, ContainerId);
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

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SmallRecordMinSize, SmallRecordMaxSize, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LargeRecordMinSize, LargeRecordMaxSize, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(ContainerId, FrameSize)) {
            dataLog.initialize(Timeout).join();

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
                for (int i = 0; i < records.size(); i++) {
                    b.append(records.get(i));
                }
            }

            ErrorInjector<SerializationException> errorInjector = new ErrorInjector<>(
                    count -> count % failDeserializationEvery == 0,
                    () -> new SerializationException("intentional", "TestLogItem.deserialize"));

            TestLogItemFactory logItemFactory = new TestLogItemFactory();
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
        int failReadAsyncEvery = 5; // Fail reads asynchronously every X attempts.

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SmallRecordMinSize, SmallRecordMaxSize, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LargeRecordMinSize, LargeRecordMaxSize, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(ContainerId, FrameSize)) {
            dataLog.initialize(Timeout).join();

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
                for (int i = 0; i < records.size(); i++) {
                    b.append(records.get(i));
                }
            }

            TestLogItemFactory logItemFactory = new TestLogItemFactory();

            // Test 1: Initial call to getReader.
            ErrorInjector<Exception> getReaderErrorInjector = new ErrorInjector<>(
                    count -> true, // Fail every time.
                    () -> new DataLogNotAvailableException("intentional getReader exception"));
            dataLog.setReadErrorInjectors(getReaderErrorInjector, null, null);
            AssertExtensions.assertThrows(
                    "No exception or wrong type of exception thrown by getNext() with exception thrown by getReader().",
                    () -> new DataFrameReader<>(dataLog, logItemFactory, ContainerId),
                    ex -> ExceptionHelpers.getRealException(ex) == getReaderErrorInjector.getLastCycleException());

            // Test 2: Sync failures during getNext().
            ErrorInjector<Exception> readSyncErrorInjector = new ErrorInjector<>(
                    count -> count % failReadSyncEvery == 0,
                    () -> new DataLogNotAvailableException("intentional getNext sync exception"));
            dataLog.setReadErrorInjectors(null, readSyncErrorInjector, null);
            testReadWithException(dataLog, logItemFactory, ex -> ex == readSyncErrorInjector.getLastCycleException());

            // Test 3: Async failures during getNext().
            ErrorInjector<Exception> readAsyncErrorInjector = new ErrorInjector<>(
                    count -> count % failReadAsyncEvery == 0,
                    () -> new DataLogNotAvailableException("intentional getNext async exception"));
            dataLog.setReadErrorInjectors(null, null, readAsyncErrorInjector);
            testReadWithException(dataLog, logItemFactory, ex -> ex == readAsyncErrorInjector.getLastCycleException());
        }
    }

    private void testReadWithException(DurableDataLog dataLog, LogItemFactory<TestLogItem> logItemFactory, Predicate<Throwable> exceptionVerifier) throws Exception {
        try (DataFrameReader<TestLogItem> reader = new DataFrameReader<>(dataLog, logItemFactory, ContainerId)) {
            boolean encounteredException = false;
            while (true) {
                DataFrameReader.ReadResult<TestLogItem> readResult;

                try {
                    readResult = reader.getNext(Timeout).join();
                    Assert.assertFalse("getNext() succeeded after read exception was thrown.", encounteredException);
                    Assert.assertNotNull("Expected an exception but none got thrown.");
                }
                catch (Exception ex) {
                    Throwable realException = ExceptionHelpers.getRealException(ex);

                    //Verify we were really expecting this exception.
                    if (encounteredException) {
                        // We've already encountered a read exception. Verify we cannot read anymore.
                        Assert.assertTrue("Wrong exception type (expecting ObjectClosedException). " + realException, realException instanceof ObjectClosedException);
                        break;
                    }
                    else {
                        // First time we see an exception. Verify it's a Data Corruption Exception.
                        boolean isValidException = exceptionVerifier.test(realException);
                        Assert.assertTrue("Wrong exception: " + realException, isValidException);
                        encounteredException = true;
                        continue; // We need to verify we cannot read anymore; we'll do that in the next loop iteration.                    }
                    }
                }

                if (readResult == null) {
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
            DataFrameReader.ReadResult<TestLogItem> readResult = reader.getNext(Timeout).join();
            if (readResult == null) {
                // We have reached the end.
                break;
            }

            // Check the monotonicity of the DataFrameSequence. If we encountered a ReadResult with the flag isLastFrameEntry,
            // then we must ensure the DataFrameSequence changes (increases).
            if (expectDifferentDataFrameSequence) {
                AssertExtensions.assertGreaterThan("Expecting a different (and larger) DataFrameSequence.", lastDataFrameSequence, readResult.getDataFrameSequence());
                expectDifferentDataFrameSequence = false;
            }
            else {
                AssertExtensions.assertGreaterThanOrEqual("Expecting a increasing (or equal) DataFrameSequence.", lastDataFrameSequence, readResult.getDataFrameSequence());
            }

            if (readResult.isLastFrameEntry()) {
                expectDifferentDataFrameSequence = true;
            }

            result.add(readResult.getItem());
        }

        return result;
    }
}
