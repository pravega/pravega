/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for DataFrameBuilder class.
 */
public class DataFrameBuilderTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1234567;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SMALL_RECORD_MIN_SIZE = 0;
    private static final int SMALL_RECORD_MAX_SIZE = 128;
    private static final int LARGE_RECORD_MIN_SIZE = 1024;
    private static final int LARGE_RECORD_MAX_SIZE = 10240;
    private static final int FRAME_SIZE = 512;
    private static final int DEFAULT_WRITE_CAPACITY = 1;

    /**
     * Tests the happy case: append a set of LogItems, and make sure that frames that get output contain all of them.
     */
    @Test
    public void testAppendNoFailure() throws Exception {
        // Happy case: append a bunch of data, and make sure the frames that get output contain it.
        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(DEFAULT_WRITE_CAPACITY, commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
                for (TestLogItem item : records) {
                    b.append(item);
                }
            }

            // Check the correctness of the commit callback.
            AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, commitFrames.size());
            DataFrameBuilder.DataFrameCommitArgs previousCommitArgs = null;
            for (int i = 0; i < commitFrames.size(); i++) {
                DataFrameBuilder.DataFrameCommitArgs ca = commitFrames.get(i);
                if (previousCommitArgs != null) {
                    AssertExtensions.assertGreaterThanOrEqual("DataFrameCommitArgs.getLastFullySerializedSequenceNumber() is not monotonically increasing.",
                            previousCommitArgs.getLastFullySerializedSequenceNumber(), ca.getLastFullySerializedSequenceNumber());
                    AssertExtensions.assertGreaterThanOrEqual("DataFrameCommitArgs.getLastStartedSequenceNumber() is not monotonically increasing.",
                            previousCommitArgs.getLastStartedSequenceNumber(), ca.getLastStartedSequenceNumber());
                }

                previousCommitArgs = ca;
            }

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));
            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }

    /**
     * Tests the case when the appends fail because of Serialization failures.
     * Serialization errors should only affect the append that caused it. It should not cause any data to be dropped
     * or put the DataFrameBuilder in a stuck state.
     * This should be done both with large and with small LogItems. Large items span multiple frames.
     */
    @Test
    public void testAppendWithSerializationFailure() throws Exception {
        int failEvery = 7; // Fail every X records.

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        // Have every other 'failEvery' record fail after writing 90% of itself.
        for (int i = 0; i < records.size(); i += failEvery) {
            records.get(i).failSerializationAfterComplete(0.9, new IOException("intentional " + i));
        }
        HashSet<Integer> failedIndices = new HashSet<>();

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(DEFAULT_WRITE_CAPACITY, commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
                for (int i = 0; i < records.size(); i++) {
                    try {
                        b.append(records.get(i));
                    } catch (IOException ex) {
                        failedIndices.add(i);
                    }
                }
            }

            // Check the correctness of the commit callback.
            AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, commitFrames.size());
            AssertExtensions.assertGreaterThan("Not enough LogItems were failed.", records.size() / failEvery, failedIndices.size());

            // Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));

            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, failedIndices, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }

    /**
     * Tests the case when the DataLog fails to commit random frames.
     * Commit errors should affect only the LogItems that were part of it. It should cause data to be dropped
     * and affected appends failed.
     * This should be done both with large and with small LogItems. Large items span multiple frames.
     */
    @Test
    public void testAppendWithCommitFailure() throws Exception {
        int failAt = 7; // Fail the commit to DurableDataLog after this many writes.

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        HashSet<Integer> failedIndices = new HashSet<>();
        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService());
        dataLog.initialize(TIMEOUT);

        val asyncInjector = new ErrorInjector<Exception>(count -> count == failAt, IntentionalException::new);
        dataLog.setAppendErrorInjectors(null, asyncInjector);

        // lastCommitIndex & lastAttemptIndex are indices inside the records array that indicate what we think
        // we have committed and what we have not.
        // We may use the array index interchangeably with the LogItem.SequenceNumber. The only reason this works
        // is because the array index equals the LogItem.SequenceNumber - this simplifies things a lot.
        AtomicInteger lastCommitIndex = new AtomicInteger(-1);
        AtomicInteger lastAttemptIndex = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();
        ArrayList<DataFrameBuilder.DataFrameCommitArgs> successCommits = new ArrayList<>();
        DataFrameBuilder.SuccessCallback commitCallback = cc -> {
            successCommits.add(cc);
            lastCommitIndex.set((int) cc.getLastFullySerializedSequenceNumber());
        };

        AtomicBoolean errorEncountered = new AtomicBoolean(false);
        Consumer<Throwable> errorCallback = ex -> {
            // Check that we actually did want an exception to happen.
            Throwable expectedError = ExceptionHelpers.getRealException(asyncInjector.getLastCycleException());

            Assert.assertNotNull("An error happened but none was expected: " + ex, expectedError);
            Throwable actualError = ExceptionHelpers.getRealException(ex);
            if (errorEncountered.getAndSet(true)) {
                // An error had previously been encountered. Verify that now we throw IllegalState since the DataFrameBuilder
                // is no longer usable.
                Assert.assertTrue("", actualError instanceof IllegalStateException);
                actualError = ExceptionHelpers.getRealException(actualError.getCause());
            }

            Assert.assertEquals("Unexpected error occurred upon commit.", expectedError, actualError);
            failCount.incrementAndGet();

            // Need to indicate that all LogItems since the last one committed until the one currently executing have been failed.
            for (int i = lastCommitIndex.get() + 1; i <= lastAttemptIndex.get(); i++) {
                failedIndices.add(i);
            }
        };

        val args = new DataFrameBuilder.Args(DEFAULT_WRITE_CAPACITY, commitCallback, errorCallback, executorService());
        try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
            for (int i = 0; i < records.size(); i++) {
                try {
                    lastAttemptIndex.set(i);
                    b.append(records.get(i));
                } catch (IOException ex) {
                    failedIndices.add(i);
                }
            }
        }

        // Check the correctness of the commit callback.
        AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, successCommits.size());
        AssertExtensions.assertGreaterThan("Not enough LogItems were failed.", 1, failedIndices.size());

        // Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
        List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));

        Assert.assertEquals("Unexpected number of frames generated.", successCommits.size(), frames.size());
        DataFrameTestHelpers.checkReadRecords(frames, records, failedIndices, r -> new ByteArraySegment(r.getFullSerialization()));
    }

    /**
     * Tests the fact that, upon calling close() on DataFrameBuilder, it auto-flushes all its contents.
     * This may already be covered in the other cases, but it makes sense to explicitly test it.
     */
    @Test
    public void testClose() throws Exception {
        // Append two records, make sure they are not flushed, close the Builder, then make sure they are flushed.
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(DEFAULT_WRITE_CAPACITY, commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
                for (TestLogItem item : records) {
                    b.append(item);
                }

                // Check the correctness of the commit callback.
                Assert.assertEquals("A Data Frame was generated but none was expected yet.", 0, commitFrames.size());
            }

            // Check the correctness of the commit callback (after closing the builder).
            Assert.assertEquals("Exactly one Data Frame was expected so far.", 1, commitFrames.size());

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));
            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }
}
