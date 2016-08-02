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

package com.emc.pravega.service.server.logs;

import com.emc.nautilus.common.util.ByteArraySegment;
import com.emc.nautilus.common.function.ConsumerWithException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.TestDurableDataLog;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.emc.nautilus.testcommon.ErrorInjector;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Unit tests for DataFrameBuilder class.
 */
public class DataFrameBuilderTests {
    private static final String CONTAINER_ID = "TestContainer";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SMALL_RECORD_MIN_SIZE = 0;
    private static final int SMALL_RECORD_MAX_SIZE = 128;
    private static final int LARGE_RECORD_MIN_SIZE = 1024;
    private static final int LARGE_RECORD_MAX_SIZE = 10240;
    private static final int FRAME_SIZE = 512;

    /**
     * Tests the happy case: append a set of LogItems, and make sure that frames that get output contain all of them.
     *
     * @throws Exception
     */
    @Test
    public void testAppendNoFailure() throws Exception {
        // Happy case: append a bunch of data, and make sure the frames that get output contain it.
        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE)) {
            dataLog.initialize(TIMEOUT);

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
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
                    AssertExtensions.assertGreaterThanOrEqual("DataFrameCommitArgs.getLastFullySerializedSequenceNumber() is not monotonically increasing.", previousCommitArgs.getLastFullySerializedSequenceNumber(), ca.getLastFullySerializedSequenceNumber());
                    AssertExtensions.assertGreaterThanOrEqual("DataFrameCommitArgs.getLastStartedSequenceNumber() is not monotonically increasing.", previousCommitArgs.getLastStartedSequenceNumber(), ca.getLastStartedSequenceNumber());
                }

                previousCommitArgs = ca;
            }

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload()));
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

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE)) {
            dataLog.initialize(TIMEOUT);

            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
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
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload()));

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
        int failSyncEvery = 7; // Fail synchronously every X DataFrames.
        int failAsyncEvery = 11; // Fail async every X DataFrames.

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(100, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(100, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        HashSet<Integer> failedIndices = new HashSet<>();
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE)) {
            dataLog.initialize(TIMEOUT);

            ErrorInjector<Exception> syncErrorInjector = new ErrorInjector<>(
                    count -> count % failSyncEvery == 0,
                    () -> new Exception("intentional sync"));
            ErrorInjector<Exception> asyncErrorInjector = new ErrorInjector<>(
                    count -> count % failAsyncEvery == 0,
                    () -> new Exception("intentional async"));
            dataLog.setAppendErrorInjectors(syncErrorInjector, asyncErrorInjector);

            // lastCommitIndex & lastAttemptIndex are indices inside the records array that indicate what we think
            // we have committed and what we have not.
            // We may use the array index interchangeably with the LogItem.SequenceNumber. The only reason this works
            // is because the array index equals the LogItem.SequenceNumber - this simplifies things a lot.
            AtomicInteger lastCommitIndex = new AtomicInteger(-1);
            AtomicInteger lastAttemptIndex = new AtomicInteger();
            AtomicInteger failCount = new AtomicInteger();
            ArrayList<DataFrameBuilder.DataFrameCommitArgs> successCommits = new ArrayList<>();
            ConsumerWithException<DataFrameBuilder.DataFrameCommitArgs, Exception> commitCallback = cc -> {
                successCommits.add(cc);
                lastCommitIndex.set((int) cc.getLastFullySerializedSequenceNumber());
            };

            Consumer<Throwable> errorCallback = ex -> {
                // Check that we actually did want an exception to happen.
                Throwable expectedError = ErrorInjector.getLastCycleException(syncErrorInjector, asyncErrorInjector);
                expectedError = ExceptionHelpers.getRealException(expectedError);

                Assert.assertNotNull(String.format("Unexpected error occurred upon commit. %s", ex), expectedError);
                Assert.assertEquals("Unexpected error occurred upon commit.", expectedError, ExceptionHelpers.getRealException(ex));
                failCount.incrementAndGet();

                // Need to indicate that all LogItems since the last one committed until the one currently executing have been failed.
                for (int i = lastCommitIndex.get() + 1; i <= lastAttemptIndex.get(); i++) {
                    failedIndices.add(i);
                }
            };

            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitCallback, errorCallback)) {
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
            AssertExtensions.assertGreaterThan("Not enough LogItems were failed.", records.size() / Math.max(failAsyncEvery, failSyncEvery), failedIndices.size());

            // Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload()));

            Assert.assertEquals("Unexpected number of frames generated.", successCommits.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, failedIndices, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }

    /**
     * Tests the fact that, upon calling close() on DataFrameBuilder, it auto-flushes all its contents.
     * This may already be covered in the other cases, but it makes sense to explicitly test it.
     */
    @Test
    public void testClose() throws Exception {
        // Append two records, make sure they are not flushed, close the Builder, then make sure they are flushed.
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE)) {
            dataLog.initialize(TIMEOUT);

            ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
            ArrayList<DataFrameBuilder.DataFrameCommitArgs> commitFrames = new ArrayList<>();
            Consumer<Throwable> errorCallback = ex -> Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, commitFrames::add, errorCallback)) {
                for (TestLogItem item : records) {
                    b.append(item);
                }

                // Check the correctness of the commit callback.
                Assert.assertEquals("A Data Frame was generated but none was expected yet.", 0, commitFrames.size());
            }

            // Check the correctness of the commit callback (after closing the builder).
            Assert.assertEquals("Exactly one Data Frame was expected so far.", 1, commitFrames.size());

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload()));
            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }
}
