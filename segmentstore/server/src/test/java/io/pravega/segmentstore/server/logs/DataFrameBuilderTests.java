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
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

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
    private static final int APPEND_DELAY_MILLIS = 1;
    private static final int RECORD_COUNT = 200;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the happy case: append a set of LogItems, and make sure that frames that get output contain all of them.
     * For this test, there is no delay in the DurableDataLog append implementations - it is as close to sync as possible.
     */
    @Test
    public void testAppendNoFailureNoDelay() throws Exception {
        testAppendNoFailure(0);
    }

    /**
     * Tests the happy case: append a set of LogItems, and make sure that frames that get output contain all of them.
     * For this test, there is no delay in the DurableDataLog append implementations - it is as close to sync as possible.
     */
    @Test
    public void testAppendNoFailureWithDelay() throws Exception {
        testAppendNoFailure(APPEND_DELAY_MILLIS);
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

        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        // Have every other 'failEvery' record fail after writing 90% of itself.
        for (int i = 0; i < records.size(); i += failEvery) {
            records.get(i).failSerializationAfterComplete(0.9, new IOException("intentional " + i));
        }
        HashSet<Integer> failedIndices = new HashSet<>();

        val order = new HashMap<DataFrameBuilder.CommitArgs, Integer>();
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            List<DataFrameBuilder.CommitArgs> commitFrames = Collections.synchronizedList(new ArrayList<>());
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(DataFrameTestHelpers.appendOrder(order), commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
                for (int i = 0; i < records.size(); i++) {
                    try {
                        b.append(records.get(i));
                    } catch (IOException ex) {
                        failedIndices.add(i);
                    }
                }
            }
            // Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            await(() -> commitFrames.size() >= order.size(), 20);

            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));
            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());

            // Check the correctness of the commit callback.
            AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, commitFrames.size());
            AssertExtensions.assertGreaterThan("Not enough LogItems were failed.", records.size() / failEvery, failedIndices.size());

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

        List<TestLogItem> records = DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        @Cleanup
        TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService());
        dataLog.initialize(TIMEOUT);

        val asyncInjector = new ErrorInjector<Exception>(count -> count >= failAt, IntentionalException::new);
        dataLog.setAppendErrorInjectors(null, asyncInjector);

        // lastCommitIndex is an index inside the records array that indicate what we think we have committed so far.
        // We may use the array index interchangeably with the LogItem.SequenceNumber. The only reason this works
        // is because the array index equals the LogItem.SequenceNumber - this simplifies things a lot.
        AtomicInteger lastCommitIndex = new AtomicInteger(-1);
        AtomicInteger failCount = new AtomicInteger();
        List<DataFrameBuilder.CommitArgs> successCommits = Collections.synchronizedList(new ArrayList<>());
        Consumer<DataFrameBuilder.CommitArgs> commitCallback = cc -> {
            successCommits.add(cc);
            synchronized (lastCommitIndex) {
                lastCommitIndex.set(Math.max(lastCommitIndex.get(), (int) cc.getLastFullySerializedSequenceNumber()));
            }
        };

        // Keep a reference to the builder (once created) so we can inspect its failure cause).
        val builderRef = new AtomicReference<DataFrameBuilder>();
        val attemptCount = new AtomicInteger();
        BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) -> {
            attemptCount.decrementAndGet();

            // Check that we actually did want an exception to happen.
            Throwable expectedError = ExceptionHelpers.getRealException(asyncInjector.getLastCycleException());
            Assert.assertNotNull("An error happened but none was expected: " + ex, expectedError);
            Throwable actualError = ExceptionHelpers.getRealException(ex);
            if (!(ex instanceof ObjectClosedException)) {
                // First failure.
                Assert.assertEquals("Unexpected error occurred upon commit.", expectedError, actualError);
            }

            if (builderRef.get().failureCause() != null) {
                checkFailureCause(builderRef.get(), ce -> ce instanceof IntentionalException);
            }

            failCount.incrementAndGet();
        };

        val args = new DataFrameBuilder.Args(ca -> attemptCount.incrementAndGet(), commitCallback, errorCallback, executorService());
        try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
            builderRef.set(b);
            try {
                for (val r : records) {
                    b.append(r);
                }

                b.close();
            } catch (ObjectClosedException ex) {
                attemptCount.decrementAndGet();
                await(() -> b.failureCause() != null, 20);

                // If DataFrameBuilder is closed, then we must have had an exception thrown via the callback before.
                Assert.assertNotNull("DataFrameBuilder is closed, yet failure cause is not set yet.", b.failureCause());
                checkFailureCause(b, ce -> ce instanceof IntentionalException);
            }
        }

        await(() -> successCommits.size() >= attemptCount.get(), 20);

        // Read all committed items.
        val reader = new DataFrameReader<TestLogItem>(dataLog, new TestLogItemFactory(), CONTAINER_ID);
        val readItems = new ArrayList<TestLogItem>();
        DataFrameReader.ReadResult<TestLogItem> readItem;
        while ((readItem = reader.getNext()) != null) {
            readItems.add(readItem.getItem());
        }

        val expectedItems = records.stream().filter(r -> r.getSequenceNumber() <= lastCommitIndex.get()).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Items read back do not match expected values.", expectedItems, readItems, TestLogItem::equals);

        // Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
        List<DataFrame> frames = dataLog.getAllEntries(ri -> new DataFrame(ri.getPayload(), ri.getLength()));

        // Check the correctness of the commit callback.
        AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, frames.size());
        Assert.assertEquals("Unexpected number of frames generated.", successCommits.size(), frames.size());
    }

    private void checkFailureCause(DataFrameBuilder builder, Predicate<Throwable> exceptionTester) {
        Throwable causingException = builder.failureCause();
        Assert.assertTrue("Unexpected failure cause for DataFrameBuilder: " + builder.failureCause(),
                exceptionTester.test(causingException));
    }

    /**
     * Tests the flush() method.
     */
    @Test
    public void testFlush() throws Exception {
        testWithAction(DataFrameBuilder::flush);
    }

    /**
     * Tests the fact that, upon calling close() on DataFrameBuilder, it auto-flushes all its contents.
     * This may already be covered in the other cases, but it makes sense to explicitly test it.
     */
    @Test
    public void testClose() throws Exception {
        testWithAction(DataFrameBuilder::close);
    }

    private void testWithAction(Consumer<DataFrameBuilder> action) throws Exception {
        // Append two records, make sure they are not flushed, close the Builder, then make sure they are flushed.
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);

            ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
            List<DataFrameBuilder.CommitArgs> commitFrames = Collections.synchronizedList(new ArrayList<>());
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, commitFrames::add, errorCallback, executorService());

            @Cleanup
            DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args);
            for (TestLogItem item : records) {
                b.append(item);
            }

            // Check the correctness of the commit callback.
            Assert.assertEquals("A Data Frame was generated but none was expected yet.", 0, commitFrames.size());

            // Invoke custom action.
            action.accept(b);

            // Wait for all the frames commit callbacks to be invoked.
            await(() -> commitFrames.size() >= 1, 20);

            // Check the correctness of the commit callback (after closing the builder).
            Assert.assertEquals("Exactly one Data Frame was expected so far.", 1, commitFrames.size());

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));
            Assert.assertEquals("Unexpected number of frames generated.", commitFrames.size(), frames.size());
            DataFrameTestHelpers.checkReadRecords(frames, records, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }

    private void testAppendNoFailure(int delayMillis) throws Exception {
        // Happy case: append a bunch of data, and make sure the frames that get output contain it.
        ArrayList<TestLogItem> records = DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, SMALL_RECORD_MIN_SIZE, SMALL_RECORD_MAX_SIZE, 0);
        records.addAll(DataFrameTestHelpers.generateLogItems(RECORD_COUNT / 2, LARGE_RECORD_MIN_SIZE, LARGE_RECORD_MAX_SIZE, records.size()));

        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, delayMillis, executorService())) {
            dataLog.initialize(TIMEOUT);

            val order = new HashMap<DataFrameBuilder.CommitArgs, Integer>();
            List<DataFrameBuilder.CommitArgs> commitFrames = Collections.synchronizedList(new ArrayList<>());
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(DataFrameTestHelpers.appendOrder(order), commitFrames::add, errorCallback, executorService());
            try (DataFrameBuilder<TestLogItem> b = new DataFrameBuilder<>(dataLog, args)) {
                for (TestLogItem item : records) {
                    b.append(item);
                }

                b.close();
            }

            // Wait for all the frames commit callbacks to be invoked. Even though the DataFrameBuilder waits (upon close)
            // for the OrderedItemProcessor to finish, there are other callbacks chained that need to be completed (such
            // as the one collecting frames in the list above).
            await(() -> commitFrames.size() >= order.size(), delayMillis);

            // It is quite likely that acks will arrive out of order. The DataFrameBuilder has no responsibility for
            // rearrangement; that should be done by its user.
            commitFrames.sort(Comparator.comparingInt(order::get));

            // Check the correctness of the commit callback.
            AssertExtensions.assertGreaterThan("Not enough Data Frames were generated.", 1, commitFrames.size());
            DataFrameBuilder.CommitArgs previousCommitArgs = null;
            for (val ca : commitFrames) {
                if (previousCommitArgs != null) {
                    AssertExtensions.assertGreaterThanOrEqual("CommitArgs.getLastFullySerializedSequenceNumber() is not monotonically increasing.",
                            previousCommitArgs.getLastFullySerializedSequenceNumber(), ca.getLastFullySerializedSequenceNumber());
                    AssertExtensions.assertGreaterThanOrEqual("CommitArgs.getLastStartedSequenceNumber() is not monotonically increasing.",
                            previousCommitArgs.getLastStartedSequenceNumber(), ca.getLastStartedSequenceNumber());

                    AssertExtensions.assertGreaterThanOrEqual("CommitArgs.getLogAddress() is not monotonically increasing.",
                            previousCommitArgs.getLogAddress().getSequence(), ca.getLogAddress().getSequence());
                }

                previousCommitArgs = ca;
            }

            //Read all entries in the Log and interpret them as DataFrames, then verify the records can be reconstructed.
            List<DataFrame> frames = dataLog.getAllEntries(readItem -> new DataFrame(readItem.getPayload(), readItem.getLength()));
            DataFrameTestHelpers.checkReadRecords(frames, records, r -> new ByteArraySegment(r.getFullSerialization()));
        }
    }

    private void await(Supplier<Boolean> condition, int checkFrequencyMillis) throws TimeoutException {
        long remainingMillis = TIMEOUT.toMillis();
        while (!condition.get() && remainingMillis > 0) {
            Exceptions.handleInterrupted(() -> Thread.sleep(checkFrequencyMillis));
            remainingMillis -= checkFrequencyMillis;
        }

        if (!condition.get() && remainingMillis <= 0) {
            throw new TimeoutException("Timeout expired prior to the condition becoming true.");
        }
    }
}
