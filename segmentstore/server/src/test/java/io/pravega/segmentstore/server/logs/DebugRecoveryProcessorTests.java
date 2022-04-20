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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Unit tests for the DebugRecoveryProcessor class.
 *
 * */
public class DebugRecoveryProcessorTests extends ThreadPooledTestSuite {

    protected static final Duration TIMEOUT = Duration.ofMillis(30000);
    private static final int CONTAINER_ID = 1234567;
    private static final int FRAME_SIZE = 512;
    private static final int SMALL_RECORD_MIN_SIZE = 0;
    private static final int SMALL_RECORD_MAX_SIZE = 128;
    private static final int LARGE_RECORD_MIN_SIZE = 1024;
    private static final int LARGE_RECORD_MAX_SIZE = 10240;
    private static final Serializer<TestLogItem> SERIALIZER = new TestLogItem.TestLogItemSerializer();
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests recovery with data corruption, that is there being duplicate operations
     * present in the DurableDataLog. The actual functionality is primarily used
     * by DurableDataLogRepairCommand, which can be used to edit the underlying DurableDataLog
     * and repair such situations. This test basically makes sure that we do not throw
     * the DataCorruptionException during the repair process, to help finish the repair.
     * @throws Exception
     */
    @Test
    public void testRecoveryWithDataCorruption() throws Exception {

        ArrayList<Operation> records = new ArrayList<>();
        StreamSegmentTruncateOperation tro1 = new StreamSegmentTruncateOperation(12, 123);
        tro1.setSequenceNumber(2);
        MetadataCheckpointOperation chop = new MetadataCheckpointOperation();
        chop.setContents(new ByteArraySegment(new byte[100]));
        chop.setSequenceNumber(3);
        StreamSegmentTruncateOperation tro2 = new StreamSegmentTruncateOperation(12, 12345);
        tro2.setSequenceNumber(4);
        records.add(tro1);
        records.add(chop);
        records.add(tro2);
        records.add(tro2);

        // Append the duplicate records to the DurableDataLog
        try (TestDurableDataLog dataLog = TestDurableDataLog.create(CONTAINER_ID, FRAME_SIZE, executorService())) {
            dataLog.initialize(TIMEOUT);
            BiConsumer<Throwable, DataFrameBuilder.CommitArgs> errorCallback = (ex, a) ->
                    Assert.fail(String.format("Unexpected error occurred upon commit. %s", ex));
            val args = new DataFrameBuilder.Args(Callbacks::doNothing, Callbacks::doNothing, errorCallback, executorService());
            try (TestDataFrameBuilder<Operation> b = new TestDataFrameBuilder<>(dataLog, OperationSerializer.DEFAULT, args)) {
                for (Operation r : records) {
                    b.append(r);
                }
                b.flush();
            }
            val callbacks = new DebugRecoveryProcessor.OperationCallbacks(
                    new BiConsumer<Operation, List<DataFrameRecord.EntryInfo>>() {
                        @Override
                        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> entryInfos) {
                        }
                    },
                    op -> false,
                    op -> { },
                    (op, ex) -> { });

            // Use the DebugRecoveryProcessor to recover the operations
            DebugRecoveryProcessor drp = DebugRecoveryProcessor.create(CONTAINER_ID, dataLog,  ContainerConfig.builder().build(),
                    ReadIndexConfig.builder().build(), executorService(), callbacks, false);
            // Must not throw exception, errorOnDataCorruption is false
            drp.performRecovery();

            final DebugRecoveryProcessor drp2 = DebugRecoveryProcessor.create(CONTAINER_ID, dataLog,  ContainerConfig.builder().build(),
                    ReadIndexConfig.builder().build(), executorService(), callbacks, true);
            // Must throw data-corruption exception, errorOnDataCorruption is true
            AssertExtensions.assertThrows("Should have thrown DataCorruptionException for duplicate entries", () -> drp2.performRecovery(), ex -> ex instanceof DataCorruptionException);
            drp.close();
            drp2.close();
        }
    }

    /**
     * Tests the case when the DebugDataFrameReader reads from a log and it encounters log read failures.
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
            try (TestDataFrameBuilder<TestLogItem> b = new TestDataFrameBuilder<>(dataLog, SERIALIZER, args)) {
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

    private void testReadWithException(DurableDataLog dataLog, Serializer<TestLogItem> serializer, Predicate<Throwable> exceptionVerifier) throws Exception {
        try (DataFrameReader<TestLogItem> reader = new DebugDataFrameReader<>(dataLog, serializer, CONTAINER_ID, false)) {
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

    private static class TestDataFrameBuilder<T extends SequencedElement> implements AutoCloseable {

        private final DataFrameOutputStream outputStream;
        private final DurableDataLog targetLog;
        private final Serializer<T> serializer;
        private final DataFrameBuilder.Args args;

        public TestDataFrameBuilder(DurableDataLog targetLog, Serializer<T> serializer, DataFrameBuilder.Args args) {
          this.targetLog = targetLog;
          this.serializer = serializer;
          this.args = args;
          this.outputStream = new DataFrameOutputStream(targetLog.getWriteSettings().getMaxWriteLength(), this::handleDataFrameComplete);
        }

        private void handleDataFrameComplete(DataFrame dataFrame) {
            this.targetLog.append(dataFrame.getData(), this.args.writeTimeout)
                    .thenAcceptAsync(logAddress -> {
                    }, this.args.executor)
                    .exceptionally(ex -> {
                        throw new RuntimeException("Error during dataFrameComplete");
                    });
        }

        public void append(T logItem) throws Exception {
            long seqNo = logItem.getSequenceNumber();
            try {
                // Indicate to the output stream that are about to write a new record.
                this.outputStream.startNewRecord();
                // Completely serialize the entry. Note that this may span more than one Data Frame.
                this.serializer.serialize(this.outputStream, logItem);
                // Indicate to the output stream that have finished writing the record.
                this.outputStream.endRecord();
            } catch (Exception ex) {
                throw ex;
            }
        }

        public void flush() {
            this.outputStream.flush();
        }

        @Override
        public void close() throws Exception {
        }
    }
}
