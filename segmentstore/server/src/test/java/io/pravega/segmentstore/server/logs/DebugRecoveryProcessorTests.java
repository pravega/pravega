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


import io.pravega.common.function.Callbacks;
import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.BiConsumer;

/**
 * Unit tests for the DebugRecoveryProcessor class.
 *
 * */
public class DebugRecoveryProcessorTests extends ThreadPooledTestSuite {

    protected static final Duration TIMEOUT = Duration.ofMillis(30000);
    private static final int CONTAINER_ID = 1234567;
    private static final int FRAME_SIZE = 512;
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
        StreamSegmentTruncateOperation tro = new StreamSegmentTruncateOperation(12, 12345);
        tro.setSequenceNumber(2);
        //insert duplicates
        records.add(tro);
        records.add(tro);

        //append the duplicate reocrds to the DurableDataLog
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
                    null,
                    op -> false,
                    null,
                    null);

            //use the DebugRecoveryProcessor to recover the operations
            DebugRecoveryProcessor drp = DebugRecoveryProcessor.create(CONTAINER_ID, dataLog,  ContainerConfig.builder().build(),
                    ReadIndexConfig.builder().build(), executorService(), callbacks, false);
            // must not throw exception, errorOnDataCorruption is false
            drp.performRecovery();

            final DebugRecoveryProcessor drp2 = DebugRecoveryProcessor.create(CONTAINER_ID, dataLog,  ContainerConfig.builder().build(),
                    ReadIndexConfig.builder().build(), executorService(), callbacks, true);
            //must throw datacorruption exception, errorOnDataCorruption is true
            AssertExtensions.assertThrows("Should have thrown DataCorruptionException for duplicate entries", () -> drp2.performRecovery(), ex -> ex instanceof DataCorruptionException);
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
