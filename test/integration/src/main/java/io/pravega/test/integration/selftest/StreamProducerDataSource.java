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
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;

/**
 * Represents a Data Source for all Producers of the SelfTest that affect Streams.
 */
class StreamProducerDataSource extends ProducerDataSource<Event> {
    //region Members

    private final ConcurrentHashMap<String, EventGenerator> eventGenerators;
    private final Random appendSizeGenerator;
    private final boolean sealSupported;
    private final boolean transactionsSupported;
    private final boolean appendSupported;
    @GuardedBy("lock")
    private int lastCreatedTransaction;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    StreamProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        super(config, state, store);
        this.eventGenerators = new ConcurrentHashMap<>();
        this.appendSizeGenerator = RandomFactory.create();
        this.lastCreatedTransaction = 0;
        this.appendSupported = this.store.isFeatureSupported(StoreAdapter.Feature.Append);
        this.transactionsSupported = this.store.isFeatureSupported(StoreAdapter.Feature.Transaction);
        this.sealSupported = this.store.isFeatureSupported(StoreAdapter.Feature.SealStream);
    }

    //endregion

    //region Operation Generation

    /**
     * Generates the next ProducerOperation to be executed. Based on the current TestState, this operation can be:
     * * CreateTransaction: If no CreateTransaction happened for a while (based on the TestConfig).
     * * MergeTransaction: If a transaction has exceeded its maximum number of appends (based on the TestConfig).
     * * Event: If any of the above are not met (this represents the majority of cases).
     *
     * @return The next ProducerOperation, or null if not such operation can be generated (we reached the end of the test).
     */
    @Override
    ProducerOperation<Event> nextOperation(int producerId) {
        int operationIndex = this.state.newOperation();
        ProducerOperation<Event> result = null;
        synchronized (this.lock) {
            if (operationIndex > this.config.getOperationCount()) {
                // We have reached the end of the test. We need to seal all Streams before we stop.
                val si = this.state.getStream(s -> !s.isClosed());
                if (si != null) {
                    if (si.isTransaction()) {
                        // Transactions can't be Sealed - abort them instead.
                        result = new ProducerOperation<>(ProducerOperationType.ABORT_STREAM_TRANSACTION, si.getName());
                        result.setWaitOn(si.close());
                    } else if (this.sealSupported) {
                        // Seal the Stream.
                        result = new ProducerOperation<>(ProducerOperationType.STREAM_SEAL, si.getName());
                        result.setWaitOn(si.close());
                    }
                }

                if (result == null) {
                    // If we have nothing else to do, this will return null, which signals the end of the test.
                    return null;
                }
            } else if (this.transactionsSupported) {
                if (operationIndex - this.lastCreatedTransaction >= this.config.getTransactionFrequency()) {
                    // We have exceeded the number of operations since we last created a transaction.
                    result = new ProducerOperation<>(ProducerOperationType.CREATE_STREAM_TRANSACTION, this.state.getNonTransactionStreamName(operationIndex));
                    this.lastCreatedTransaction = operationIndex;
                } else {
                    // If any transaction has already exceeded the max number of appends, then merge it.
                    val si = this.state.getStream(s -> s.isTransaction() && !s.isClosed() && s.getCompletedOperationCount() >= this.config.getMaxTransactionAppendCount());
                    if (si != null) {
                        result = new ProducerOperation<>(ProducerOperationType.MERGE_STREAM_TRANSACTION, si.getName());
                        result.setWaitOn(si.close());
                    }
                }
            }

            // Otherwise append to random segment.
            if (result == null) {
                if (!this.appendSupported) {
                    return null;
                }

                val si = this.state.getStreamOrTransaction(operationIndex);
                si.operationStarted();
                result = new ProducerOperation<>(ProducerOperationType.STREAM_APPEND, si.getName());
            }
        }

        // Check to see if we need to end the warm-up period.
        if (this.state.isWarmup() && operationIndex == this.config.getWarmupCount()) {
            this.state.setWarmup(false);
            synchronized (this.lock) {
                this.lastCreatedTransaction = 0;
            }
        }

        if (result.getType() == ProducerOperationType.STREAM_APPEND) {
            result.setUpdate(generatePayload(result.getTarget(), producerId));
        }

        // Attach operation completion callbacks (both for success and for failure).
        result.setCompletionCallback(this::operationCompletionCallback);
        result.setFailureCallback(this::operationFailureCallback);
        return result;
    }

    /**
     * Generates a new Event (which follows a deterministic pattern and has a routing key).
     */
    private Event generatePayload(String streamName, int producerId) {
        EventGenerator generator = this.eventGenerators.getOrDefault(streamName, null);
        if (generator == null) {
            // If the argument is indeed correct, this segment was deleted between the time the operation got generated
            // and when this method was invoked.
            throw new UnknownTargetException(streamName);
        }

        int maxSize = this.config.getMaxAppendSize();
        int minSize = this.config.getMinAppendSize();
        int size = maxSize;
        if (maxSize != minSize) {
            size = this.appendSizeGenerator.nextInt(maxSize - minSize) + minSize;
        }

        return generator.newEvent(size, producerId);
    }

    private void operationCompletionCallback(ProducerOperation<Event> op) {
        // Record the operation as completed in the State.
        this.state.operationCompleted(op.getTarget());
        this.state.recordDuration(op.getType(), op.getElapsedMillis());

        // OperationType-specific updates.
        if (op.getType() == ProducerOperationType.MERGE_STREAM_TRANSACTION) {
            postStreamDeletion(op.getTarget());
        } else if (op.getType() == ProducerOperationType.CREATE_STREAM_TRANSACTION) {
            Object r = op.getResult();
            if (!(r instanceof String)) {
                TestLogger.log(LOG_ID, "Operation %s completed but has result of wrong type.", op);
                throw new IllegalArgumentException("Completed CreateTransaction operation has result of wrong type.");
            }

            String transactionName = (String) r;
            this.state.recordNewTransaction(transactionName);
            int id;
            synchronized (this.lock) {
                id = -this.lastCreatedTransaction;
            }
            this.eventGenerators.put(transactionName, new EventGenerator(id, false));
        } else if (op.getType() == ProducerOperationType.STREAM_APPEND) {
            this.state.recordDataWritten(op.getLength());
        }
    }

    private void operationFailureCallback(ProducerOperation<Event> op, Throwable ex) {
        // Record the operation as failed in the State.
        this.state.operationFailed(op.getTarget());
    }

    //endregion

    //region Segment Management

    /**
     * Creates all the Streams/Segments required for this test.
     *
     * @return A CompletableFuture that will be completed when all Streams/Segments are created.
     */
    @Override
    CompletableFuture<Void> createAll() {
        Preconditions.checkArgument(this.state.getAllStreams().size() == 0, "Cannot call createAll more than once.");
        ArrayList<CompletableFuture<Void>> creationFutures = new ArrayList<>();

        TestLogger.log(LOG_ID, "Creating Streams.");
        StoreAdapter.Feature.CreateStream.ensureSupported(this.store, "create streams");
        for (int i = 0; i < this.config.getStreamCount(); i++) {
            // Streams names are of the form: Stream<TestId><StreamId> - to avoid clashes between different tests.
            final int streamId = i;
            String name = String.format("Stream%s%s", this.config.getTestId(), streamId);
            creationFutures.add(this.store.createStream(name, this.config.getTimeout())
                    .thenRun(() -> {
                        this.state.recordNewStreamName(name);
                        this.eventGenerators.put(name, new EventGenerator(streamId, true));
                    }));
        }

        return Futures.allOf(creationFutures);
    }

    /**
     * Deletes all the Streams/Segments required for this test.
     */
    @Override
    CompletableFuture<Void> deleteAll() {
        if (!this.store.isFeatureSupported(StoreAdapter.Feature.DeleteStream)) {
            TestLogger.log(LOG_ID, "Not deleting Streams because the store adapter does not support it.");
            return CompletableFuture.completedFuture(null);
        }

        TestLogger.log(LOG_ID, "Deleting Streams.");
        return deleteStreams(this.state.getTransactionNames())
                .thenCompose(v -> deleteStreams(this.state.getAllStreamNames()));
    }

    private CompletableFuture<Void> deleteStreams(Collection<String> segmentNames) {
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            try {
                deletionFutures.add(deleteStream(segmentName));
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex) || !(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }
        }

        return Futures.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteStream(String name) {
        return Futures.exceptionallyExpecting(this.store.deleteStream(name, this.config.getTimeout()),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null)
                .thenRun(() -> postStreamDeletion(name));
    }

    private void postStreamDeletion(String name) {
        this.eventGenerators.remove(name);
        this.state.recordDeletedStream(name);
    }

    //endregion
}
