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
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import io.pravega.test.integration.selftest.adapters.StoreReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Represents an operation consumer. Attaches to a StoreAdapter, listens to tail-reads (on segments), and validates
 * incoming data. Currently this does not do catch-up reads or storage reads/validations.
 */
public class Consumer extends Actor {
    //region Members

    private static final int CATCHUP_READ_COUNT = 10000;
    @Getter
    private final String logId;
    private final String streamName;
    private final TestState testState;
    private final StoreReader reader;
    private final CancellationToken cancellationToken;
    private final ConcurrentHashMap<Integer, Integer> lastSequenceNumbers;
    private final BlockingDrainingQueue<StoreReader.ReadItem> catchupQueue;
    @GuardedBy("storageQueue")
    private final ArrayDeque<StoreReader.ReadItem> storageQueue;
    @GuardedBy("storageQueue")
    private final ArrayDeque<Event> storageReadQueue;
    private final boolean catchupReadsSupported;
    private final boolean storageReadsSupported;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Consumer class.
     *
     * @param streamName     The name of the Stream to monitor.
     * @param config          Test Configuration.
     * @param testState       A TestState representing the current state of the test. This will be used for reporting purposes.
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    Consumer(String streamName, TestConfig config, TestState testState, StoreAdapter store, ScheduledExecutorService executorService) {
        super(config, store, executorService);

        Preconditions.checkArgument(canUseStoreAdapter(store), "StoreAdapter does not support all required features; cannot create a consumer for it.");
        this.logId = String.format("Consumer[%s]", streamName);
        this.streamName = Preconditions.checkNotNull(streamName, "streamName");
        this.testState = Preconditions.checkNotNull(testState, "testState");
        this.reader = store.createReader();
        this.catchupReadsSupported = store.isFeatureSupported(StoreAdapter.Feature.RandomRead);
        this.storageReadsSupported = store.isFeatureSupported(StoreAdapter.Feature.StorageDirect);
        this.cancellationToken = new CancellationToken();
        this.lastSequenceNumbers = new ConcurrentHashMap<>();
        this.catchupQueue = new BlockingDrainingQueue<>();
        this.storageQueue = new ArrayDeque<>();
        this.storageReadQueue = new ArrayDeque<>();
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        return CompletableFuture.allOf(
                processTailReads(),
                processCatchupReads(),
                processStorageReads());
    }

    //endregion

    static boolean canUseStoreAdapter(StoreAdapter storeAdapter) {
        return storeAdapter.isFeatureSupported(StoreAdapter.Feature.TailRead);
    }

    private boolean canRun() {
        return isRunning() && !this.cancellationToken.isCancellationRequested();
    }

    private void validationFailed(ValidationSource source, ValidationResult validationResult) {
        if (source != null) {
            validationResult.setSource(source);
        }

        // Log the event.
        TestLogger.log(this.logId, "VALIDATION FAILED: Segment = %s, %s", this.streamName, validationResult);

        // Then stop the consumer. No point in moving on if we detected a validation failure.
        fail(new ValidationException(this.streamName, validationResult));
    }

    //region Storage Reads

    /**
     * Reads the entire data from Storage for the given Target and validates it, piece by piece.
     */
    private CompletableFuture<Void> processStorageReads() {
        if (!this.storageReadsSupported) {
            return CompletableFuture.completedFuture(null);
        }

        return this.reader
                .readAllStorage(this.streamName, this::processStorageRead, this.cancellationToken)
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (!(ex instanceof StreamSegmentSealedException)) {
                        throw new CompletionException(ex);
                    }
                    return null;
                });
    }

    private void processStorageRead(Event event) {
        val toCompare = new ArrayList<Map.Entry<StoreReader.ReadItem, Event>>();
        synchronized (this.storageQueue) {
            // There's a chance that (due to scheduling reasons), we nay not yet have the Event to compare in memory;
            // if that's the case, then store it, and we'll check it later.
            this.storageReadQueue.addLast(event);

            // Fetch all comparison pairs. Both of these Queues should have elements in the same order, so create the pairs
            // by picking the first element from each.
            while (!this.storageReadQueue.isEmpty() && !this.storageQueue.isEmpty()) {
                toCompare.add(new AbstractMap.SimpleImmutableEntry<>(this.storageQueue.removeFirst(), this.storageReadQueue.removeFirst()));
            }
        }

        // Compare the pairs.
        for (val e : toCompare) {
            ValidationResult validationResult;
            try {
                validationResult = compareReads(e.getKey(), e.getValue());
                this.testState.recordStorageRead(validationResult.getLength());
            } catch (Throwable ex) {
                validationResult = ValidationResult.failed(String.format("General failure: Ex = %s.", ex));
            }

            if (!validationResult.isSuccess()) {
                validationResult.setAddress(e.getKey().getAddress());
                validationFailed(ValidationSource.StorageRead, validationResult);
                break;
            }
        }
    }


    //endregion

    //region Store Reads

    /**
     * Performs Tail & Catchup reads by issuing a long read from the beginning to infinity on the target, processing the
     * output of that read as a 'tail read', then re-issuing concise reads from the store for those offsets.
     */
    private CompletableFuture<Void> processTailReads() {
        return this.reader.readAll(this.streamName, this::processTailEvent, this.cancellationToken)
                          .thenRun(() -> this.catchupQueue.add(new EndItem())); // Signal that we are done.
    }

    private void processTailEvent(StoreReader.ReadItem readItem) {
        ValidationResult validationResult = EventGenerator.validate(readItem.getEvent());
        validationResult.setAddress(readItem.getAddress());
        Event event = readItem.getEvent();
        if (validationResult.isSuccess() && event.getOwnerId() >= 0) {
            // Verify order.
            int currentSequence = event.getSequence();
            Integer prevSequence = this.lastSequenceNumbers.put(event.getRoutingKey(), currentSequence);
            if (prevSequence != null && prevSequence >= currentSequence) {
                validationResult = ValidationResult.failed(String.format(
                        "Out of order events detected. Previous Sequence = %d, Event = '%s'", prevSequence, readItem));
            }
        }

        if (validationResult.isSuccess()) {
            if (this.catchupReadsSupported) {
                this.catchupQueue.add(readItem);
            }

            if (this.storageReadsSupported) {
                synchronized (this.storageQueue) {
                    this.storageQueue.addLast(readItem);
                }
            }

            this.testState.recordTailRead(validationResult.getLength());
            Duration elapsed = validationResult.getElapsed();
            if (elapsed != null) {
                this.testState.recordDuration(ConsumerOperationType.END_TO_END, elapsed.toMillis());
            }
        } else {
            validationFailed(ValidationSource.TailRead, validationResult);
        }
    }

    private CompletableFuture<Void> processCatchupReads() {
        if (!this.catchupReadsSupported) {
            return CompletableFuture.completedFuture(null);
        }

        return Futures.loop(
                this::canRun,
                () -> this.catchupQueue.take(CATCHUP_READ_COUNT)
                        .thenComposeAsync(this::processCatchupReads, this.executorService),
                this.executorService)
                      .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof ObjectClosedException) {
                        // This a normal shutdown, as the catchupQueue is closed when we are done.
                        return null;
                    }

                    throw new CompletionException(ex);
                });
    }

    private CompletableFuture<Void> processCatchupReads(Queue<StoreReader.ReadItem> catchupReads) {
        return Futures.loop(
                () -> !catchupReads.isEmpty(),
                () -> processCatchupRead(catchupReads.poll()),
                this.executorService);
    }

    private CompletableFuture<Void> processCatchupRead(StoreReader.ReadItem toValidate) {
        if (toValidate instanceof EndItem) {
            this.catchupQueue.close();
            return CompletableFuture.completedFuture(null);
        }

        final Timer timer = new Timer();
        return this.reader
                .readExact(this.streamName, toValidate.getAddress())
                .handleAsync((actualRead, ex) -> {
                    ValidationResult validationResult;
                    try {
                        if (ex == null) {
                            validationResult = compareReads(toValidate, actualRead.getEvent());
                            Event e = toValidate.getEvent();
                            this.testState.recordDuration(ConsumerOperationType.CATCHUP_READ, timer.getElapsed().toMillis());
                            this.testState.recordCatchupRead(e.getTotalLength());
                        } else {
                            validationResult = ValidationResult.failed(ex.getMessage());
                        }
                    } catch (Throwable ex2) {
                        validationResult = ValidationResult.failed(String.format("General failure: Ex = %s.", ex2));
                    }

                    if (!validationResult.isSuccess()) {
                        validationResult.setAddress(toValidate.getAddress());
                        validationFailed(ValidationSource.CatchupRead, validationResult);
                    }

                    return null;
                }, this.executorService);
    }

    @SneakyThrows(IOException.class)
    private ValidationResult compareReads(StoreReader.ReadItem expected, Event actualEvent) {
        Event expectedEvent = expected.getEvent();
        String mismatchProperty = null;

        if (expectedEvent.getOwnerId() != actualEvent.getOwnerId()) {
            mismatchProperty = "OwnerId";
        } else if (expectedEvent.getRoutingKey() != actualEvent.getRoutingKey()) {
            mismatchProperty = "RoutingKey";
        } else if (expectedEvent.getSequence() != actualEvent.getSequence()) {
            mismatchProperty = "Sequence";
        } else if (expectedEvent.getContentLength() != actualEvent.getContentLength()) {
            mismatchProperty = "ContentLength";
        } else {
            // Check, byte-by-byte, that the data matches what we expect.
            InputStream expectedData = expectedEvent.getSerialization().getReader();
            InputStream actualData = actualEvent.getSerialization().getReader();
            int readSoFar = 0;
            while (readSoFar < expectedEvent.getTotalLength()) {
                int b1 = expectedData.read();
                int b2 = actualData.read();
                if (b1 != b2) {
                    // This also includes the case when one stream ends prematurely.
                    return ValidationResult.failed(String.format("Corrupted data at Address %s + %d. Expected '%s', found '%s'.", expected.getAddress(), readSoFar, b1, b2));
                }

                readSoFar++;
                if (b1 < 0) {
                    break; // We have reached the end of both streams.
                }
            }
        }

        if (mismatchProperty != null) {
            return ValidationResult.failed(String.format("%s mismatch at Address %s. Expected %s, actual %s.",
                    mismatchProperty, expected.getAddress(), expectedEvent, actualEvent));
        }

        return ValidationResult.success(expectedEvent.getRoutingKey(), expectedEvent.getTotalLength());
    }


    //endregion

    //region EndItem

    private static class EndItem implements StoreReader.ReadItem {
        @Override
        public Event getEvent() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getAddress() {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}