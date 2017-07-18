/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.function.CallbackHelpers;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Represents an operation consumer. Attaches to a StoreAdapter, listens to tail-reads (on segments), and validates
 * incoming data. Currently this does not do catch-up reads or storage reads/validations.
 */
public class Consumer extends Actor {
    //region Members

    private final String logId;
    private final String segmentName;
    private final TestState testState;
    private final StoreReader reader;
    private final CancellationToken cancellationToken;
    private final BlockingDrainingQueue<StoreReader.ReadItem> catchupQueue;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Consumer class.
     *
     * @param segmentName     The name of the Segment to monitor.
     * @param config          Test Configuration.
     * @param dataSource      Data Source.
     * @param testState       A TestState representing the current state of the test. This will be used for reporting purposes.
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    Consumer(String segmentName, TestConfig config, ProducerDataSource dataSource, TestState testState, StoreAdapter store, ScheduledExecutorService executorService) {
        super(config, dataSource, store, executorService);
        Preconditions.checkNotNull(testState, "testState");
        Preconditions.checkArgument(canUseStoreAdapter(store), "StoreAdapter does not support all required features; cannot create a consumer for it.");
        this.logId = String.format("Consumer[%s]", segmentName);
        this.segmentName = segmentName;
        this.testState = testState;
        this.reader = store.createReader();
        this.cancellationToken = new CancellationToken();
        this.catchupQueue = new BlockingDrainingQueue<>();
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        CompletableFuture<Void> tailReadProcessor = processTailReads();
        CompletableFuture<Void> catchupReadProcessor = processCatchupReads();
        CompletableFuture<Void> storageReadProcessor = processStorageReads();
        return CompletableFuture.allOf(tailReadProcessor, catchupReadProcessor, storageReadProcessor);
    }

    @Override
    protected String getLogId() {
        return this.logId;
    }

    //endregion

    static boolean canUseStoreAdapter(StoreAdapter storeAdapter) {
        return storeAdapter.isFeatureSupported(StoreAdapter.Feature.GetInfo)
                && storeAdapter.isFeatureSupported(StoreAdapter.Feature.Read)
                && storeAdapter.isFeatureSupported(StoreAdapter.Feature.StorageDirect);
    }

    private boolean canRun() {
        return isRunning() && !this.cancellationToken.isCancellationRequested();
    }

    private void validationFailed(ValidationSource source, ValidationResult validationResult) {
        if (source != null) {
            validationResult.setSource(source);
        }

        // Log the event.
        TestLogger.log(this.logId, "VALIDATION FAILED: Segment = %s, %s", this.segmentName, validationResult);

        // Then stop the consumer. No point in moving on if we detected a validation failure.
        fail(new ValidationException(this.segmentName, validationResult));
    }

    //region Storage Reads

    //    private CompletableFuture<Void> processStorageReads() {
    //        CompletableFuture<Void> result = new CompletableFuture<>();
    //
    //        // Register an update listener with the storage.
    //        // We use the FutureExecutionSerializer here because we may get new callbacks while the previous one(s) are still
    //        // running, and any overlap in execution will cause repeated reads to the same locations, as well as complications
    //        // with respect to verification and state updates.
    //        val listener = new VerificationStorage.SegmentUpdateListener(
    //                this.segmentName,
    //                (length, sealed) -> this.storageVerificationQueue.queue(() ->
    //                        FutureHelpers.runOrFail(() -> storageSegmentChangedHandler(length, sealed, result), result)));
    //        this.store.getStorageAdapter().registerListener(listener);
    //
    //        // Make sure the listener is closed (and thus unregistered) when we are done, whether successfully or not.
    //        result.whenComplete((r, ex) -> {
    //            listener.close();
    //            if (ex != null) {
    //                fail(ExceptionHelpers.getRealException(ex));
    //            }
    //        });
    //
    //        return result;
    //    }
    //
    //    private CompletableFuture<Void> storageSegmentChangedHandler(long segmentLength, boolean sealed, CompletableFuture<Void> processingFuture) {
    //        if (sealed) {
    //            // We reached the end of the Segment (this callback is the result of a Seal operation), so no point in listening further.
    //            logState(ValidationSource.StorageRead.toString(), "StorageLength=%s, Sealed=True", segmentLength);
    //            processingFuture.complete(null);
    //            return CompletableFuture.completedFuture(null);
    //        }
    //
    //        long segmentStartOffset;
    //        int length;
    //        InputStream expectedData;
    //        synchronized (this.lock) {
    //            // Start from where we left off (no point in re-checking old data).
    //            segmentStartOffset = Math.max(this.storageReadValidatedOffset, this.readBufferSegmentOffset);
    //
    //            // bufferOffset is where in the buffer we start validating at.
    //            int bufferOffset = (int) (segmentStartOffset - this.readBufferSegmentOffset);
    //
    //            // Calculate the amount of data we want to read. Stop either at the segment length or where the tail read validator stopped.
    //            length = (int) (Math.min(segmentLength, this.tailReadValidatedOffset) - segmentStartOffset);
    //
    //            if (length <= 0) {
    //                // Nothing to do (yet).
    //                return CompletableFuture.completedFuture(null);
    //            }
    //
    //            expectedData = this.readBuffer.getReader(bufferOffset, length);
    //        }
    //
    //        // Execute a Storage Read, then validate that the read data matches what was in there.
    //        byte[] storageReadBuffer = new byte[length];
    //        val storage = this.store.getStorageAdapter();
    //        return storage
    //                .openRead(this.segmentName)
    //                .thenComposeAsync(handle -> storage.read(handle, segmentStartOffset, storageReadBuffer, 0, length, this.config.getTimeout()), this.executorService)
    //                .thenAcceptAsync(l -> {
    //                    ValidationResult validationResult = validateStorageRead(expectedData, storageReadBuffer, segmentStartOffset);
    //                    validationResult.setSource(ValidationSource.StorageRead);
    //                    if (!validationResult.isSuccess()) {
    //                        validationFailed(ValidationSource.StorageRead, validationResult);
    //                        return;
    //                    }
    //
    //                    // After a successful validation, update the state and truncate the buffer.
    //                    long diff;
    //                    synchronized (this.lock) {
    //                        diff = segmentStartOffset + length - this.storageReadValidatedOffset;
    //                        this.storageReadValidatedOffset += diff;
    //                        truncateBuffer();
    //                    }
    //
    //                    this.testState.recordStorageRead((int) diff);
    //                    logState(ValidationSource.StorageRead.toString(), "StorageLength=%s", segmentLength);
    //                }, this.executorService)
    //                .thenComposeAsync(v -> {
    //                    long truncateOffset;
    //                    synchronized (this.lock) {
    //                        truncateOffset = Math.min(this.storageReadValidatedOffset, this.catchupReadValidatedOffset);
    //                    }
    //
    //                    return this.store.getStorageAdapter().truncate(this.segmentName, truncateOffset, this.config.getTimeout());
    //                }, this.executorService)
    //                .exceptionally(ex -> {
    //                    processingFuture.completeExceptionally(ex);
    //                    return null;
    //                });
    //    }
    //
    //    private ValidationResult validateStorageRead(InputStream expectedData, byte[] storageReadBuffer, long segmentOffset) {
    //        try {
    //            for (int i = 0; i < storageReadBuffer.length; i++) {
    //                byte expected = (byte) expectedData.read();
    //                if (expected != storageReadBuffer[i]) {
    //                    // This also includes the case when one stream ends prematurely.
    //                    val result = ValidationResult.failed(String.format("Corrupted data at Segment offset %s. Expected '%s', found '%s'.", segmentOffset + i, expected, storageReadBuffer[i]));
    //                    result.setOffset(segmentOffset + i);
    //                    return result;
    //                }
    //            }
    //        } catch (IOException ex) {
    //            throw new UncheckedIOException(ex);
    //        }
    //
    //        return ValidationResult.success(Event.NO_ROUTING_KEY, storageReadBuffer.length);
    //    }

    //endregion

    //region Store Reads

    /**
     * Performs Tail & Catchup reads by issuing a long read from offset 0 to infinity, processing the output
     * of that read as a 'tail read', then re-issuing concise reads from the store for those offsets.
     */
    private CompletableFuture<Void> processTailReads() {
        return this.reader.readAll(this.segmentName, this::processTailEvent, this.cancellationToken);
    }

    private void processTailEvent(StoreReader.ReadItem readItem) {
        // TODO: synchronized

        ValidationResult validationResult = EventGenerator.validate(readItem.getEvent());
        validationResult.setAddress(readItem.getAddress());
        validationResult.setSource(ValidationSource.TailRead);
        if (validationResult.isSuccess()) {
            this.testState.recordTailRead(validationResult.getLength());
            Duration elapsed = validationResult.getElapsed();
            if (elapsed != null) {
                this.testState.recordDuration(ConsumerOperationType.END_TO_END, elapsed);
            }
        } else {
            validationFailed(ValidationSource.TailRead, validationResult);
        }
    }

    private CompletableFuture<Void> processCatchupReads() {
        return FutureHelpers.loop(
                this::canRun,
                () -> this.catchupQueue
                        .take(1000)
                        .thenComposeAsync(this::processCatchupReads, this.executorService),
                this.executorService);
    }

    private CompletableFuture<Void> processCatchupReads(Queue<StoreReader.ReadItem> catchupReads) {
        return FutureHelpers.loop(
                () -> !catchupReads.isEmpty(),
                () -> FutureHelpers.toVoid(processCatchupRead(catchupReads.poll())),
                this.executorService);
    }

    private CompletableFuture<?> processCatchupRead(StoreReader.ReadItem toValidate) {
        final Timer timer = new Timer();
        return this.reader
                .readExact(this.segmentName, toValidate.getAddress())
                .thenApplyAsync(actualRead -> compareReads(toValidate, actualRead), this.executorService)
                .whenComplete((validationResult, ex) -> {
                    try {
                        if (validationResult == null) {
                            if (ex == null) {
                                validationResult = ValidationResult.failed("No exception and no result set.");
                            } else {
                                validationResult = ValidationResult.failed(ex.getMessage());
                            }
                        } else {
                            Event e = toValidate.getEvent();
                            this.testState.recordDuration(ConsumerOperationType.CATCHUP_READ, timer.getElapsed());
                            this.testState.recordCatchupRead(e.getTotalLength());
                        }
                    } catch (Throwable ex2) {
                        validationResult = ValidationResult.failed(String.format("General failure: Ex = %s.", ex2));
                    }

                    validationResult.setSource(ValidationSource.CatchupRead);
                    validationResult.setAddress(toValidate.getAddress());
                    if (!validationResult.isSuccess()) {
                        validationFailed(ValidationSource.CatchupRead, validationResult);
                    }
                });
    }

    @SneakyThrows(IOException.class)
    private ValidationResult compareReads(StoreReader.ReadItem expected, StoreReader.ReadItem actual) {
        Event expectedEvent = expected.getEvent();
        Event actualEvent = actual.getEvent();
        String mismatchProperty = null;

        if (expectedEvent.getOwnerId() == actualEvent.getOwnerId()) {
            mismatchProperty = "OwnerId";
        } else if (expectedEvent.getRoutingKey() == actualEvent.getRoutingKey()) {
            mismatchProperty = "RoutingKey";
        } else if (expectedEvent.getSequence() == actualEvent.getSequence()) {
            mismatchProperty = "Sequence";
        } else if (expectedEvent.getContentLength() == actualEvent.getContentLength()) {
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

        return ValidationResult.success(expectedEvent.getRoutingKey(), expectedEvent.getContentLength());
    }


    //endregion

    //region FutureExecutionSerializer

    /**
     * Helps execute futures sequentially, ensuring that they never overlap in execution. This uses the CompletableFuture's
     * chaining, which executes the given futures one after another. Note that if one future in the chain resulted in an
     * exception, the subsequent futures will not execute.
     */
    @RequiredArgsConstructor
    private static class FutureExecutionSerializer {
        private final int maxSize;
        private final Executor executorService;
        private final Object lock = new Object();
        private final AtomicInteger size = new AtomicInteger();
        private CompletableFuture<Void> storageReadQueueTail = CompletableFuture.completedFuture(null);

        /**
         * Adds the given future at the 'end' of the current queue of future to execute. If no future is currently
         * queued up, it should be invoked immediately.
         *
         * @param future The future to queue up.
         */
        void queue(Supplier<CompletableFuture<Void>> future) {
            synchronized (this.lock) {
                Preconditions.checkState(
                        !this.storageReadQueueTail.isCompletedExceptionally(),
                        "Unable to queue any more futures because at least one preceding future failed.");
                if (this.size.get() >= this.maxSize) {
                    // Already exceeding max size.
                    return;
                }

                // Increment the size and queue up the future.
                this.size.incrementAndGet();
                this.storageReadQueueTail = this.storageReadQueueTail.thenComposeAsync(v -> future.get(), this.executorService);

                // Decrement the size when the current future is complete, regardless of outcome.
                this.storageReadQueueTail.whenComplete((r, e) -> this.size.decrementAndGet());
            }
        }
    }

    //endregion
}