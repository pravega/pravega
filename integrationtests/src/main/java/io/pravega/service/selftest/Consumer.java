/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.selftest;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.function.CallbackHelpers;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.ReadResultEntry;
import io.pravega.service.contracts.ReadResultEntryContents;
import io.pravega.service.contracts.ReadResultEntryType;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.server.reading.AsyncReadResultHandler;
import io.pravega.service.server.reading.AsyncReadResultProcessor;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;

import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Represents an operation consumer. Attaches to a StoreAdapter, listens to tail-reads (on segments), and validates
 * incoming data. Currently this does not do catch-up reads or storage reads/validations.
 */
public class Consumer extends Actor {
    //region Members

    private static final String SOURCE_TAIL_READ = "TailRead";
    private static final String SOURCE_CATCHUP_READ = "CatchupRead";
    private static final String SOURCE_STORAGE_READ = "StorageRead";
    private static final Duration READ_TIMEOUT = Duration.ofDays(100);
    private final String logId;
    private final String segmentName;
    private final AtomicBoolean canContinue;
    private final TestState testState;
    private final FutureExecutionSerializer storageVerificationQueue;
    private final FutureExecutionSerializer catchupVerificationQueue;
    @GuardedBy("lock")
    private final TruncateableArray readBuffer;

    /**
     * The offset in the Segment of the first byte that we have in readBuffer.
     * This value will always be at an append boundary.
     */
    @GuardedBy("lock")
    private long readBufferSegmentOffset;

    /**
     * The offset in the Segment up to which we performed Tail-Read validation.
     * This value will always be at an append boundary.
     */
    @GuardedBy("lock")
    private long tailReadValidatedOffset;

    /**
     * The offset in the Segment up to which we performed catch-up read validation.
     * This value will always be at an append boundary.
     */
    @GuardedBy("lock")
    private long catchupReadValidatedOffset;

    /**
     * The offset in the Segment up to which we performed Storage read validation.
     * This value will always be at an append boundary.
     */
    @GuardedBy("lock")
    private long storageReadValidatedOffset;
    private final Object lock = new Object();

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
        this.canContinue = new AtomicBoolean();
        this.readBuffer = new TruncateableArray();
        this.readBufferSegmentOffset = -1;
        this.tailReadValidatedOffset = 0;
        this.catchupReadValidatedOffset = 0;
        this.storageVerificationQueue = new FutureExecutionSerializer(Integer.MAX_VALUE, this.executorService);

        // Catch-up, as opposed from Storage, queues up the same future over and over again (which reads everything to
        // the end of the Segment). No need to have more than two in the queue.
        this.catchupVerificationQueue = new FutureExecutionSerializer(2, this.executorService);
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        this.canContinue.set(true);
        CompletableFuture<Void> storeReadProcessor = processStoreReads();
        CompletableFuture<Void> storageReadProcessor = processStorageReads();
        return CompletableFuture.allOf(storeReadProcessor, storageReadProcessor);
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
        return isRunning() && this.canContinue.get();
    }

    private void validationFailed(String source, ValidationResult validationResult) {
        if (source != null) {
            validationResult.setSource(source);
        }

        // Log the event.
        TestLogger.log(this.logId, "VALIDATION FAILED: Segment = %s, %s", this.segmentName, validationResult);

        // Then stop the consumer. No point in moving on if we detected a validation failure.
        fail(new ValidationException(this.segmentName, validationResult));
    }

    private void truncateBuffer() {
        synchronized (this.lock) {
            // Make sure we don't truncate anything we haven't verified. Usually Storage Reads fall behind Catchup Reads,
            // but that's not necessarily the case all the time.
            long validatedOffset = Math.min(this.storageReadValidatedOffset, this.catchupReadValidatedOffset);
            int truncationLength = (int) (validatedOffset - this.readBufferSegmentOffset);
            if (truncationLength > 0) {
                this.readBuffer.truncate(truncationLength);
                this.readBufferSegmentOffset = validatedOffset;
            }
        }
    }

    private void logState(String stepName, String additionalMessage, Object... additionalMessageFormatArgs) {
        if (this.config.isVerboseLoggingEnabled()) {
            if (additionalMessage != null && additionalMessageFormatArgs != null && additionalMessageFormatArgs.length > 0) {
                additionalMessage = String.format(additionalMessage, additionalMessageFormatArgs);
            }

            TestLogger.log(stepName, "Segment=%s, ReadBuffer=%s (+%s), TailRead=%s, CatchupRead=%s, StorageRead=%s, %s.",
                    this.segmentName,
                    this.readBufferSegmentOffset,
                    this.readBuffer.getLength(),
                    this.tailReadValidatedOffset,
                    this.catchupReadValidatedOffset,
                    this.storageReadValidatedOffset,
                    additionalMessage);
        }
    }

    //region Storage Reads

    private CompletableFuture<Void> processStorageReads() {
        CompletableFuture<Void> result = new CompletableFuture<>();

        // Register an update listener with the storage.
        // We use the FutureExecutionSerializer here because we may get new callbacks while the previous one(s) are still
        // running, and any overlap in execution will cause repeated reads to the same locations, as well as complications
        // with respect to verification and state updates.
        val listener = new VerificationStorage.SegmentUpdateListener(
                this.segmentName,
                (length, sealed) -> this.storageVerificationQueue.queue(() ->
                        FutureHelpers.runOrFail(() -> storageSegmentChangedHandler(length, sealed, result), result)));
        this.store.getStorageAdapter().registerListener(listener);

        // Make sure the listener is closed (and thus unregistered) when we are done, whether successfully or not.
        result.whenComplete((r, ex) -> {
            listener.close();
            if (ex != null) {
                fail(ExceptionHelpers.getRealException(ex));
            }
        });

        return result;
    }

    private CompletableFuture<Void> storageSegmentChangedHandler(long segmentLength, boolean sealed, CompletableFuture<Void> processingFuture) {
        if (sealed) {
            // We reached the end of the Segment (this callback is the result of a Seal operation), so no point in listening further.
            logState(SOURCE_STORAGE_READ, "StorageLength=%s, Sealed=True", segmentLength);
            processingFuture.complete(null);
            return CompletableFuture.completedFuture(null);
        }

        long segmentStartOffset;
        int length;
        InputStream expectedData;
        synchronized (this.lock) {
            // Start from where we left off (no point in re-checking old data).
            segmentStartOffset = Math.max(this.storageReadValidatedOffset, this.readBufferSegmentOffset);

            // bufferOffset is where in the buffer we start validating at.
            int bufferOffset = (int) (segmentStartOffset - this.readBufferSegmentOffset);

            // Calculate the amount of data we want to read. Stop either at the segment length or where the tail read validator stopped.
            length = (int) (Math.min(segmentLength, this.tailReadValidatedOffset) - segmentStartOffset);

            if (length <= 0) {
                // Nothing to do (yet).
                return CompletableFuture.completedFuture(null);
            }

            expectedData = this.readBuffer.getReader(bufferOffset, length);
        }

        // Execute a Storage Read, then validate that the read data matches what was in there.
        byte[] storageReadBuffer = new byte[length];
        val storage = this.store.getStorageAdapter();
        return storage
                .openRead(this.segmentName)
                .thenComposeAsync(handle -> storage.read(handle, segmentStartOffset, storageReadBuffer, 0, length, this.config.getTimeout()), this.executorService)
                .thenAcceptAsync(l -> {
                    ValidationResult validationResult = validateStorageRead(expectedData, storageReadBuffer, segmentStartOffset);
                    validationResult.setSource(SOURCE_STORAGE_READ);
                    if (!validationResult.isSuccess()) {
                        validationFailed(SOURCE_STORAGE_READ, validationResult);
                        return;
                    }

                    // After a successful validation, update the state and truncate the buffer.
                    long diff;
                    synchronized (this.lock) {
                        diff = segmentStartOffset + length - this.storageReadValidatedOffset;
                        this.storageReadValidatedOffset += diff;
                        truncateBuffer();
                    }

                    this.testState.recordStorageRead((int) diff);
                    logState(SOURCE_STORAGE_READ, "StorageLength=%s", segmentLength);
                }, this.executorService)
                .thenComposeAsync(v -> {
                    long truncateOffset;
                    synchronized (this.lock) {
                        truncateOffset = Math.min(this.storageReadValidatedOffset, this.catchupReadValidatedOffset);
                    }

                    return this.store.getStorageAdapter().truncate(this.segmentName, truncateOffset, this.config.getTimeout());
                }, this.executorService)
                .exceptionally(ex -> {
                    processingFuture.completeExceptionally(ex);
                    return null;
                });
    }

    private ValidationResult validateStorageRead(InputStream expectedData, byte[] storageReadBuffer, long segmentOffset) {
        try {
            for (int i = 0; i < storageReadBuffer.length; i++) {
                byte expected = (byte) expectedData.read();
                if (expected != storageReadBuffer[i]) {
                    // This also includes the case when one stream ends prematurely.
                    val result = ValidationResult.failed(String.format("Corrupted data at Segment offset %s. Expected '%s', found '%s'.", segmentOffset + i, expected, storageReadBuffer[i]));
                    result.setSegmentOffset(segmentOffset + i);
                    return result;
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        return ValidationResult.success(storageReadBuffer.length);
    }

    //endregion

    //region Store Reads

    /**
     * Performs Tail & Catchup reads by issuing a long read from offset 0 to infinity, processing the output
     * of that read as a 'tail read', then re-issuing concise reads from the store for those offsets.
     */
    private CompletableFuture<Void> processStoreReads() {
        AtomicLong readOffset = new AtomicLong(0);
        // We run in a loop because it may be possible that we write more than Integer.MAX_VALUE to the segment; if that's
        // the case, we reissue the read for the next offset.
        return FutureHelpers.loop(
                this::canRun,
                () -> this.store
                        .read(this.segmentName, readOffset.get(), Integer.MAX_VALUE, READ_TIMEOUT)
                        .thenComposeAsync(readResult -> {
                            // Process the read result by reading all bits of available data as soon as they are available.
                            ReadResultHandler entryHandler = new ReadResultHandler(this.config, this::processTailRead, this::canRun, this::fail);
                            AsyncReadResultProcessor.process(readResult, entryHandler, this.executorService);
                            return entryHandler.completed.thenAccept(readOffset::addAndGet);
                        }, this.executorService)
                        .thenCompose(v -> this.store.getStreamSegmentInfo(this.segmentName, this.config.getTimeout()))
                        .handle((r, ex) -> {
                            if (ex != null) {
                                ex = ExceptionHelpers.getRealException(ex);
                                if (ex instanceof StreamSegmentNotExistsException) {
                                    // Cannot continue anymore (segment has been deleted).
                                    this.canContinue.set(false);
                                } else {
                                    // Unexpected exception.
                                    throw new CompletionException(ex);
                                }
                            } else if (r.isSealed() && readOffset.get() >= r.getLength()) {
                                // Cannot continue anymore (segment has been sealed and we reached its end).
                                this.canContinue.set(false);
                            }

                            return null;
                        }),
                this.executorService);
    }

    private void processTailRead(InputStream data, long segmentOffset, int length) {
        synchronized (this.lock) {
            // Verify that append data blocks are contiguous.
            if (this.readBufferSegmentOffset >= 0) {
                long expectedOffset = this.readBufferSegmentOffset + this.readBuffer.getLength();
                Preconditions.checkArgument(segmentOffset == expectedOffset, "Unexpected tail read offset for segment %s. Expected %s, got %s.", this.segmentName, expectedOffset, segmentOffset);
            } else {
                this.readBufferSegmentOffset = segmentOffset;
            }

            // Append data to buffer.
            this.readBuffer.append(data, length);
            logState("PROCESS_READ", "Offset=%s, Length=%s", segmentOffset, length);
        }

        // Trigger validation (this doesn't necessarily mean validation will happen, though).
        triggerTailReadValidation();

        // Just like Storage Reads, we want to make sure we execute catch-up reads in sequence. If not, we may get
        // overlapping requests, which add unnecessary burden on the test and may cause test state discrepancies.
        this.catchupVerificationQueue.queue(this::triggerCatchupReadValidation);
    }

    private void triggerTailReadValidation() {
        List<ValidationResult> successfulValidations = new ArrayList<>();
        synchronized (this.lock) {
            // Repeatedly validate the contents of the buffer, until we found a non-successful result or until it is completely drained.
            ValidationResult validationResult;
            do {
                // Validate the tip of the buffer.
                int validationStartOffset = (int) (this.tailReadValidatedOffset - this.readBufferSegmentOffset);
                validationResult = AppendContentGenerator.validate(this.readBuffer, validationStartOffset);
                validationResult.setSegmentOffset(this.readBufferSegmentOffset);
                validationResult.setSource(SOURCE_TAIL_READ);
                if (validationResult.isSuccess()) {
                    // Successful validation; advance the tail-read validated offset.
                    this.tailReadValidatedOffset += validationResult.getLength();
                    successfulValidations.add(validationResult);
                    logState(SOURCE_TAIL_READ, null);
                } else if (validationResult.isFailed()) {
                    // Validation failed. Invoke callback.
                    validationFailed(SOURCE_TAIL_READ, validationResult);
                }
            }
            while (validationResult.isSuccess() && this.readBufferSegmentOffset + this.readBuffer.getLength() > this.tailReadValidatedOffset);
        }

        // Record statistics, outside of the sync block above.
        successfulValidations.forEach(v -> {
            this.testState.recordTailRead(v.getLength());
            Duration elapsed = v.getElapsed();
            if (elapsed != null) {
                this.testState.recordDuration(ConsumerOperationType.END_TO_END, elapsed);
            }
        });
    }

    private CompletableFuture<Void> triggerCatchupReadValidation() {
        // Issue a read from Store from the first offset in the read buffer up to the validated offset
        // Verify, byte-by-byte, that the data matches.
        long segmentStartOffset;
        int length;
        InputStream expectedData;
        synchronized (this.lock) {
            // Start from where we left off (no point in re-checking old data).
            segmentStartOffset = Math.max(this.catchupReadValidatedOffset, this.readBufferSegmentOffset);

            // bufferOffset is where in the buffer we start validating at.
            int bufferOffset = (int) (segmentStartOffset - this.readBufferSegmentOffset);

            length = (int) (this.tailReadValidatedOffset - segmentStartOffset);
            if (length <= 0) {
                // Nothing to validate.
                return CompletableFuture.completedFuture(null);
            }

            expectedData = this.readBuffer.getReader(bufferOffset, length);
        }

        final Timer timer = new Timer();
        return this.store.read(this.segmentName, segmentStartOffset, length, this.config.getTimeout())
                         .thenAcceptAsync(readResult -> {
                             ValidationResult validationResult;
                             try {
                                 validationResult = validateCatchupRead(readResult, expectedData, segmentStartOffset, length);
                                 if (!validationResult.isSuccess()) {
                                     validationFailed(SOURCE_CATCHUP_READ, validationResult);
                                     return;
                                 }

                                 this.testState.recordDuration(ConsumerOperationType.CATCHUP_READ, timer.getElapsed());

                                 // Validation is successful, update current state.
                                 int verifiedLength;
                                 synchronized (this.lock) {
                                     long newLength = segmentStartOffset + length;
                                     verifiedLength = (int) (newLength - this.catchupReadValidatedOffset);
                                     this.catchupReadValidatedOffset = newLength;
                                 }

                                 this.testState.recordCatchupRead(verifiedLength);
                                 logState(SOURCE_CATCHUP_READ, null);
                             } catch (Throwable ex) {
                                 validationResult = ValidationResult.failed(String.format("General failure: ReadLength = %s, Ex = %s.", length, ex));
                                 validationResult.setSegmentOffset(segmentStartOffset);
                                 validationFailed(SOURCE_CATCHUP_READ, validationResult);
                             } finally {
                                 readResult.close();
                             }
                         }, this.executorService);
    }

    private ValidationResult validateCatchupRead(ReadResult readResult, InputStream expectedData, long segmentOffset, int length) throws Exception {
        final int initialLength = length;
        ValidationResult result = null;
        while (length > 0) {
            ReadResultEntry entry;
            if (!readResult.hasNext() || (entry = readResult.next()) == null) {
                // Reached a premature end of the ReadResult.
                result = ValidationResult.failed(String.format("Reached the end of the catch-up ReadResult, but expecting %s more bytes to be read.", length));
            } else if (entry.getStreamSegmentOffset() != segmentOffset) {
                // Something is not right.
                result = ValidationResult.failed(String.format("Invalid ReadResultEntry offset. Expected %s, Actual %s (%s bytes remaining).", segmentOffset, entry.getStreamSegmentOffset(), length));
            } else if (entry.getType() == ReadResultEntryType.EndOfStreamSegment || entry.getType() == ReadResultEntryType.Future) {
                // Not expecting EndOfSegment or Future read for catch-up reads.
                result = ValidationResult.failed(String.format("Unexpected ReadResultEntry type '%s' at offset %s.", entry.getType(), segmentOffset));
            } else {
                // Validate contents.
                entry.requestContent(this.config.getTimeout());
                //ReadResultEntryContents contents = entry.getContent().join();
                ReadResultEntryContents contents = entry.getContent().get(this.config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);

                if (contents.getLength() > length) {
                    result = ValidationResult.failed(String.format("ReadResultEntry has more data than requested (Max %s, Actual %s).", length, contents.getLength()));
                } else {
                    // Check, byte-by-byte, that the data matches what we expect.
                    InputStream actualData = contents.getData();
                    for (int i = 0; i < contents.getLength(); i++) {
                        int b1 = expectedData.read();
                        int b2 = actualData.read();
                        if (b1 != b2) {
                            // This also includes the case when one stream ends prematurely.
                            result = ValidationResult.failed(String.format("Corrupted data at Segment offset %s. Expected '%s', found '%s'.", segmentOffset + i, b1, b2));
                            break;
                        }
                    }

                    // Contents match. Update Segment pointers.
                    segmentOffset += contents.getLength();
                    length -= contents.getLength();
                }
            }

            // If we have a failure, don't bother continuing.
            if (result != null) {
                result.setSegmentOffset(segmentOffset);
                break;
            }
        }

        if (result == null) {
            result = ValidationResult.success(initialLength);
        }

        result.setSource(SOURCE_CATCHUP_READ);
        return result;
    }

    //endregion

    //region ReadResultHandler

    /**
     * Handler for the AsyncReadResultProcessor that processes the Segment read.
     */
    private static class ReadResultHandler implements AsyncReadResultHandler {
        //region Members

        private final TestConfig config;
        private final TailReadConsumer tailReadConsumer;
        private final AtomicLong readLength;
        private final Supplier<Boolean> canRun;
        private final java.util.function.Consumer<Throwable> failureHandler;
        private final CompletableFuture<Long> completed;
        //endregion

        //region Constructor

        ReadResultHandler(TestConfig config, TailReadConsumer tailReadConsumer, Supplier<Boolean> canRun, java.util.function.Consumer<Throwable> failureHandler) {
            this.config = config;
            this.tailReadConsumer = tailReadConsumer;
            this.canRun = canRun;
            this.readLength = new AtomicLong();
            this.failureHandler = failureHandler;
            this.completed = new CompletableFuture<>();
        }

        //endregion

        //region Properties

        //endregion

        //region AsyncReadResultHandler Implementation

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            if (!entry.getContent().isDone()) {
                // Make sure we only request content if it's not already available.
                entry.requestContent(this.config.getTimeout());
            }

            val contents = entry.getContent().join();
            this.readLength.addAndGet(contents.getLength());
            this.tailReadConsumer.accept(contents.getData(), entry.getStreamSegmentOffset(), contents.getLength());
            return this.canRun.get();
        }

        @Override
        public void processError(Throwable cause) {
            cause = ExceptionHelpers.getRealException(cause);
            this.completed.completeExceptionally(cause);
            if (!(cause instanceof StreamSegmentSealedException)) {
                CallbackHelpers.invokeSafely(this.failureHandler, cause, null);
            }
        }

        @Override
        public void processResultComplete() {
            this.completed.complete(this.readLength.get());
        }

        @Override
        public Duration getRequestContentTimeout() {
            return this.config.getTimeout();
        }

        //endregion

        @FunctionalInterface
        private interface TailReadConsumer {
            void accept(InputStream data, long segmentOffset, int length);
        }
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