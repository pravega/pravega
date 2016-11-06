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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.reading.AsyncReadResultEntryHandler;
import com.emc.pravega.service.server.reading.AsyncReadResultProcessor;
import com.emc.pravega.service.storage.SegmentHandle;
import com.google.common.base.Preconditions;
import lombok.val;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    private final String segmentName;
    private final String logId;
    private final AtomicBoolean canContinue;
    private final TruncateableArray readBuffer;
    private final CompletableFuture<SegmentHandle> segmentHandle;

    /**
     * The offset in the Segment of the first byte that we have in readBuffer.
     * This value will always be at an append boundary.
     */
    private long readBufferSegmentOffset;

    /**
     * The offset in the Segment up to which we performed Tail-Read validation.
     * This value will always be at an append boundary.
     */
    private long tailReadValidatedOffset;

    /**
     * The offset in the Segment up to which we performed catch-up read validation.
     * This value will always be at an append boundary.
     */
    private long catchupReadValidatedOffset;

    /**
     * The offset in the Segment up to which we performed Storage read validation.
     * This value will always be at an append boundary.
     */
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
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    Consumer(String segmentName, TestConfig config, ProducerDataSource dataSource, StoreAdapter store, ScheduledExecutorService executorService) {
        super(config, dataSource, store, executorService);
        this.logId = String.format("Consumer[%s]", segmentName);
        this.segmentName = segmentName;
        this.segmentHandle = this.store.getStorageAdapter().open(segmentName, Duration.ofSeconds(10)); // No join() or get() - we are under a lock.
        this.canContinue = new AtomicBoolean();
        this.readBuffer = new TruncateableArray();
        this.readBufferSegmentOffset = -1;
        this.tailReadValidatedOffset = 0;
        this.catchupReadValidatedOffset = 0;
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

    private boolean canRun() {
        return isRunning() && this.canContinue.get();
    }

    private void validationFailed(ValidationResult validationResult) {
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
        val listener = new VerificationStorage.SegmentUpdateListener(
                this.segmentName,
                (length, sealed) -> {
                    try {
                        storageSegmentChangedHandler(length, sealed, result);
                    } catch (Throwable ex) {
                        result.completeExceptionally(ex); // Make sure we catch exceptions; otherwise this will be stuck in a loop forever.
                    }
                });
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

    private void storageSegmentChangedHandler(long segmentLength, boolean sealed, CompletableFuture<Void> processingFuture) {
        if (sealed) {
            // We reached the end of the Segment (this callback is the result of a Seal operation), so no point in listening further.
            logState(SOURCE_STORAGE_READ, "StorageLength=%s, Sealed=True", segmentLength);
            processingFuture.complete(null);
            return;
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
                return;
            }

            expectedData = this.readBuffer.getReader(bufferOffset, length);
        }

        // Execute a Storage Read, then validate that the read data matches what was in there.
        byte[] storageReadBuffer = new byte[length];
        this.segmentHandle
                .thenCompose(handle -> this.store.getStorageAdapter().read(handle, segmentStartOffset, storageReadBuffer, 0, length, this.config.getTimeout()))
                .thenAccept(l -> {
                    ValidationResult validationResult = validateStorageRead(expectedData, storageReadBuffer, segmentStartOffset);
                    validationResult.setSource(SOURCE_STORAGE_READ);
                    if (!validationResult.isSuccess()) {
                        validationFailed(validationResult);
                        return;
                    }

                    // After a successful validation, update the state and truncate the buffer.
                    synchronized (this.lock) {
                        this.storageReadValidatedOffset = segmentLength;
                        truncateBuffer();
                    }

                    logState(SOURCE_STORAGE_READ, "StorageLength=%s", segmentLength);
                })
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        processingFuture.completeExceptionally(ex);
                    }
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

    private CompletableFuture<Void> processStoreReads() {
        val entryHandler = new ReadResultEntryHandler(this.config, this::processTailRead, this::canRun, this::fail);
        return FutureHelpers.loop(
                this::canRun,
                () -> this.store
                        .read(this.segmentName, entryHandler.getCurrentOffset(), Integer.MAX_VALUE, READ_TIMEOUT)
                        .thenComposeAsync(readResult -> {
                            // Create a future that we will immediately return.
                            CompletableFuture<Void> processComplete = new CompletableFuture<>();

                            // Create an AsyncReadResultProcessor and make sure that when it ends, we complete the future.
                            AsyncReadResultProcessor rrp = new AsyncReadResultProcessor(readResult, entryHandler, this.executorService);
                            rrp.addListener(
                                    new ServiceShutdownListener(() -> processComplete.complete(null), processComplete::completeExceptionally),
                                    this.executorService);

                            // Start the processor.
                            rrp.startAsync().awaitRunning();
                            return processComplete;
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
                            } else if (r.isSealed() && entryHandler.getCurrentOffset() >= r.getLength()) {
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
                Preconditions.checkArgument(segmentOffset == this.readBufferSegmentOffset + this.readBuffer.getLength());
            } else {
                this.readBufferSegmentOffset = segmentOffset;
            }

            // Append data to buffer.
            this.readBuffer.append(data, length);
            logState("PROCESS_READ", "Offset=%s, Length=%s", segmentOffset, length);
        }

        // Trigger validation (this doesn't necessarily mean validation will happen, though).
        triggerTailReadValidation();
        triggerCatchupReadValidation();
    }

    private void triggerTailReadValidation() {
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
                    logState(SOURCE_TAIL_READ, null);
                } else if (validationResult.isFailed()) {
                    // Validation failed. Invoke callback.
                    validationFailed(validationResult);
                }
            }
            while (validationResult.isSuccess() && this.readBufferSegmentOffset + this.readBuffer.getLength() > this.tailReadValidatedOffset);
        }
    }

    private void triggerCatchupReadValidation() {
        // Issue a read from Store from the first offset in the read buffer up to the validated offset
        // Verify, byte-by-byte, that the data matches.
        // If success, truncate data (TBD: later, we will truncate only when we read from storage).
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
                return;
            }

            expectedData = this.readBuffer.getReader(bufferOffset, length);
        }

        this.store.read(this.segmentName, segmentStartOffset, length, this.config.getTimeout())
                  .thenAcceptAsync(readResult -> {
                      try {
                          ValidationResult validationResult = validateCatchupRead(readResult, expectedData, segmentStartOffset, length);
                          if (!validationResult.isSuccess()) {
                              validationFailed(validationResult);
                              return;
                          }

                          // Validation is successful, update current state.
                          synchronized (this.lock) {
                              this.catchupReadValidatedOffset = segmentStartOffset + length;
                          }

                          logState(SOURCE_CATCHUP_READ, null);
                      } finally {
                          readResult.close();
                      }
                  }, this.executorService);
    }

    private ValidationResult validateCatchupRead(ReadResult readResult, InputStream expectedData, long segmentOffset, int length) {
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
                ReadResultEntryContents contents = entry.getContent().join();
                if (contents.getLength() > length) {
                    result = ValidationResult.failed(String.format("ReadResultEntry has more data than requested (Max %s, Actual %s).", length, contents.getLength()));
                } else {
                    // Check, byte-by-byte, that the data matches what we expect.
                    InputStream actualData = contents.getData();
                    try {
                        for (int i = 0; i < contents.getLength(); i++) {
                            int b1 = expectedData.read();
                            int b2 = actualData.read();
                            if (b1 != b2) {
                                // This also includes the case when one stream ends prematurely.
                                result = ValidationResult.failed(String.format("Corrupted data at Segment offset %s. Expected '%s', found '%s'.", segmentOffset + i, b1, b2));
                                break;
                            }
                        }
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
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

    //region ReadResultEntryHandler

    /**
     * Handler for the AsyncReadResultProcessor that processes the Segment read.
     */
    private static class ReadResultEntryHandler implements AsyncReadResultEntryHandler {
        //region Members

        private final TestConfig config;
        private final TailReadConsumer tailReadConsumer;
        private final AtomicLong readOffset;
        private final Supplier<Boolean> canRun;
        private final java.util.function.Consumer<Throwable> failureHandler;

        //endregion

        //region Constructor

        ReadResultEntryHandler(TestConfig config, TailReadConsumer tailReadConsumer, Supplier<Boolean> canRun, java.util.function.Consumer<Throwable> failureHandler) {
            this.config = config;
            this.tailReadConsumer = tailReadConsumer;
            this.canRun = canRun;
            this.readOffset = new AtomicLong();
            this.failureHandler = failureHandler;
        }

        //endregion

        //region Properties

        long getCurrentOffset() {
            return this.readOffset.get();
        }

        //endregion

        //region AsyncReadResultEntryHandler Implementation

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            val contents = entry.getContent().join();
            this.readOffset.addAndGet(contents.getLength());
            this.tailReadConsumer.accept(contents.getData(), entry.getStreamSegmentOffset(), contents.getLength());
            return this.canRun.get();
        }

        @Override
        public void processError(ReadResultEntry entry, Throwable cause) {
            cause = ExceptionHelpers.getRealException(cause);
            if (!(cause instanceof StreamSegmentSealedException)) {
                CallbackHelpers.invokeSafely(this.failureHandler, cause, null);
            }
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
}