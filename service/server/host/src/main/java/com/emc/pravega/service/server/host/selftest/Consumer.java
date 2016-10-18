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
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.reading.AsyncReadResultEntryHandler;
import com.emc.pravega.service.server.reading.AsyncReadResultProcessor;
import lombok.val;

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

    private static final Duration READ_TIMEOUT = Duration.ofDays(100);
    private final String logId;
    private final String segmentName;
    private final AtomicBoolean canContinue;
    private final DataValidator dataValidator;

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
        this.canContinue = new AtomicBoolean();
        this.dataValidator = new DataValidator(this::validationFailed);
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        this.canContinue.set(true);
        val entryHandler = new ReadResultEntryHandler(this.config, this.dataValidator, this::canRun, this::fail);
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

    @Override
    protected String getLogId() {
        return this.logId;
    }

    //endregion

    private boolean canRun() {
        return isRunning() && this.canContinue.get();
    }

    private void validationFailed(long offset, AppendContentGenerator.ValidationResult validationResult) {
        // Log the event.
        TestLogger.log(this.logId, "VALIDATION FAILED: Segment = %s, Offset = %s (%s)", this.segmentName, offset, validationResult);

        // Then stop the consumer. No point in moving on if we detected a validation failure.
        fail(new ValidationException(this.segmentName, offset, validationResult));
    }

    //region ReadResultEntryHandler

    /**
     * Handler for the AsyncReadResultProcessor that processes the Segment read.
     */
    private static class ReadResultEntryHandler implements AsyncReadResultEntryHandler {
        //region Members

        private final TestConfig config;
        private final DataValidator dataValidator;
        private final AtomicLong readOffset;
        private final Supplier<Boolean> canRun;
        private final java.util.function.Consumer<Throwable> failureHandler;

        //endregion

        //region Constructor

        ReadResultEntryHandler(TestConfig config, DataValidator dataValidator, Supplier<Boolean> canRun, java.util.function.Consumer<Throwable> failureHandler) {
            this.config = config;
            this.dataValidator = dataValidator;
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
            this.dataValidator.process(contents.getData(), entry.getStreamSegmentOffset(), contents.getLength());
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
    }

    //endregion
}
