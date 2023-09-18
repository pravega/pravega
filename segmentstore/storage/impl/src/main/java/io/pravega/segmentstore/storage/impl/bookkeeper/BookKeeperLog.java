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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.ThrottlerSourceListenerCollection;
import io.pravega.segmentstore.storage.WriteFailureException;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.segmentstore.storage.WriteTooLongException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.function.Function;

/**
 * Apache BookKeeper implementation of the DurableDataLog interface.
 * Overview:
 * * A Log is made up of several BookKeeper Ledgers plus a Log Metadata stored in ZooKeeper (separate from BookKeeper).
 * <p>
 * The Log Metadata:
 * * Is made up of an ordered list of active Ledgers along with their sequence (in the Log), the Log Truncation Address
 * and the Log Epoch.
 * * Is updated upon every successful initialization, truncation, or ledger rollover.
 * * The Epoch is updated only upon a successful initialization.
 * <p>
 * Fencing and Rollovers:
 * * This is done according to the protocol described here: https://bookkeeper.apache.org/docs/r4.4.0/bookkeeperLedgers2Logs.html
 * * See JavaDocs for the initialize() method (Open-Fence) and the rollover() method (for Rollovers) for details.
 * <p>
 * Reading the log
 * * Reading the log can only be done from the beginning. There is no random-access available.
 * * The Log Reader is designed to work well immediately after recovery. Due to BookKeeper behavior, reading while writing
 * may not immediately provide access to the last written entry, even if it was acknowledged by BookKeeper.
 * * See the LogReader class for more details.
 */
@Slf4j
@ThreadSafe
class BookKeeperLog implements DurableDataLog {
    //region Members

    private static final long REPORT_INTERVAL = 1000;
    @Getter
    private final int logId;
    @Getter(AccessLevel.PACKAGE)
    private final String logNodePath;
    @Getter(AccessLevel.PACKAGE)
    private final CuratorFramework zkClient;
    private final BookKeeper bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object lock = new Object();
    private final String traceObjectId;
    @GuardedBy("lock")
    private WriteLedger writeLedger;
    @GuardedBy("lock")
    private LogMetadata logMetadata;
    private final WriteQueue writes;
    private final SequentialAsyncProcessor writeProcessor;
    private final SequentialAsyncProcessor rolloverProcessor;
    private final BookKeeperMetrics.BookKeeperLog metrics;
    private final ScheduledFuture<?> metricReporter;
    private final ThrottlerSourceListenerCollection queueStateChangeListeners;
    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeper log class.
     *
     * @param containerId     The Id of the Container whose BookKeeperLog to open.
     * @param zkClient        A reference to the CuratorFramework client to use.
     * @param bookKeeper      A reference to the BookKeeper client to use.
     * @param config          Configuration to use.
     * @param executorService An Executor to use for async operations.
     */
    BookKeeperLog(int containerId, CuratorFramework zkClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative integer.");
        this.logId = containerId;
        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
        this.bookKeeper = Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.closed = new AtomicBoolean();
        this.logNodePath = HierarchyUtils.getPath(containerId, this.config.getZkHierarchyDepth());
        this.traceObjectId = String.format("Log[%d]", containerId);
        this.writes = new WriteQueue();
        val retry = createRetryPolicy(this.config.getMaxWriteAttempts(), this.config.getBkWriteTimeoutMillis());
        this.writeProcessor = new SequentialAsyncProcessor(this::processWritesSync, retry, this::handleWriteProcessorFailures, this.executorService);
        this.rolloverProcessor = new SequentialAsyncProcessor(this::rollover, retry, this::handleRolloverFailure, this.executorService);
        this.metrics = new BookKeeperMetrics.BookKeeperLog(containerId);
        this.metricReporter = this.executorService.scheduleWithFixedDelay(this::reportMetrics, REPORT_INTERVAL, REPORT_INTERVAL, TimeUnit.MILLISECONDS);
        this.queueStateChangeListeners = new ThrottlerSourceListenerCollection();
    }

    private Retry.RetryAndThrowBase<? extends Exception> createRetryPolicy(int maxWriteAttempts, int writeTimeout) {
        int initialDelay = writeTimeout / maxWriteAttempts;
        int maxDelay = writeTimeout * maxWriteAttempts;
        return Retry.withExpBackoff(initialDelay, 2, maxWriteAttempts, maxDelay)
                    .retryWhen(ex -> true); // Retry for every exception.
    }

    private void handleWriteProcessorFailures(Throwable exception) {
        log.warn("{}: Too many write processor failures; closing.", this.traceObjectId, exception);
        close();
    }

    private void handleRolloverFailure(Throwable exception) {
        log.warn("{}: Too many rollover failures; closing.", this.traceObjectId, exception);
        close();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.metricReporter.cancel(true);
            this.metrics.close();
            this.rolloverProcessor.close();
            this.writeProcessor.close();

            // Close active ledger.
            WriteLedger writeLedger;
            synchronized (this.lock) {
                writeLedger = this.writeLedger;
                this.writeLedger = null;
                this.logMetadata = null;
            }

            // Close the write queue and cancel the pending writes.
            this.writes.close().forEach(w -> w.fail(new ObjectClosedException(this), true));

            if (writeLedger != null) {
                try {
                    Ledgers.close(writeLedger.ledger);
                } catch (DurableDataLogException bkEx) {
                    log.error("{}: Unable to close LedgerHandle for Ledger {}.", this.traceObjectId, writeLedger.ledger.getId(), bkEx);
                }
            }

            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region DurableDataLog Implementation

    /**
     * Open-Fences this BookKeeper log using the following protocol:
     * 1. Read Log Metadata from ZooKeeper.
     * 2. Fence at least the last 2 ledgers in the Ledger List.
     * 3. Create a new Ledger.
     * 3.1 If any of the steps so far fails, the process is interrupted at the point of failure, and no cleanup is attempted.
     * 4. Update Log Metadata using compare-and-set (this update contains the new ledger and new epoch).
     * 4.1 If CAS fails on metadata update, the newly created Ledger is deleted (this means we were fenced out by some
     * other instance) and no other update is performed.
     *
     * @param timeout Timeout for the operation.
     * @throws DataLogWriterNotPrimaryException If we were fenced-out during this process.
     * @throws DataLogNotAvailableException     If BookKeeper or ZooKeeper are not available.
     * @throws DataLogDisabledException         If the BookKeeperLog is disabled. No fencing is attempted in this case.
     * @throws DataLogInitializationException   If a general initialization error occurred.
     * @throws DurableDataLogException          If another type of exception occurred.
     */
    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        List<Long> ledgersToDelete;
        LogMetadata newMetadata;
        synchronized (this.lock) {
            Preconditions.checkState(this.writeLedger == null, "BookKeeperLog is already initialized.");
            assert this.logMetadata == null : "writeLedger == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata oldMetadata = loadMetadata();

            if (oldMetadata != null) {
                if (!oldMetadata.isEnabled()) {
                    throw new DataLogDisabledException("BookKeeperLog is disabled. Cannot initialize.");
                }

                // Fence out ledgers.
                val emptyLedgerIds = Ledgers.fenceOut(oldMetadata.getLedgers(), this.bookKeeper, this.config, this.traceObjectId);

                // Update Metadata to reflect those newly found empty ledgers.
                oldMetadata = oldMetadata.updateLedgerStatus(emptyLedgerIds);
            }

            // Create new ledger.
            WriteHandle newLedger = Ledgers.create(this.bookKeeper, this.config, this.logId);
            log.info("{}: Created Ledger {}.", this.traceObjectId, newLedger.getId());

            // Update Metadata with new Ledger and persist to ZooKeeper.
            newMetadata = updateMetadata(oldMetadata, newLedger, true);
            LedgerMetadata ledgerMetadata = newMetadata.getLedger(newLedger.getId());
            assert ledgerMetadata != null : "cannot find newly added ledger metadata";
            this.writeLedger = new WriteLedger(newLedger, ledgerMetadata);
            this.logMetadata = newMetadata;
            ledgersToDelete = getLedgerIdsToDelete(oldMetadata, newMetadata);
        }

        // Delete the orphaned ledgers from BookKeeper.
        ledgersToDelete.forEach(id -> {
            try {
                Ledgers.delete(id, this.bookKeeper);
                log.info("{}: Deleted orphan empty ledger {}.", this.traceObjectId, id);
            } catch (DurableDataLogException ex) {
                // A failure here has no effect on the initialization of BookKeeperLog. In this case, the (empty) Ledger
                // will remain in BookKeeper until manually deleted by a cleanup tool.
                log.warn("{}: Unable to delete orphan empty ledger {}.", this.traceObjectId, id, ex);
            }
        });
        log.info("{}: Initialized (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, newMetadata.getEpoch(), newMetadata.getUpdateVersion());
    }

    @Override
    public void enable() throws DurableDataLogException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.lock) {
            Preconditions.checkState(this.writeLedger == null, "BookKeeperLog is already initialized; cannot re-enable.");
            assert this.logMetadata == null : "writeLedger == null but logMetadata != null";

            // Load existing metadata. Inexistent metadata means the BookKeeperLog has never been accessed, and therefore
            // enabled by default.
            LogMetadata metadata = loadMetadata();
            Preconditions.checkState(metadata != null && !metadata.isEnabled(), "BookKeeperLog is already enabled.");
            metadata = metadata.asEnabled();
            persistMetadata(metadata, false);
            log.info("{}: Enabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata.getEpoch(), metadata.getUpdateVersion());
        }
    }

    @Override
    public void disable() throws DurableDataLogException {
        // Get the current metadata, disable it, and then persist it back.
        synchronized (this.lock) {
            ensurePreconditions();
            LogMetadata metadata = getLogMetadata();
            Preconditions.checkState(metadata.isEnabled(), "BookKeeperLog is already disabled.");
            metadata = this.logMetadata.asDisabled();
            persistMetadata(metadata, false);
            this.logMetadata = metadata;
            log.info("{}: Disabled (Epoch = {}, UpdateVersion = {}).", this.traceObjectId, metadata.getEpoch(), metadata.getUpdateVersion());
        }

        // Close this instance of the BookKeeperLog. This ensures the proper cancellation of any ongoing writes.
        close();
    }

    @Override
    public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
        ensurePreconditions();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "append", data.getLength());
        if (data.getLength() > BookKeeperConfig.MAX_APPEND_LENGTH) {
            return Futures.failedFuture(new WriteTooLongException(data.getLength(), BookKeeperConfig.MAX_APPEND_LENGTH));
        }

        Timer timer = new Timer();

        // Queue up the write.
        CompletableFuture<LogAddress> result = new CompletableFuture<>();
        this.writes.add(new Write(data, getWriteLedger(), result));

        // Trigger Write Processor.
        this.writeProcessor.runAsync();

        // Post append tasks. We do not need to wait for these to happen before returning the call.
        result.whenCompleteAsync((address, ex) -> {
            if (ex != null) {
                handleWriteException(ex);
            } else {
                // Update metrics and take care of other logging tasks.
                this.metrics.writeCompleted(timer.getElapsed());
                LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, data.getLength(), address);
            }
        }, this.executorService);
        return result;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        Preconditions.checkArgument(upToAddress instanceof LedgerAddress, "upToAddress must be of type LedgerAddress.");
        return CompletableFuture.runAsync(() -> tryTruncate((LedgerAddress) upToAddress), this.executorService);
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader() throws DurableDataLogException {
        ensurePreconditions();
        return new LogReader(this.logId, getLogMetadata(), this.bookKeeper, this.config);
    }

    @Override
    public WriteSettings getWriteSettings() {
        return new WriteSettings(BookKeeperConfig.MAX_APPEND_LENGTH,
                Duration.ofMillis(this.config.getBkWriteTimeoutMillis()),
                this.config.getMaxOutstandingBytes());
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        return getLogMetadata().getEpoch();
    }

    @Override
    public QueueStats getQueueStatistics() {
        return this.writes.getStatistics();
    }

    @Override
    public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
        this.queueStateChangeListeners.register(listener);
    }

    //endregion

    //region Writes

    /**
     * Write Processor main loop. This method is not thread safe and should only be invoked as part of the Write Processor.
     */
    private void processWritesSync() {
        if (this.closed.get()) {
            // BookKeeperLog is closed. No point in trying anything else.
            return;
        }

        if (getWriteLedger().ledger.isClosed()) {
            // Current ledger is closed. Execute the rollover processor to safely create a new ledger. This will reinvoke
            // the write processor upon finish, so the writes can be reattempted.
            this.rolloverProcessor.runAsync();
        } else if (!processPendingWrites() && !this.closed.get()) {
            // We were not able to complete execution of all writes. Try again.
            this.writeProcessor.runAsync();
        }
    }

    /**
     * Executes pending Writes to BookKeeper. This method is not thread safe and should only be invoked as part of
     * the Write Processor.
     * @return True if the no errors, false if at least one write failed.
     */
    private boolean processPendingWrites() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "processPendingWrites");

        // Clean up the write queue of all finished writes that are complete (successfully or failed for good)
        val cleanupResult = this.writes.removeFinishedWrites();
        if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.WriteFailed) {
            // We encountered a failed write. As such, we must close immediately and not process anything else.
            // Closing will automatically cancel all pending writes.
            close();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, cleanupResult);
            return false;
        } else {
            if (cleanupResult.getRemovedCount() > 0) {
                this.queueStateChangeListeners.notifySourceChanged();
            }

            if (cleanupResult.getStatus() == WriteQueue.CleanupStatus.QueueEmpty) {
                // Queue is empty - nothing else to do.
                LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, cleanupResult);
                return true;
            }
        }

        // Get the writes to execute from the queue.
        List<Write> toExecute = getWritesToExecute();

        // Execute the writes, if any.
        boolean success = true;
        if (!toExecute.isEmpty()) {
            success = executeWrites(toExecute);

            if (success) {
                // After every run where we did write, check if need to trigger a rollover.
                this.rolloverProcessor.runAsync();
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "processPendingWrites", traceId, toExecute.size(), success);
        return success;
    }

    /**
     * Collects an ordered list of Writes to execute to BookKeeper.
     *
     * @return The list of Writes to execute.
     */
    private List<Write> getWritesToExecute() {
        // Calculate how much estimated space there is in the current ledger.
        final long maxTotalSize = this.config.getBkLedgerMaxSize() - getWriteLedger().ledger.getLength();

        // Get the writes to execute from the queue.
        List<Write> toExecute = this.writes.getWritesToExecute(maxTotalSize);

        // Check to see if any writes executed on closed ledgers, in which case they either need to be failed (if deemed
        // appropriate, or retried).
        if (handleClosedLedgers(toExecute)) {
            // If any changes were made to the Writes in the list, re-do the search to get a more accurate list of Writes
            // to execute (since some may have changed Ledgers, more writes may not be eligible for execution).
            toExecute = this.writes.getWritesToExecute(maxTotalSize);
        }

        return toExecute;
    }

    /**
     * Executes the given Writes to BookKeeper.
     *
     * @param toExecute The Writes to execute.
     * @return True if all the writes succeeded, false if at least one failed (if a Write failed, all subsequent writes
     * will be failed as well).
     */
    private boolean executeWrites(List<Write> toExecute) {
        log.debug("{}: Executing {} writes.", this.traceObjectId, toExecute.size());
        for (int i = 0; i < toExecute.size(); i++) {
            Write w = toExecute.get(i);
            try {
                // Record the beginning of a new attempt.
                int attemptCount = w.beginAttempt();
                if (attemptCount > this.config.getMaxWriteAttempts()) {
                    // Retried too many times.
                    throw new RetriesExhaustedException(w.getFailureCause());
                }

                // Invoke the BookKeeper write.
                w.getWriteLedger()
                      .ledger.appendAsync(w.getData().retain())
                             .whenComplete((Long entryId, Throwable error) -> {
                                addCallback(entryId, error, w);
                             });
            } catch (Throwable ex) {
                // Synchronous failure (or RetriesExhausted). Fail current write.
                boolean isFinal = !isRetryable(ex);
                w.fail(ex, isFinal);

                // And fail all remaining writes as well.
                for (int j = i + 1; j < toExecute.size(); j++) {
                    toExecute.get(j).fail(new DurableDataLogException("Previous write failed.", ex), isFinal);
                }

                return false;
            }
        }

        // Success.
        return true;
    }

    /**
     * Checks each Write in the given list if it is pointing to a closed WriteLedger. If so, it verifies if the write has
     * actually been committed (in case we hadn't been able to determine its outcome) and updates the Ledger, if needed.
     *
     * @param writes An ordered list of Writes to inspect and update.
     * @return True if any of the Writes in the given list has been modified (either completed or had its WriteLedger
     * changed).
     */
    private boolean handleClosedLedgers(List<Write> writes) {
        if (writes.size() == 0 || !writes.get(0).getWriteLedger().ledger.isClosed()) {
            // Nothing to do. We only need to check the first write since, if a Write failed with LedgerClosed, then the
            // first write must have failed for that reason (a Ledger is closed implies all ledgers before it are closed too).
            return false;
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "handleClosedLedgers", writes.size());
        WriteLedger currentLedger = getWriteLedger();
        Map<Long, Long> lastAddsConfirmed = new HashMap<>();
        boolean anythingChanged = false;
        for (Write w : writes) {
            if (w.isDone() || !w.getWriteLedger().ledger.isClosed()) {
                continue;
            }

            // Write likely failed because of LedgerClosedException. Need to check the LastAddConfirmed for each
            // involved Ledger and see if the write actually made it through or not.
            long lac = fetchLastAddConfirmed(w.getWriteLedger(), lastAddsConfirmed);
            if (w.getEntryId() >= 0 && w.getEntryId() <= lac) {
                // Write was actually successful. Complete it and move on.
                completeWrite(w);
                anythingChanged = true;
            } else if (currentLedger.ledger.getId() != w.getWriteLedger().ledger.getId()) {
                // Current ledger has changed; attempt to write to the new one.
                w.setWriteLedger(currentLedger);
                anythingChanged = true;
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "handleClosedLedgers", traceId, writes.size(), anythingChanged);
        return anythingChanged;
    }

    /**
     * Reliably gets the LastAddConfirmed for the WriteLedger
     *
     * @param writeLedger       The WriteLedger to query.
     * @param lastAddsConfirmed A Map of LedgerIds to LastAddConfirmed for each known ledger id. This is used as a cache
     *                          and will be updated if necessary.
     * @return The LastAddConfirmed for the WriteLedger.
     */
    @SneakyThrows(DurableDataLogException.class)
    private long fetchLastAddConfirmed(WriteLedger writeLedger, Map<Long, Long> lastAddsConfirmed) {
        long ledgerId = writeLedger.ledger.getId();
        long lac = lastAddsConfirmed.getOrDefault(ledgerId, -1L);
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "fetchLastAddConfirmed", ledgerId, lac);
        if (lac < 0) {
            if (writeLedger.isRolledOver()) {
                // This close was not due to failure, rather a rollover - hence lastAddConfirmed can be relied upon.
                lac = writeLedger.ledger.getLastAddConfirmed();
            } else {
                // Ledger got closed. This could be due to some external factor, and lastAddConfirmed can't be relied upon.
                // We need to re-open the ledger to get fresh data.
                lac = Ledgers.readLastAddConfirmed(ledgerId, this.bookKeeper, this.config);
            }

            lastAddsConfirmed.put(ledgerId, lac);
            log.info("{}: Fetched actual LastAddConfirmed ({}) for LedgerId {}.", this.traceObjectId, lac, ledgerId);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "fetchLastAddConfirmed", traceId, ledgerId, lac);
        return lac;
    }

    /**
     * Callback for BookKeeper appends.
     *
     * @param entryId Assigned EntryId.
     * @param error   Error.
     * @param write   the Write we were writing.
     */
    private void addCallback(Long entryId, Throwable error, Write write) {
        try {
            if (error == null) {
                assert entryId != null;
                write.setEntryId(entryId);
                // Successful write. If we get this, then by virtue of how the Writes are executed (always wait for writes
                // in previous ledgers to complete before initiating, and BookKeeper guaranteeing that all writes in this
                // ledger prior to this writes are done), it is safe to complete the callback future now.
                completeWrite(write);
                return;
            }

            // Convert the response code into an Exception. Eventually this will be picked up by the WriteProcessor which
            // will retry it or fail it permanently (this includes exceptions from rollovers).
            handleWriteException(error, write, this);
        } catch (Throwable ex) {
            // Most likely a bug in our code. We still need to fail the write so we don't leave it hanging.
            write.fail(ex, !isRetryable(ex));
        } finally {
            // Process all the appends in the queue after any change. This finalizes the completion, does retries (if needed)
            // and triggers more appends.
            try {
                this.writeProcessor.runAsync();
            } catch (ObjectClosedException ex) {
                // In case of failures, the WriteProcessor may already be closed. We don't want the exception to propagate
                // to BookKeeper.
                log.warn("{}: Not running WriteProcessor as part of callback due to BookKeeperLog being closed.", this.traceObjectId, ex);
            }
        }
    }

    /**
     * Completes the given Write and makes any necessary internal updates.
     *
     * @param write The write to complete.
     */
    private void completeWrite(Write write) {
        Timer t = write.complete();
        if (t != null) {
            this.metrics.bookKeeperWriteCompleted(write.getLength(), t.getElapsed());
        }
    }

    /**
     * Handles a general Write exception.
     */
    private void handleWriteException(Throwable ex) {
        if (ex instanceof ObjectClosedException && !this.closed.get()) {
            log.warn("{}: Caught ObjectClosedException but not closed; closing now.", this.traceObjectId, ex);
            close();
        }
    }

    /**
     * Handles an exception after a Write operation, converts it to a Pravega Exception and completes the given future
     * exceptionally using it.
     *
     * @param ex The exception from BookKeeper client.
     * @param write The Write that failed.
     */
    @VisibleForTesting
    static void handleWriteException(Throwable ex, Write write, BookKeeperLog bookKeeperLog) {
        try {
            int code = Code.UnexpectedConditionException;
            if (ex instanceof BKException) {
                BKException bKException = (BKException) ex;
                code = bKException.getCode();
            }
            switch (code) {
                case Code.LedgerFencedException:
                    // We were fenced out.
                    ex = new DataLogWriterNotPrimaryException("BookKeeperLog is not primary anymore.", ex);
                    break;
                case Code.NotEnoughBookiesException:
                    // Insufficient Bookies to complete the operation. This is a retryable exception.
                    ex = new DataLogNotAvailableException("BookKeeperLog is not available.", ex);
                    break;
                case Code.LedgerClosedException:
                    // LedgerClosed can happen because we just rolled over the ledgers or because BookKeeper closed a ledger
                    // due to some error. In either case, this is a retryable exception.
                    ex = new WriteFailureException("Active Ledger is closed.", ex);
                    break;
                case Code.WriteException:
                    // Write-related failure or current Ledger closed. This is a retryable exception.
                    ex = new WriteFailureException("Unable to write to active Ledger.", ex);
                    break;
                case Code.ClientClosedException:
                    // The BookKeeper client was closed externally. We cannot restart it here. We should close.
                    ex = new ObjectClosedException(bookKeeperLog, ex);
                    break;
                default:
                    // All the other kind of exceptions go in the same bucket.
                    ex = new DurableDataLogException("General exception while accessing BookKeeper.", ex);
            }
        } finally {
            write.fail(ex, !isRetryable(ex));
        }
    }

    /**
     * Determines whether the given exception can be retried.
     */
    private static boolean isRetryable(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return ex instanceof WriteFailureException
                || ex instanceof DataLogNotAvailableException;
    }

    /**
     * Attempts to truncate the Log. The general steps are:
     * 1. Create an in-memory copy of the metadata reflecting the truncation.
     * 2. Attempt to persist the metadata to ZooKeeper.
     * 2.1. This is the only operation that can fail the process. If this fails, the operation stops here.
     * 3. Swap in-memory metadata pointers.
     * 4. Delete truncated-out ledgers.
     * 4.1. If any of the ledgers cannot be deleted, no further attempt to clean them up is done.
     *
     * @param upToAddress The address up to which to truncate.
     */
    @SneakyThrows(DurableDataLogException.class)
    private void tryTruncate(LedgerAddress upToAddress) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "tryTruncate", upToAddress);

        // Truncate the metadata and get a new copy of it.
        val oldMetadata = getLogMetadata();
        val newMetadata = oldMetadata.truncate(upToAddress);

        // Attempt to persist the new Log Metadata. We need to do this first because if we delete the ledgers but were
        // unable to update the metadata, then the log will be corrupted (metadata points to inexistent ledgers).
        persistMetadata(newMetadata, false);

        // Repoint our metadata to the new one.
        synchronized (this.lock) {
            this.logMetadata = newMetadata;
        }

        // Determine ledgers to delete and delete them.
        val ledgerIdsToKeep = newMetadata.getLedgers().stream().map(LedgerMetadata::getLedgerId).collect(Collectors.toSet());
        val ledgersToDelete = oldMetadata.getLedgers().stream().filter(lm -> !ledgerIdsToKeep.contains(lm.getLedgerId())).iterator();
        while (ledgersToDelete.hasNext()) {
            val lm = ledgersToDelete.next();
            try {
                Ledgers.delete(lm.getLedgerId(), this.bookKeeper);
            } catch (DurableDataLogException ex) {
                // Nothing we can do if we can't delete a ledger; we've already updated the metadata. Log the error and
                // move on.
                log.error("{}: Unable to delete truncated ledger {}.", this.traceObjectId, lm.getLedgerId(), ex);
            }
        }

        log.info("{}: Truncated up to {}.", this.traceObjectId, upToAddress);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "tryTruncate", traceId, upToAddress);
    }

    //endregion

    //region Metadata Management

    /**
     * Loads the metadata for the current log, as stored in ZooKeeper.
     *
     * @return A new LogMetadata object with the desired information, or null if no such node exists.
     * @throws DataLogInitializationException If an Exception (other than NoNodeException) occurred.
     */
    @VisibleForTesting
    public LogMetadata loadMetadata() throws DataLogInitializationException {
        try {
            Stat storingStatIn = new Stat();
            byte[] serializedMetadata = this.zkClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.SERIALIZER.deserialize(serializedMetadata);
            result.withUpdateVersion(storingStatIn.getVersion());
            return result;
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("{}: No ZNode found for path '{}{}'. This is OK if this is the first time accessing this log.",
                    this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath);
            return null;
        } catch (Exception ex) {
            throw new DataLogInitializationException(String.format("Unable to load ZNode contents for path '%s%s'.",
                    this.zkClient.getNamespace(), this.logNodePath), ex);
        }
    }

    /**
     * Updates the metadata and persists it as a result of adding a new Ledger.
     *
     * @param currentMetadata   The current metadata.
     * @param newLedger         The newly added Ledger.
     * @param clearEmptyLedgers If true, the new metadata will not contain any pointers to empty Ledgers. Setting this
     *                          to true will not remove a pointer to the last few ledgers in the Log (controlled by
     *                          Ledgers.MIN_FENCE_LEDGER_COUNT), even if they are indeed empty (this is so we don't interfere
     *                          with any ongoing fencing activities as another instance of this Log may not have yet been
     *                          fenced out).
     * @return A new instance of the LogMetadata, which includes the new ledger.
     * @throws DurableDataLogException If an Exception occurred.
     */
    private LogMetadata updateMetadata(LogMetadata currentMetadata, WriteHandle newLedger, boolean clearEmptyLedgers) throws DurableDataLogException {
        boolean create = currentMetadata == null;
        if (create) {
            // This is the first ledger ever in the metadata.
            currentMetadata = new LogMetadata(newLedger.getId());
        } else {
            currentMetadata = currentMetadata.addLedger(newLedger.getId());
            if (clearEmptyLedgers) {
                // Remove those ledgers from the metadata that are empty.
                currentMetadata = currentMetadata.removeEmptyLedgers(Ledgers.MIN_FENCE_LEDGER_COUNT);
            }
        }

        try {
            persistMetadata(currentMetadata, create);
        } catch (DataLogWriterNotPrimaryException ex) {
            // Only attempt to cleanup the newly created ledger if we were fenced out. Any other exception is not indicative
            // of whether we were able to persist the metadata or not, so it's safer to leave the ledger behind in case
            // it is still used. If indeed our metadata has been updated, a subsequent recovery will pick it up and delete it
            // because it (should be) empty.
            try {
                Ledgers.delete(newLedger.getId(), this.bookKeeper);
            } catch (Exception deleteEx) {
                log.warn("{}: Unable to delete newly created ledger {}.", this.traceObjectId, newLedger.getId(), deleteEx);
                ex.addSuppressed(deleteEx);
            }

            throw ex;
        } catch (Exception ex) {
            log.warn("{}: Error while using ZooKeeper. Leaving orphaned ledger {} behind.", this.traceObjectId, newLedger.getId());
            throw ex;
        }

        log.info("{} Metadata updated ({}).", this.traceObjectId, currentMetadata);
        return currentMetadata;
    }

    /**
     * Persists the given metadata into ZooKeeper.
     *
     * @param metadata The LogMetadata to persist. At the end of this method, this metadata will have its Version updated
     *                 to the one in ZooKeeper.
     * @param create   Whether to create (true) or update (false) the data in ZooKeeper.
     * @throws DataLogWriterNotPrimaryException If the metadata update failed (if we were asked to create and the node
     *                                          already exists or if we had to update and there was a version mismatch).
     * @throws DurableDataLogException          If another kind of exception occurred.
     */
    private void persistMetadata(LogMetadata metadata, boolean create) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize(metadata).getCopy();
            Stat result = create
                    ? createZkMetadata(serializedMetadata)
                    : updateZkMetadata(serializedMetadata, metadata.getUpdateVersion());
            metadata.withUpdateVersion(result.getVersion());
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            if (reconcileMetadata(metadata)) {
                log.info("{}: Received '{}' from ZooKeeper while persisting metadata (path = '{}{}'), however metadata has been persisted correctly. Not rethrowing.",
                        this.traceObjectId, keeperEx.toString(), this.zkClient.getNamespace(), this.logNodePath);
            } else {
                // We were fenced out. Convert to an appropriate exception.
                throw new DataLogWriterNotPrimaryException(
                        String.format("Unable to acquire exclusive write lock for log (path = '%s%s').", this.zkClient.getNamespace(), this.logNodePath),
                        keeperEx);
            }
        } catch (Exception generalEx) {
            // General exception. Convert to an appropriate exception.
            throw new DataLogInitializationException(
                    String.format("Unable to update ZNode for path '%s%s'.", this.zkClient.getNamespace(), this.logNodePath),
                    generalEx);
        }

        log.info("{} Metadata persisted ({}).", this.traceObjectId, metadata);
    }

    @VisibleForTesting
    protected Stat createZkMetadata(byte[] serializedMetadata) throws Exception {
        val result = new Stat();
        this.zkClient.create().creatingParentsIfNeeded().storingStatIn(result).forPath(this.logNodePath, serializedMetadata);
        return result;
    }

    @VisibleForTesting
    protected Stat updateZkMetadata(byte[] serializedMetadata, int version) throws Exception {
        return this.zkClient.setData().withVersion(version).forPath(this.logNodePath, serializedMetadata);
    }

    /**
     * Verifies the given {@link LogMetadata} against the actual one stored in ZooKeeper.
     *
     * @param metadata The Metadata to check.
     * @return True if the metadata stored in ZooKeeper is an identical match to the given one, false otherwise. If true,
     * {@link LogMetadata#getUpdateVersion()} will also be updated with the one stored in ZooKeeper.
     */
    private boolean reconcileMetadata(LogMetadata metadata) {
        try {
            val actualMetadata = loadMetadata();
            if (metadata.equals(actualMetadata)) {
                metadata.withUpdateVersion(actualMetadata.getUpdateVersion());
                return true;
            }
        } catch (DataLogInitializationException ex) {
            log.warn("{}: Unable to verify persisted metadata (path = '{}{}').", this.traceObjectId, this.zkClient.getNamespace(), this.logNodePath, ex);
        }
        return false;
    }

    /**
     * Persists the given metadata into ZooKeeper, overwriting whatever was there previously.
     *
     * @param metadata Thew metadata to write.
     * @throws IllegalStateException    If this BookKeeperLog is not disabled.
     * @throws IllegalArgumentException If `metadata.getUpdateVersion` does not match the current version in ZooKeeper.
     * @throws DurableDataLogException  If another kind of exception occurred. See {@link #persistMetadata}.
     */
    @VisibleForTesting
    void overWriteMetadata(LogMetadata metadata) throws DurableDataLogException {
        LogMetadata currentMetadata = loadMetadata();
        boolean create = currentMetadata == null;
        if (!create) {
            Preconditions.checkState(!currentMetadata.isEnabled(), "Cannot overwrite metadata if BookKeeperLog is enabled.");
            Preconditions.checkArgument(currentMetadata.getUpdateVersion() == metadata.getUpdateVersion(),
                    "Wrong Update Version; expected %s, given %s.", currentMetadata.getUpdateVersion(), metadata.getUpdateVersion());
        }

        persistMetadata(metadata, create);
    }


    //endregion

    //region Ledger Rollover

    /**
     * Triggers an asynchronous rollover, if the current Write Ledger has exceeded its maximum length.
     * The rollover protocol is as follows:
     * 1. Create a new ledger.
     * 2. Create an in-memory copy of the metadata and add the new ledger to it.
     * 3. Update the metadata in ZooKeeper using compare-and-set.
     * 3.1 If the update fails, the newly created ledger is deleted and the operation stops.
     * 4. Swap in-memory pointers to the active Write Ledger (all future writes will go to the new ledger).
     * 5. Close the previous ledger (and implicitly seal it).
     * 5.1 If closing fails, there is nothing we can do. We've already opened a new ledger and new writes are going to it.
     *
     * NOTE: this method is not thread safe and is not meant to be executed concurrently. It should only be invoked as
     * part of the Rollover Processor.
     */
    @SneakyThrows(DurableDataLogException.class) // Because this is an arg to SequentialAsyncProcessor, which wants a Runnable.
    private void rollover() {
        if (this.closed.get()) {
            // BookKeeperLog is closed; no point in running this.
            return;
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollover");
        val l = getWriteLedger().ledger;
        if (!l.isClosed() && l.getLength() < this.config.getBkLedgerMaxSize()) {
            // Nothing to do. Trigger the write processor just in case this rollover was invoked because the write
            // processor got a pointer to a LedgerHandle that was just closed by a previous run of the rollover processor.
            this.writeProcessor.runAsync();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, false);
            return;
        }

        try {
            // Create new ledger.
            WriteHandle newLedger = Ledgers.create(this.bookKeeper, this.config, this.logId);
            log.debug("{}: Rollover: created new ledger {}.", this.traceObjectId, newLedger.getId());

            // Update the metadata.
            LogMetadata metadata = getLogMetadata();
            metadata = updateMetadata(metadata, newLedger, false);
            LedgerMetadata ledgerMetadata = metadata.getLedger(newLedger.getId());
            assert ledgerMetadata != null : "cannot find newly added ledger metadata";
            log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata);

            // Update pointers to the new ledger and metadata.
            WriteHandle oldLedger;
            synchronized (this.lock) {
                oldLedger = this.writeLedger.ledger;
                if (!oldLedger.isClosed()) {
                    // Only mark the old ledger as Rolled Over if it is still open. Otherwise it means it was closed
                    // because of some failure and should not be marked as such.
                    this.writeLedger.setRolledOver(true);
                }

                this.writeLedger = new WriteLedger(newLedger, ledgerMetadata);
                this.logMetadata = metadata;
            }

            // Close the old ledger. This must be done outside of the lock, otherwise the pending writes (and their callbacks)
            // will be invoked within the lock, thus likely candidates for deadlocks.
            Ledgers.close(oldLedger);
            log.info("{}: Rollover: swapped ledger and metadata pointers (Old = {}, New = {}) and closed old ledger.",
                    this.traceObjectId, oldLedger.getId(), newLedger.getId());
        } finally {
            // It's possible that we have writes in the queue that didn't get picked up because they exceeded the predicted
            // ledger length. Invoke the Write Processor to execute them.
            this.writeProcessor.runAsync();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, true);
        }
    }

    @Override
    public void overrideEpoch(long epoch) throws DurableDataLogException {
        LogMetadata metadata = this.getLogMetadata();
        synchronized ( this.lock ) {
            val newMetadata = LogMetadata
                    .builder()
                    .enabled(true)
                    .epoch(epoch)
                    .truncationAddress(getOrDefault(metadata, LogMetadata::getTruncationAddress, LogMetadata.INITIAL_TRUNCATION_ADDRESS))
                    .updateVersion(getOrDefault(metadata, LogMetadata::getUpdateVersion, LogMetadata.INITIAL_VERSION))
                    .ledgers(getOrDefault(metadata, LogMetadata::getLedgers, new ArrayList<LedgerMetadata>()))
                    .build();
            this.persistMetadata(newMetadata, false);
            this.logMetadata = newMetadata;
        }
        log.info("{}: Overridden epoch to {} in metadata {}", this.traceObjectId, epoch, metadata);
    }

    private <T> T getOrDefault(LogMetadata metadata, Function<LogMetadata, T> getter, T defaultValue) {
        return metadata == null ? defaultValue : getter.apply(metadata);
    }

    /**
     * Determines which Ledger Ids are safe to delete from BookKeeper.
     *
     * @param oldMetadata     A pointer to the previous version of the metadata, that contains all Ledgers eligible for
     *                        deletion. Only those Ledgers that do not exist in currentMetadata will be selected.
     * @param currentMetadata A pointer to the current version of the metadata. No Ledger that is referenced here will
     *                        be selected.
     * @return A List that contains Ledger Ids to remove. May be empty.
     */
    @GuardedBy("lock")
    private List<Long> getLedgerIdsToDelete(LogMetadata oldMetadata, LogMetadata currentMetadata) {
        if (oldMetadata == null) {
            return Collections.emptyList();
        }

        val existingIds = currentMetadata.getLedgers().stream()
                .map(LedgerMetadata::getLedgerId)
                .collect(Collectors.toSet());
        return oldMetadata.getLedgers().stream()
                .map(LedgerMetadata::getLedgerId)
                .filter(id -> !existingIds.contains(id))
                .collect(Collectors.toList());
    }

    //endregion

    //region Helpers

    private void reportMetrics() {
        LogMetadata metadata = getLogMetadata();
        if (metadata != null) {
            this.metrics.ledgerCount(metadata.getLedgers().size());
            this.metrics.queueStats(this.writes.getStatistics());
        }
    }

    private LogMetadata getLogMetadata() {
        synchronized (this.lock) {
            return this.logMetadata;
        }
    }

    private WriteLedger getWriteLedger() {
        synchronized (this.lock) {
            return this.writeLedger;
        }
    }

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.lock) {
            Preconditions.checkState(this.writeLedger != null, "BookKeeperLog is not initialized.");
            assert this.logMetadata != null : "writeLedger != null but logMetadata == null";
        }
    }

    //endregion
}
