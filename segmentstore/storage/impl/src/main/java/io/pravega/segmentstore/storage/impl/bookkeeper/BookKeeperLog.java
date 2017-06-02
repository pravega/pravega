/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.SequentialAsyncProcessor;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.WriteFailureException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

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

    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 100;

    private final String logNodePath;
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
    @GuardedBy("writes")
    private final Deque<Write> writes;
    private final SequentialAsyncProcessor writeProcessor;
    private final SequentialAsyncProcessor rolloverProcessor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeper log class.
     *
     * @param logId           The BookKeeper Log Id to open.
     * @param zkClient        A reference to the CuratorFramework client to use.
     * @param bookKeeper      A reference to the BookKeeper client to use.
     * @param config          Configuration to use.
     * @param executorService An Executor to use for async operations.
     */
    BookKeeperLog(int logId, CuratorFramework zkClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(logId >= 0, "logId must be a non-negative integer.");

        this.zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
        this.bookKeeper = Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.closed = new AtomicBoolean();
        this.logNodePath = HierarchyUtils.getPath(logId, this.config.getZkHierarchyDepth());
        this.traceObjectId = String.format("Log[%d]", logId);
        this.writes = new ArrayDeque<>();
        this.writeProcessor = new SequentialAsyncProcessor(this::processWritesSync, this.executorService);
        this.rolloverProcessor = new SequentialAsyncProcessor(this::rollover, this.executorService);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.rolloverProcessor.close();
            this.writeProcessor.close();

            // Close active ledger.
            WriteLedger writeLedger;
            synchronized (this.lock) {
                writeLedger = this.writeLedger;
                this.writeLedger = null;
                this.logMetadata = null;
            }

            // Cancel pending writes.
            List<Write> toCancel;
            synchronized (this.writes) {
                toCancel = new ArrayList<>(this.writes);
                this.writes.clear();
            }

            toCancel.forEach(w -> w.fail(new ObjectClosedException(this), true));

            if (writeLedger != null) {
                try {
                    Ledgers.close(writeLedger.ledger);
                } catch (DurableDataLogException bkEx) {
                    log.error("{}: Unable to close LedgerHandle for Ledger {}.", this.traceObjectId, writeLedger.ledger.getId(), bkEx);
                }
            }
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
     * @throws DataLogInitializationException   If a general initialization error occurred.
     * @throws DurableDataLogException          If another type of exception occurred.
     */
    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        synchronized (this.lock) {
            Preconditions.checkState(this.writeLedger == null, "BookKeeperLog is already initialized.");
            assert this.logMetadata == null : "writeLedger == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata metadata = loadMetadata();

            // Fence out ledgers.
            if (metadata != null) {
                Ledgers.fenceOut(metadata.getLedgers(), this.bookKeeper, this.config, this.traceObjectId);
            }

            // Create new ledger.
            LedgerHandle newLedger = Ledgers.create(this.bookKeeper, this.config);
            log.info("{}: Created Ledger {}.", this.traceObjectId, newLedger.getId());

            // Update node with new ledger.
            metadata = updateMetadata(metadata, newLedger);
            LedgerMetadata ledgerMetadata = metadata.getLedger(newLedger.getId());
            assert ledgerMetadata != null : "cannot find newly added ledger metadata";
            this.writeLedger = new WriteLedger(newLedger, ledgerMetadata);
            this.logMetadata = metadata;
        }
    }

    @Override
    public CompletableFuture<LogAddress> append(ArrayView data, Duration timeout) {
        ensurePreconditions();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "append");
        Timer timer = new Timer();

        // Queue up the write.
        CompletableFuture<LogAddress> result = new CompletableFuture<>();
        synchronized (this.writes) {
            this.writes.addLast(new Write(data, getWriteLedger(), result));
        }

        // Trigger Write Processor.
        this.writeProcessor.runAsync();

        // Post append tasks. We do not need to wait for these to happen before returning the call.
        result.whenCompleteAsync((address, ex) -> {
            if (ex != null) {
                handleWriteException(ex);
            } else {
                // Update metrics and take care of other logging tasks.
                Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
                Metrics.WRITE_BYTES.add(data.getLength());
                LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, address, data.getLength());
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
        return new LogReader(getLogMetadata(), this.bookKeeper, this.config);
    }

    @Override
    public int getMaxAppendLength() {
        return MAX_APPEND_LENGTH;
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        return getLogMetadata().getEpoch();
    }

    //endregion

    //region Writes

    /**
     * Write Processor main loop. This method is not thread safe and should only be invoked as part of the Write Processor.
     */
    private void processWritesSync() {
        List<Write> toExecute = getWritesToExecute();
        if (!processWrites(toExecute)) {
            // We were not able to complete execution of all writes. Try again.
            this.writeProcessor.runAsync();
        } else if (toExecute.size() > 0) {
            // After every append, check if we need to trigger a rollover.
            this.rolloverProcessor.runAsync();
        }
    }

    /**
     * Executes the given writes to BookKeeper. This method is not thread safe and should only be invoked as part of
     * the Write Processor.
     *
     * @param toExecute An ordered List of Writes to execute.
     */
    private boolean processWrites(List<Write> toExecute) {
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
                w.getLedger().asyncAddEntry(w.data.array(), w.data.arrayOffset(), w.data.getLength(), this::addCallback, w);
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
        return true;
    }

    /**
     * Gets an ordered list of Writes to execute.
     */
    private List<Write> getWritesToExecute() {
        long accumulatedSize = getWriteLedger().ledger.getLength();
        int activeWriteCount = 0;
        synchronized (this.writes) {
            // Clean up the write queue of all finished writes that are complete (successfully or failed for good)
            while (!this.writes.isEmpty() && this.writes.peekFirst().isDone()) {
                this.writes.removeFirst();
            }

            if (this.writes.isEmpty()) {
                return Collections.emptyList();
            }

            // Collect all remaining writes, as long as they are not currently in-progress and have the same ledger id
            // as the first item in the ledger.
            long firstLedgerId = this.writes.peekFirst().getLedgerMetadata().getLedgerId();
            boolean canSkip = true;

            List<Write> result = new ArrayList<>();
            for (Write write : this.writes) {
                if (accumulatedSize >= this.config.getBkLedgerMaxSize()
                        || activeWriteCount >= this.config.getMaxConcurrentWrites()) {
                    // Either reached the throttling limit or ledger max size limit.
                    // If we try to send too many writes to this ledger, the writes are likely to be rejected with
                    // LedgerClosedException and simply be retried again.
                    break;
                }

                // Account for this write's size, even if it's complete or in progress.
                accumulatedSize += write.data.getLength();
                if (write.isInProgress()) {
                    activeWriteCount++;
                    if (!canSkip) {
                        // We stumbled across an in-progress write after a not-in-progress write. We can't retry now.
                        // This is likely due to a bunch of writes failing (i.e. due to a LedgerClosedEx), but we overlapped
                        // with their updating their status. Try again next time (when that write completes).
                        return Collections.emptyList();
                    }
                } else if (write.getLedger().getId() != firstLedgerId) {
                    // We cannot initiate writes in a new ledger until all writes in the previous ledger completed.
                    break;
                } else if (!write.isDone()) {
                    canSkip = false;
                    result.add(write);
                    activeWriteCount++;
                }
            }

            return result;
        }
    }

    /**
     * Callback for BookKeeper appends.
     *
     * @param rc      Response Code.
     * @param handle  LedgerHandle.
     * @param entryId Assigned EntryId.
     * @param ctx     Write Context. In our case, the Write we were writing.
     */
    private void addCallback(int rc, LedgerHandle handle, long entryId, Object ctx) {
        @SuppressWarnings("unchecked")
        Write write = (Write) ctx;
        try {
            assert handle.getId() == write.getLedger().getId() : "Handle.Id mismatch: " + write.getLedger().getId() + " vs " + handle.getId();
            if (rc == 0) {
                // Successful write. If we get this, then by virtue of how the Writes are executed (always wait for writes
                // in previous ledgers to complete before initiating, and BookKeeper guaranteeing that all writes in this
                // ledger prior to this writes are done), it is safe to complete the callback future now.
                write.complete(new LedgerAddress(write.getLedgerMetadata(), entryId));
                return;
            }

            if (rc == BKException.Code.LedgerClosedException) {
                // LedgerClosed can happen because we just rolled over the ledgers. The way to detect this is to
                // check if the ledger we were trying to write to is closed and whether we have a new ledger.
                // If that's the case, retry the write with the new ledger.
                WriteLedger currentLedger = getWriteLedger();
                if (!currentLedger.ledger.isClosed() && currentLedger.ledger.getId() != handle.getId()) {
                    // Note that we do not set a failure for this - this is considered normal operations (so we also
                    // roll back the attempt count).
                    write.setWriteLedger(currentLedger);
                    write.endAttempt(true);
                    return;
                }
            }

            handleWriteException(rc, write);
        } catch (Throwable ex) {
            // Most likely a bug in our code. We still need to fail the write so we don't leave it hanging.
            write.fail(ex, !isRetryable(ex));
        } finally {
            // Process all the appends in the queue after any change. This finalizes the completion, does retries (if needed)
            // and triggers more appends.
            this.writeProcessor.runAsync();
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
     * @param responseCode   The BookKeeper response code to interpret.
     * @param write          The Write that failed.
     */
    private void handleWriteException(int responseCode, Write write) {
        assert responseCode != BKException.Code.OK : "cannot handle an exception when responseCode == " + BKException.Code.OK;
        Exception ex = BKException.create(responseCode);
        try {
            if (ex instanceof BKException.BKLedgerFencedException) {
                // We were fenced out.
                ex = new DataLogWriterNotPrimaryException("Log not primary anymore.", ex);
            } else if (ex instanceof BKException.BKNotEnoughBookiesException) {
                // Insufficient Bookies to complete the operation. This is a retryable exception.
                ex = new DataLogNotAvailableException("Log not available.", ex);
            } else if (ex instanceof BKException.BKWriteException) {
                // Write-related failure. This is a retryable exception.
                ex = new WriteFailureException("Unable to write to active ledger.", ex);
            } else if (ex instanceof BKException.BKClientClosedException) {
                // The BookKeeper client was closed externally. We cannot restart it here. We should close.
                ex = new ObjectClosedException(this, ex);
            } else {
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
        ex = ExceptionHelpers.getRealException(ex);
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
    private LogMetadata loadMetadata() throws DataLogInitializationException {
        try {
            Stat storingStatIn = new Stat();
            byte[] serializedMetadata = this.zkClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.deserialize(serializedMetadata);
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
     * @param currentMetadata The current metadata.
     * @param newLedger       The newly added Ledger.
     * @return A new instance of the LogMetadata, which includes the new ledger.
     * @throws DurableDataLogException If an Exception occurred.
     */
    private LogMetadata updateMetadata(LogMetadata currentMetadata, LedgerHandle newLedger) throws DurableDataLogException {
        boolean create = currentMetadata == null;
        if (create) {
            // This is the first ledger ever in the metadata.
            currentMetadata = new LogMetadata(newLedger.getId());
        } else {
            currentMetadata = currentMetadata.addLedger(newLedger.getId(), true);
        }

        try {
            persistMetadata(currentMetadata, create);
        } catch (DurableDataLogException ex) {
            try {
                Ledgers.delete(newLedger.getId(), this.bookKeeper);
            } catch (Exception deleteEx) {
                log.warn("{}: Unable to delete newly created ledger {}.", this.traceObjectId, newLedger.getId(), deleteEx);
                ex.addSuppressed(deleteEx);
            }
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
            if (create) {
                byte[] serializedMetadata = metadata.serialize();
                this.zkClient.create()
                             .creatingParentsIfNeeded()
                             .forPath(this.logNodePath, serializedMetadata);
                // Set version to 0 as that will match the ZNode's version.
                metadata.withUpdateVersion(0);
            } else {
                byte[] serializedMetadata = metadata.serialize();
                this.zkClient.setData()
                             .withVersion(metadata.getUpdateVersion())
                             .forPath(this.logNodePath, serializedMetadata);

                // Increment the version to keep up with the ZNode's value (after writing it to ZK).
                metadata.withUpdateVersion(metadata.getUpdateVersion() + 1);
            }
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            // We were fenced out. Clean up and throw appropriate exception.
            throw new DataLogWriterNotPrimaryException(
                    String.format("Unable to acquire exclusive write lock for log (path = '%s%s').", this.zkClient.getNamespace(), this.logNodePath),
                    keeperEx);
        } catch (Exception generalEx) {
            // General exception. Clean up and rethrow appropriate exception.
            throw new DataLogInitializationException(
                    String.format("Unable to update ZNode for path '%s%s'.", this.zkClient.getNamespace(), this.logNodePath),
                    generalEx);
        }

        log.info("{} Metadata persisted ({}).", this.traceObjectId, metadata);
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
    private void rollover() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollover");
        val l = getWriteLedger().ledger;
        if (l.getLength() < this.config.getBkLedgerMaxSize()) {
            LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, false);
            return;
        }

        try {
            // Create new ledger.
            LedgerHandle newLedger = Ledgers.create(this.bookKeeper, this.config);
            log.debug("{}: Rollover: created new ledger {}.", this.traceObjectId, newLedger.getId());

            // Update the metadata.
            LogMetadata metadata = getLogMetadata();
            metadata = updateMetadata(metadata, newLedger);
            LedgerMetadata ledgerMetadata = metadata.getLedger(newLedger.getId());
            assert ledgerMetadata != null : "cannot find newly added ledger metadata";
            log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata);

            // Update pointers to the new ledger and metadata.
            LedgerHandle oldLedger;
            synchronized (this.lock) {
                oldLedger = this.writeLedger.ledger;
                this.writeLedger = new WriteLedger(newLedger, ledgerMetadata);
                this.logMetadata = metadata;
            }

            // Close the old ledger. This must be done outside of the lock, otherwise the pending writes (and their callbacks
            // will be invoked within the lock, thus likely candidates for deadlocks).
            Ledgers.close(oldLedger);
            log.debug("{}: Rollover: swapped ledger and metadata pointers (Old = {}, New = {}) and closed old ledger.",
                    this.traceObjectId, oldLedger.getId(), newLedger.getId());
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                log.error("{}: Rollover failure; log may be unusable.", ex);
            }
        }

        // It's possible that we have writes in the queue that didn't get picked up because they exceeded the predicted
        // ledger length. Invoke the Write Processor to execute them.
        this.writeProcessor.runAsync();
        LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId, true);
    }

    //endregion

    //region Helpers

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

    //region WriteLedger

    @RequiredArgsConstructor
    private static class WriteLedger {
        final LedgerHandle ledger;
        final LedgerMetadata metadata;

        @Override
        public String toString() {
            return String.format("%s, Length = %d, Closed = %s", this.metadata, this.ledger.getLength(), this.ledger.isClosed());
        }
    }

    //endregion

    //region Write

    private static class Write {
        final ArrayView data;
        private final CompletableFuture<LogAddress> result;
        private final AtomicInteger attemptCount;
        private final AtomicReference<WriteLedger> writeLedger;
        private final AtomicBoolean inProgress;
        private final AtomicReference<Throwable> failureCause;

        Write(ArrayView data, WriteLedger initialWriteLedger, CompletableFuture<LogAddress> result) {
            this.data = Preconditions.checkNotNull(data, "data");
            this.writeLedger = new AtomicReference<>(Preconditions.checkNotNull(initialWriteLedger, "initialWriteLedger"));
            this.result = Preconditions.checkNotNull(result, "result");
            this.attemptCount = new AtomicInteger();
            this.failureCause = new AtomicReference<>();
            this.inProgress = new AtomicBoolean();
        }

        /**
         * Gets the LedgerHandle associated with this write.
         */
        LedgerHandle getLedger() {
            return this.writeLedger.get().ledger;
        }

        /**
         * Gets the LedgerMetadata for the Ledger associated with this write.
         */
        LedgerMetadata getLedgerMetadata() {
            return this.writeLedger.get().metadata;
        }

        /**
         * Sets the WriteLedger to be associated with this write.
         *
         * @param writeLedger The WriteLedger to associate.
         */
        void setWriteLedger(WriteLedger writeLedger) {
            this.writeLedger.set(writeLedger);
        }

        /**
         * Records the fact that a new attempt to execute this write is begun.
         *
         * @return The current attempt number.
         */
        int beginAttempt() {
            Preconditions.checkState(this.inProgress.compareAndSet(false, true), "Write already in progress. Cannot restart.");
            return this.attemptCount.incrementAndGet();
        }

        /**
         * Records the fact that an attempt to execute this write has ended.
         *
         * @param rollback If true, it rolls back the attempt count.
         */
        void endAttempt(boolean rollback) {
            if (rollback && this.attemptCount.decrementAndGet() < 0) {
                this.attemptCount.set(0); // Make sure it doesn't become negative.
            }

            this.inProgress.set(false);
        }

        /**
         * Gets a value indicating whether an attempt to execute this write is in progress.
         *
         * @return True or false.
         */
        boolean isInProgress() {
            return this.inProgress.get();
        }

        /**
         * Gets a value indicating whether this write is completed (successfully or not).
         *
         * @return True or false.
         */
        boolean isDone() {
            return this.result.isDone();
        }

        /**
         * Gets the failure cause, if any.
         *
         * @return The failure cause.
         */
        Throwable getFailureCause() {
            return this.failureCause.get();
        }

        /**
         * Indicates that this write completed successfully. This will set the final result on the externalCompletion future.
         *
         * @param result The result to set.
         */
        void complete(LogAddress result) {
            this.failureCause.set(null);
            this.result.complete(result);
            endAttempt(false);
        }

        /**
         * Indicates that this write failed.
         *
         * @param cause    The failure cause. If null, the previous failure cause is preserved.
         * @param complete If true, the externalCompletion will be immediately be completed with the current failure cause.
         *                 If false, no completion will be done.
         */
        void fail(Throwable cause, boolean complete) {
            if (cause != null) {
                Throwable e = this.failureCause.get();
                if (e != null && e != cause) {
                    cause.addSuppressed(e);
                }

                this.failureCause.set(cause);
            }

            endAttempt(false);
            if (complete) {
                this.result.completeExceptionally(this.failureCause.get());
            }
        }

        @Override
        public String toString() {
            return String.format("LedgerId = %s, Length = %s, Attempts = %s, InProgress = %s, Done = %s, Failed %s",
                    this.writeLedger.get().ledger.getId(), this.data.getLength(), this.attemptCount, this.inProgress,
                    isDone(), this.failureCause.get() != null);
        }
    }

    //endregion
}
