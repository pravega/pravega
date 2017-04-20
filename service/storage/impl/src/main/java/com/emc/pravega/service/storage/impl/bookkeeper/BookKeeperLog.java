/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.Timer;
import com.emc.pravega.common.concurrent.ExecutorServiceHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.storage.DataLogInitializationException;
import com.emc.pravega.service.storage.DataLogNotAvailableException;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.service.storage.WriteFailureException;
import com.emc.pravega.service.storage.WriteTooLongException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Apache BookKeeper implementation of the DurableDataLog interface.
 */
@Slf4j
@ThreadSafe
class BookKeeperLog implements DurableDataLog {
    //region Private
    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 100;

    private final String logNodePath;
    private final CuratorFramework curatorClient;
    private final BookKeeper bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object lock = new Object();
    private final String traceObjectId;
    private final Retry.RetryAndThrowBase<Exception> retryPolicy;
    private final AtomicReference<LedgerAddress> lastAppendAddress;
    private final AtomicBoolean rolloverInProgress;
    @GuardedBy("lock")
    private WriteLedger writeLedger;
    @GuardedBy("lock")
    private LogMetadata logMetadata;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BookKeeper log class.
     *
     * @param logId           The BookKeeper Log Id to open.
     * @param curatorClient   A reference to the CuratorFramework client to use.
     * @param bookKeeper      A reference to the BookKeeper client to use.
     * @param config          Configuration to use.
     * @param executorService An Executor to use for async operations.
     */
    BookKeeperLog(int logId, CuratorFramework curatorClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(logId >= 0, "logId must be a non-negative integer.");

        this.curatorClient = Preconditions.checkNotNull(curatorClient, "curatorClient");
        this.bookKeeper = Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.closed = new AtomicBoolean();
        this.logNodePath = HierarchyUtils.getPath(this.config.getNamespace(), logId, this.config.getZkHierarchyDepth());
        this.lastAppendAddress = new AtomicReference<>(new LedgerAddress(0, 0));
        this.traceObjectId = String.format("Log[%d]", logId);
        this.rolloverInProgress = new AtomicBoolean();
        this.retryPolicy = config.getRetryPolicy()
                                 .retryWhen(BookKeeperLog::isRetryable)
                                 .throwingOn(Exception.class);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close active ledger.
            WriteLedger writeLedger;
            synchronized (this.lock) {
                writeLedger = this.writeLedger;
                this.writeLedger = null;
                this.logMetadata = null;
            }

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

    @Override
    public void initialize(Duration timeout) throws DurableDataLogException {
        synchronized (this.lock) {
            Preconditions.checkState(this.writeLedger == null, "BookKeeperLog is already initialized.");
            assert this.logMetadata == null : "writeLedger == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata metadata = loadMetadata();

            // Fence out ledgers.
            if (metadata != null) {
                val lastAddress = Ledgers.fenceOut(metadata.getLedgers(), this.bookKeeper, this.config, this.traceObjectId);
                if (lastAddress != null) {
                    this.lastAppendAddress.set(lastAddress);
                }
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
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        ensurePreconditions();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "append");
        Timer timer = new Timer();

        // TODO: refactor API to take in ByteArraySegment instead of InputStream to avoid the extra copy. Then get rid of this block of code.
        byte[] buffer;
        try {
            int dataLength = data.available();
            if (dataLength > MAX_APPEND_LENGTH) {
                return FutureHelpers.failedFuture(new WriteTooLongException(dataLength, MAX_APPEND_LENGTH));
            }

            buffer = new byte[dataLength];
            int bytesRead = StreamHelpers.readAll(data, buffer, 0, buffer.length);
            assert bytesRead == buffer.length : String.format("StreamHelpers.ReadAll did not read entire input stream. Expected %d, Actual %d.", buffer.length, bytesRead);
        } catch (IOException ex) {
            return FutureHelpers.failedFuture(ex);
        }

        // Use a retry loop to handle retryable exceptions.
        val result = this.retryPolicy.runAsync(() -> tryAppend(buffer).exceptionally(this::handleWriteException), this.executorService);

        // Post append tasks. We do not need to wait for these to happen before returning the call.
        result.thenAcceptAsync(address -> {
            // Update metrics and take care of other logging tasks.
            Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
            Metrics.WRITE_BYTES.add(buffer.length);
            LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, address, buffer.length);

            // After every append, check if we need to trigger a rollover.
            triggerRolloverIfNecessary();
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
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ensurePreconditions();
        return new LogReader(getLogMetadata(), this.bookKeeper, this.config);
    }

    @Override
    public int getMaxAppendLength() {
        return MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        ensurePreconditions();
        return this.lastAppendAddress.get().getSequence();
    }

    @Override
    public long getEpoch() {
        ensurePreconditions();
        return getLogMetadata().getEpoch();
    }

    //endregion

    //region Appends

    /**
     * Attempts to write one append to BookKeeper.
     *
     * @param buffer A byte array representing the data to append.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed (successfully or not).
     */
    private CompletableFuture<LogAddress> tryAppend(byte[] buffer) {
        val result = new CompletableFuture<LogAddress>();
        tryAppend(buffer, getWriteLedger(), result);
        return result;
    }

    /**
     * Attempts to write one append to BookKeeper. This method auto-retries if the current ledger has been rolled over.
     *
     * @param buffer      A byte array representing the data to append.
     * @param writeLedger The Ledger to write to.
     * @param result      A Future to complete when the append succeeds or fails.
     */
    private void tryAppend(byte[] buffer, WriteLedger writeLedger, CompletableFuture<LogAddress> result) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "tryAppend", buffer.length, writeLedger.ledger.getId());
        AsyncCallback.AddCallback callback = (rc, handle, entryId, ctx) -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<LogAddress> completionFuture = (CompletableFuture<LogAddress>) ctx;
            try {
                assert handle.getId() == writeLedger.ledger.getId() : "LedgerHandle.Id mismatch. Expected " + writeLedger.ledger.getId() + ", actual " + handle.getId();
                if (rc != 0) {
                    if (rc == BKException.Code.LedgerClosedException) {
                        // LedgerClosed can happen because we just rolled over the ledgers. The way to detect this is to
                        // check if the ledger we were trying to write to is closed and whether we have a new ledger.
                        // If that's the case, retry the write with the new ledger.
                        WriteLedger currentLedger = getWriteLedger();
                        if (!currentLedger.ledger.isClosed() && currentLedger.ledger.getId() != writeLedger.ledger.getId()) {
                            tryAppend(buffer, currentLedger, result);
                            return;
                        }
                    }

                    handleWriteException(rc, completionFuture);
                    return;
                }

                // Successful write. Complete the callback future and update metrics.
                LedgerAddress address = new LedgerAddress(writeLedger.metadata.getSequence(), writeLedger.ledger.getId(), entryId);
                this.lastAppendAddress.set(address);
                completionFuture.complete(address);
            } catch (Throwable ex) {
                completionFuture.completeExceptionally(ex);
            }

            LoggerHelpers.traceLeave(log, this.traceObjectId, "tryAppend", traceId, buffer.length, writeLedger.ledger.getId(), !completionFuture.isCompletedExceptionally());
        };

        writeLedger.ledger.asyncAddEntry(buffer, callback, result);
    }

    /**
     * Handles a general Write exception.
     */
    @SneakyThrows(Throwable.class)
    private <T> T handleWriteException(Throwable ex) {
        if (ex instanceof ObjectClosedException && !this.closed.get()) {
            log.warn("{}: Caught ObjectClosedException but not closed; closing now.", this.traceObjectId, ex);
            close();
        } else if (isRetryable(ex)) {
            log.warn("{}: Caught retryable exception.", this.traceObjectId, ex);
        }

        // Rethrow the original exception so that the enclosing retry loop can handle it.
        throw ex;
    }

    /**
     * Handles an exception after a Write operation, converts it to a Pravega Exception and completes the given future
     * exceptionally using it.
     *
     * @param responseCode   The BookKeeper response code to interpret.
     * @param callbackFuture The Future to complete exceptionally.
     */
    private void handleWriteException(int responseCode, CompletableFuture<?> callbackFuture) {
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
            callbackFuture.completeExceptionally(ex);
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
     * 1. Create a copy of the metadata reflecting the truncation.
     * 2. Attempt to persist the metadata to ZooKeeper.
     * 3. Swap metadata pointers.
     * 4. Delete truncated-out ledgers.
     *
     * @param upToAddress The address up to which to truncate.
     */
    @SneakyThrows(DurableDataLogException.class)
    private void tryTruncate(LedgerAddress upToAddress) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "trytruncate", upToAddress);

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
            byte[] serializedMetadata = this.curatorClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.deserialize(serializedMetadata);
            result.withUpdateVersion(storingStatIn.getVersion());
            return result;
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("{}: No ZNode found for path '{}'.", this.traceObjectId, this.logNodePath, nne);
            return null;
        } catch (Exception ex) {
            throw new DataLogInitializationException(
                    String.format("Unable to load ZNode contents for path '%s'.", this.logNodePath), ex);
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
                this.curatorClient.create()
                                  .creatingParentsIfNeeded()
                                  .forPath(this.logNodePath, serializedMetadata);
                // Set version to 0 as that will match the ZNode's version.
                metadata.withUpdateVersion(0);
            } else {
                byte[] serializedMetadata = metadata.serialize();
                this.curatorClient.setData()
                                  .withVersion(metadata.getUpdateVersion())
                                  .forPath(this.logNodePath, serializedMetadata);

                // Increment the version to keep up with the ZNode's value (after writing it to ZK).
                metadata.withUpdateVersion(metadata.getUpdateVersion() + 1);
            }
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            // We were fenced out. Clean up and throw appropriate exception.
            throw new DataLogWriterNotPrimaryException(
                    String.format("Unable to acquire exclusive write lock for log (path = '%s').", this.logNodePath), keeperEx);
        } catch (Exception generalEx) {
            // General exception. Clean up and rethrow appropriate exception.
            throw new DataLogInitializationException(String.format("Unable to update ZNode for path '%s'.", this.logNodePath), generalEx);
        }

        log.info("{} Metadata persisted ({}).", this.traceObjectId, metadata);
    }

    //endregion

    //region Ledger Rollover

    /**
     * Triggers an asynchronous rollover, but only if both these conditions are met:
     * 1. The current Write Ledger has exceeded its maximum length.
     * 2. There isn't already a rollover in progress.
     */
    private void triggerRolloverIfNecessary() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "triggerRolloverIfNecessary");
        boolean trigger = getWriteLedger().ledger.getLength() >= this.config.getBkLedgerMaxSize()
                && this.rolloverInProgress.compareAndSet(false, true);
        if (trigger) {
            ExecutorServiceHelpers.execute(this::rollover,
                    ex -> log.error("{}: Rollover failure; log may be unusable.", ex),
                    () -> this.rolloverInProgress.set(false),
                    this.executorService);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "triggerRolloverIfNecessary", traceId, trigger);
    }

    /**
     * Executes a rollover using the following protocol (described in https://bookkeeper.apache.org/docs/r4.4.0/bookkeeperLedgers2Logs.html):
     * 1. Create a new ledger
     * 2. Add the new ledger the the metadata
     * 3. Update the metadata in ZooKeeper.
     * 4. Swap pointers to the active Write Ledger (all future writes will go to the new ledger).
     * 5. Close the previous ledger (and implicitly seal it).
     */
    private void rollover() throws DurableDataLogException {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollover");

        // Get latest version of the metadata.
        LogMetadata metadata = getLogMetadata();
        log.debug("{}: Rollover: got metadata '{}'.", this.traceObjectId, metadata);

        // Create new ledger.
        LedgerHandle newLedger = Ledgers.create(this.bookKeeper, this.config);
        log.debug("{}: Rollover: created new ledger {}.", this.traceObjectId, metadata, newLedger.getId());

        // Update the metadata.
        metadata = updateMetadata(metadata, newLedger);
        LedgerMetadata ledgerMetadata = metadata.getLedger(newLedger.getId());
        assert ledgerMetadata != null : "cannot find newly added ledger metadata";
        log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata, metadata);

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
        log.debug("{}: Rollover: swapped ledger and metadata pointers (Old = {}, New = {}) and closed old ledger.", this.traceObjectId, oldLedger.getId(), newLedger.getId());
        LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId);
    }

    //endregion

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
}
