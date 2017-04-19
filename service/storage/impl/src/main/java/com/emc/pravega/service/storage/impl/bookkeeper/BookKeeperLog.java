/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.Timer;
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
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
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
    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 100;

    /**
     * How many ledgers to fence out (from the end of the list) when acquiring lock.
     */
    private static final int MIN_FENCE_LEDGER_COUNT = 2;
    private static final BookKeeper.DigestType LEDGER_DIGEST_TYPE = BookKeeper.DigestType.MAC;
    private final String logNodePath;
    private final CuratorFramework curatorClient;
    private final BookKeeper bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object ledgerLock = new Object();
    private final String traceObjectId;
    private final Retry.RetryAndThrowBase<Exception> retryPolicy;
    private final AtomicReference<LedgerAddress> lastAppendAddress;
    @GuardedBy("ledgerLock")
    private WriteLedger writeLedger;
    @GuardedBy("ledgerLock")
    private LogMetadata logMetadata;
    @GuardedBy("ledgerLock")
    private CompletableFuture<Void> ledgerRollover;

    //region Constructor

    BookKeeperLog(int logId, CuratorFramework curatorClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executorService) {
        Preconditions.checkArgument(logId >= 0, "logId must be a non-negative integer.");
        Preconditions.checkNotNull(curatorClient, "curatorClient");
        Preconditions.checkNotNull(bookKeeper, "bookKeeper");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executorService, "executorService");

        this.curatorClient = curatorClient;
        this.bookKeeper = bookKeeper;
        this.config = config;
        this.executorService = executorService;
        this.closed = new AtomicBoolean();
        this.logNodePath = getLogNodePath(this.config.getNamespace(), logId);
        this.lastAppendAddress = new AtomicReference<>(new LedgerAddress(0, 0));
        this.traceObjectId = String.format("Log[%d]", logId);
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
            synchronized (this.ledgerLock) {
                writeLedger = this.writeLedger;
                this.writeLedger = null;
                this.logMetadata = null;
            }

            if (writeLedger != null) {
                try {
                    closeLedger(writeLedger.ledger);
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
        synchronized (this.ledgerLock) {
            Preconditions.checkState(this.writeLedger == null, "BookKeeperLog is already initialized.");
            assert this.logMetadata == null : "writeLedger == null but logMetadata != null";

            // Get metadata about the current state of the log, if any.
            LogMetadata metadata = loadMetadata();

            // Fence out ledgers.
            if (metadata != null) {
                fenceOut(metadata.getLedgers());
            }

            // Create new ledger.
            LedgerHandle newLedger = createNewLedger();

            // Update node with new ledger.
            metadata = updateMetadata(metadata, newLedger);
            LedgerMetadata ledgerMetadata = metadata.getLedgerMetadata(newLedger.getId());
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
        CompletableFuture<LogAddress> result = waitForRolloverIfNecessary().thenComposeAsync(
                v -> this.retryPolicy.runAsync(() -> appendInternal(buffer).exceptionally(this::handleWriteException), this.executorService),
                this.executorService);

        // Post append tasks. We do not need to wait for these to happen before returning the call.
        result.thenAcceptAsync(address -> {
            // After every append, check if we need to trigger a rollover.
            triggerRolloverIfNecessary();

            // Update metrics and take care of other logging tasks.
            Metrics.WRITE_LATENCY.reportSuccessEvent(timer.getElapsed());
            Metrics.WRITE_BYTES.add(buffer.length);
            LoggerHelpers.traceLeave(log, this.traceObjectId, "append", traceId, address, buffer.length);
        }, this.executorService);
        return result;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        ensurePreconditions();
        // TODO: see PDP.
        return null;
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        ensurePreconditions();
        val metadata = getLogMetadata();
        val truncatedAddress = metadata.getTruncationAddress();
        final long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "getReader", truncatedAddress);
        val result = new LogReader(metadata, this.bookKeeper, this.config);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "getReader", traceId);
        return result;
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

    private CompletableFuture<LogAddress> appendInternal(byte[] buffer) {
        CompletableFuture<LogAddress> result = new CompletableFuture<>();
        val writeLedger = getWriteLedger();
        AsyncCallback.AddCallback callback = (rc, handle, entryId, ctx) -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<LogAddress> completionFuture = (CompletableFuture<LogAddress>) ctx;
            try {
                assert handle.getId() == writeLedger.ledger.getId() : "LedgerHandle.Id mismatch. Expected " + writeLedger.ledger.getId() + ", actual " + handle.getId();
                if (rc != 0) {
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
        };

        writeLedger.ledger.asyncAddEntry(buffer, callback, result);
        return result;
    }

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

    private void handleWriteException(int responseCode, CompletableFuture<?> callbackFuture) {
        assert responseCode != 0 : "cannot handle an exception when responseCode == 0";
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

    //endregion

    //region Metadata Management

    /**
     * Loads the metadata for the current log, as stored in ZooKeeper.
     *
     * @return A new LogMetadata object with the desired information, or null if no such node exists.
     * @throws DurableDataLogException If an Exception (other than NoNodeException) occurred.
     */
    private LogMetadata loadMetadata() throws DurableDataLogException {
        try {
            Stat storingStatIn = new Stat();
            byte[] serializedMetadata = this.curatorClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            LogMetadata result = LogMetadata.deserialize(serializedMetadata);
            result.setUpdateVersion(storingStatIn.getVersion());
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

    private LogMetadata updateMetadata(LogMetadata currentMetadata, LedgerHandle newLedger) throws DurableDataLogException {
        try {
            if (currentMetadata == null) {
                // This is the first ledger ever in the metadata.
                currentMetadata = new LogMetadata(newLedger.getId());
                byte[] serializedMetadata = currentMetadata.serialize();
                this.curatorClient.create()
                                  .creatingParentsIfNeeded()
                                  .forPath(this.logNodePath, serializedMetadata);
                currentMetadata.setUpdateVersion(0); // Initial ZNode creation sets the version to 0.
            } else {
                currentMetadata = currentMetadata.addLedger(newLedger.getId(), true);
                byte[] serializedMetadata = currentMetadata.serialize();
                this.curatorClient.setData()
                                  .withVersion(currentMetadata.getUpdateVersion())
                                  .forPath(this.logNodePath, serializedMetadata);

                // Increment the version to keep up with the ZNode's value.
                currentMetadata.setUpdateVersion(currentMetadata.getUpdateVersion() + 1);
            }
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            // We were fenced out. Clean up and throw appropriate exception.
            handleMetadataUpdateException(keeperEx, newLedger, ex -> new DataLogWriterNotPrimaryException(
                    String.format("Unable to acquire exclusive write lock for log (path = '%s').", this.logNodePath), ex));
        } catch (Exception generalEx) {
            // General exception. Clean up and rethrow appropriate exception.
            handleMetadataUpdateException(generalEx, newLedger, ex -> new DataLogInitializationException(
                    String.format("Unable to update ZNode for path '%s'.", this.logNodePath), ex));
        }

        log.info("{} Metadata updated ({}).", this.traceObjectId, currentMetadata);
        return currentMetadata;
    }

    private void handleMetadataUpdateException(Exception ex, LedgerHandle newLedger, Function<Exception, DurableDataLogException> exceptionConverter) throws DurableDataLogException {
        try {
            deleteLedger(newLedger.getId());
        } catch (Exception deleteEx) {
            log.warn("{}: Unable to delete newly created ledger {}.", this.traceObjectId, newLedger.getId(), deleteEx);
            ex.addSuppressed(deleteEx);
        }

        throw exceptionConverter.apply(ex);
    }

    //endregion

    //region Ledger Management

    /**
     * Creates a new Ledger in BookKeeper.
     *
     * @return A LedgerHandle for the new ledger.
     * @throws DataLogNotAvailableException If BookKeeper is unavailable or the ledger could not be created because an
     *                                      insufficient number of Bookies are available.
     * @throws DurableDataLogException      If another exception occurred.
     */
    private LedgerHandle createNewLedger() throws DurableDataLogException {
        try {
            LedgerHandle result = Exceptions.handleInterrupted(() ->
                    this.bookKeeper.createLedger(
                            this.config.getBkEnsembleSize(),
                            this.config.getBkWriteQuorumSize(),
                            this.config.getBkAckQuorumSize(),
                            LEDGER_DIGEST_TYPE,
                            this.config.getBkPassword()));
            log.info("{}: Created Ledger {}.", this.traceObjectId, result.getId());
            return result;
        } catch (BKException.BKNotEnoughBookiesException bkEx) {
            throw new DataLogNotAvailableException(
                    String.format("Unable to create new BookKeeper Ledger (Path = '%s').", this.logNodePath), bkEx);
        } catch (BKException bkEx) {
            throw new DurableDataLogException(
                    String.format("Unable to create new BookKeeper Ledger (Path = '%s').", this.logNodePath), bkEx);
        }
    }

    private void deleteLedger(long ledgerId) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(() -> this.bookKeeper.deleteLedger(ledgerId));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to delete Ledger %d.", ledgerId), bkEx);
        }
    }

    private void fenceOut(List<LedgerMetadata> ledgerIds) throws DurableDataLogException {
        // Fence out the last few ledgers, in descending order. We need to fence out at least MIN_FENCE_LEDGER_COUNT,
        // but we also need to find the LedgerId & EntryID of the last written entry (it's possible that the last few
        // ledgers are empty, so we need to look until we find one).
        int count = 0;
        val iterator = ledgerIds.listIterator(ledgerIds.size());
        LedgerAddress lastAddress = null;
        while (iterator.hasPrevious() && (count < MIN_FENCE_LEDGER_COUNT || lastAddress == null)) {
            LedgerMetadata ledgerMetadata = iterator.previous();
            LedgerHandle handle = openLedger(ledgerMetadata.getLedgerId(), this.bookKeeper, this.config);
            if (lastAddress == null && handle.getLastAddConfirmed() >= 0) {
                lastAddress = new LedgerAddress(ledgerMetadata.getSequence(), handle.getId(), handle.getLastAddConfirmed());
            }

            closeLedger(handle);
            log.info("{}: Fenced out Ledger {}.", this.traceObjectId, ledgerMetadata);
            count++;
        }

        if (lastAddress != null) {
            this.lastAppendAddress.set(lastAddress);
        }
    }

    private static LedgerHandle openLedger(long ledgerId, BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() -> bookKeeper.openLedger(ledgerId, LEDGER_DIGEST_TYPE, config.getBkPassword()));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", ledgerId), bkEx);
        }
    }

    private static void closeLedger(LedgerHandle handle) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(handle::close);
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", handle.getId()), bkEx);
        }
    }

    //endregion

    //region Ledger Rollover

    private CompletableFuture<Void> waitForRolloverIfNecessary() {
        CompletableFuture<Void> rolloverCompletion;
        synchronized (this.ledgerLock) {
            rolloverCompletion = this.ledgerRollover;
        }

        if (rolloverCompletion == null) {
            rolloverCompletion = CompletableFuture.completedFuture(null);
        } else {
            log.debug("{}: Waiting for Ledger Rollover to complete.", this.traceObjectId);
        }

        return rolloverCompletion;
    }

    private void triggerRolloverIfNecessary() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "triggerRolloverIfNecessary");
        CompletableFuture<Void> rolloverCompletion;
        synchronized (this.ledgerLock) {
            if (this.ledgerRollover != null || this.writeLedger.ledger.getLength() < this.config.getBkLedgerMaxSize()) {
                // Rollover already in progress or no need for rollover yet.
                LoggerHelpers.traceLeave(log, this.traceObjectId, "triggerRolloverIfNecessary", traceId, false);
                return;
            }

            this.ledgerRollover = new CompletableFuture<>();
            this.ledgerRollover.thenRun(() -> {
                // Cleanup after successful execution only. A failed execution means we left the log in a weird state and
                // we cannot recover from that.
                synchronized (this.ledgerLock) {
                    this.ledgerRollover = null;
                }
            });
            rolloverCompletion = this.ledgerRollover;
        }

        FutureHelpers.completeAfter(() -> CompletableFuture.runAsync(this::rollover, this.executorService), rolloverCompletion);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "triggerRolloverIfNecessary", traceId, true);
    }

    @SneakyThrows(DurableDataLogException.class)
    private void rollover() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "rollover");

        // 1. Get latest version of the metadata (and compare against current metadata).
        LogMetadata metadata = getLogMetadata();

        log.debug("{}: Rollover: got metadata '{}'.", this.traceObjectId, metadata);

        // 2.Create new ledger.
        LedgerHandle newLedger = createNewLedger();
        log.debug("{}: Rollover: created new ledger {}.", this.traceObjectId, metadata, newLedger.getId());

        // 3. Update the metadata.
        metadata = updateMetadata(metadata, newLedger);
        LedgerMetadata ledgerMetadata = metadata.getLedgerMetadata(newLedger.getId());
        assert ledgerMetadata != null : "cannot find newly added ledger metadata";
        log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata, metadata);

        // 4. Close the current ledger and update pointers to the new ledger and metadata.
        synchronized (this.ledgerLock) {
            closeLedger(this.writeLedger.ledger);
            this.writeLedger = new WriteLedger(newLedger, ledgerMetadata);
            this.logMetadata = metadata;
        }

        log.debug("{}: Rollover: swapped ledger and metadata pointers.", this.traceObjectId, metadata, metadata);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "rollover", traceId);
    }

    //endregion

    private String getLogNodePath(String zkNamespace, int logId) {
        // TODO: implement some sort of hierarchical scheme here.
        return String.format("%s/%s", zkNamespace, logId);
    }

    private LogMetadata getLogMetadata() {
        synchronized (this.ledgerLock) {
            return this.logMetadata;
        }
    }

    private WriteLedger getWriteLedger() {
        synchronized (this.ledgerLock) {
            return this.writeLedger;
        }
    }

    private void ensurePreconditions() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        synchronized (this.ledgerLock) {
            Preconditions.checkState(this.writeLedger != null, "BookKeeperLog is not initialized.");
            assert this.logMetadata != null : "writeLedger != null but logMetadata == null";
        }
    }

    @RequiredArgsConstructor
    private static class WriteLedger {
        final LedgerHandle ledger;
        final LedgerMetadata metadata;

        @Override
        public String toString() {
            return String.format("%s, Length = %d, Closed = %s", this.metadata, this.ledger.getLength(), this.ledger.isClosed());
        }
    }

    //region LogReader

    @Slf4j
    @ThreadSafe
    private static class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        private final BookKeeper bookKeeper;
        private final LogMetadata metadata;
        private final AtomicBoolean closed;
        private final BookKeeperConfig config;
        private LedgerMetadata currentLedgerMetadata;
        private LedgerHandle currentLedger;
        private Iterator<LedgerEntry> currentLedgerReader;

        LogReader(LogMetadata metadata, BookKeeper bookKeeper, BookKeeperConfig config) {
            this.metadata = metadata;
            this.bookKeeper = bookKeeper;
            this.config = config;
            this.closed = new AtomicBoolean();
        }

        @Override
        public void close() {
            if (!this.closed.getAndSet(true)) {
                this.currentLedgerReader = null;
                if (this.currentLedger != null) {
                    try {
                        closeLedger(this.currentLedger);
                    } catch (DurableDataLogException bkEx) {
                        log.error("Unable to close LedgerHandle for Ledger {}.", this.currentLedger.getId(), bkEx);
                    }

                    this.currentLedger = null;
                }

                this.currentLedgerMetadata = null;
            }
        }

        @Override
        public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
            Exceptions.checkNotClosed(this.closed.get(), this);

            if (this.currentLedgerReader == null) {
                // First time we call this. Locate the first ledger based on the metadata truncation address.
                openNextLedger(this.metadata.nextAddress(this.metadata.getTruncationAddress(), 0));
            }

            while (this.currentLedgerReader != null && !this.currentLedgerReader.hasNext()) {
                // We have reached the end of the current ledger. Find next one (the loop accounts for empty ledgers).
                val lastAddress = new LedgerAddress(this.currentLedgerMetadata.getSequence(), this.currentLedger.getId(), this.currentLedger.getLastAddConfirmed());
                closeLedger(this.currentLedger);
                openNextLedger(this.metadata.nextAddress(lastAddress, this.currentLedger.getLastAddConfirmed()));
            }

            // Try to read from the current reader.
            if (this.currentLedgerReader == null) {
                return null;
            }

            val nextEntry = this.currentLedgerReader.next();

            byte[] payload = nextEntry.getEntry();// TODO: this also exposes an InputStream, which may be more efficient.
            val address = new LedgerAddress(this.currentLedgerMetadata.getSequence(), this.currentLedger.getId(), nextEntry.getEntryId());
            return new ReadItem(payload, address);
        }

        private void openNextLedger(LedgerAddress address) throws DurableDataLogException {
            if (address == null) {
                // We have reached the end.
                close();
                return;
            }
            LedgerMetadata metadata = this.metadata.getLedgerMetadata(address.getLedgerId());
            assert metadata != null : "no LedgerMetadata could be found with valid LedgerAddress " + address;

            // Open the ledger.
            this.currentLedgerMetadata = metadata;
            this.currentLedger = openLedger(metadata.getLedgerId(), this.bookKeeper, this.config);

            long lastEntryId = this.currentLedger.getLastAddConfirmed();
            if (lastEntryId < address.getEntryId()) {
                // This ledger is empty.
                closeLedger(this.currentLedger);
                return;
            }

            try {
                this.currentLedgerReader = Exceptions.handleInterrupted(() -> Iterators.forEnumeration(this.currentLedger.readEntries(address.getEntryId(), lastEntryId)));
            } catch (Exception ex) {
                close();
                throw new DurableDataLogException("Error while reading from BookKeeper.", ex);
            }
        }

        @RequiredArgsConstructor
        private static class ReadItem implements DurableDataLog.ReadItem {
            @Getter
            private final byte[] payload;
            @Getter
            private final LedgerAddress address;

            @Override
            public String toString() {
                return String.format("%s, Length = %d.", getAddress(), getPayload().length);
            }
        }
    }

    //endregion
}
