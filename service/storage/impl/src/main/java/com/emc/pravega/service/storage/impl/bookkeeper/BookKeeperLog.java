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
    private static final int MAX_FENCE_LEDGER_COUNT = 2;
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
    @GuardedBy("ledgerLock")
    private LedgerHandle writeLedger;
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
            LedgerHandle writeLedger;
            synchronized (this.ledgerLock) {
                writeLedger = this.writeLedger;
                this.writeLedger = null;
                this.logMetadata = null;
            }

            if (writeLedger != null) {
                try {
                    closeLedger(writeLedger);
                } catch (DurableDataLogException bkEx) {
                    log.error("{}: Unable to close LedgerHandle for Ledger {}.", this.traceObjectId, writeLedger.getId(), bkEx);
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
                long currentLogLength = fenceOut(metadata.getLedgers());
                assert currentLogLength >= 0 : "currentLogLength must be non-negative " + currentLogLength;
                metadata.setLastAppendSequence(currentLogLength);
            }

            // Create new ledger.
            LedgerHandle newLedger = createNewLedger();

            // Update node with new ledger.
            metadata = updateMetadata(metadata, newLedger);
            this.writeLedger = newLedger;
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
        return getLogMetadata().getLastAppendSequence();
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
        AsyncCallback.AddCallback callback = (rc, handle, entryId, ctx) -> {
            @SuppressWarnings("unchecked")
            CompletableFuture<LogAddress> completionFuture = (CompletableFuture<LogAddress>) ctx;
            if (rc != 0) {
                handleWriteException(rc, completionFuture);
                return;
            }

            // Successful write. Complete the callback future and update metrics.
            long seqNo = getLogMetadata().recordWrite(buffer.length);
            LedgerAddress address = new LedgerAddress(seqNo, handle.getId(), entryId);
            completionFuture.complete(address);
        };

        getWriteLedger().asyncAddEntry(buffer, callback, result);
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
                val lm = new LogMetadata.LedgerMetadata(newLedger.getId(), currentMetadata.getLastAppendSequence());
                currentMetadata = currentMetadata.addLedger(lm, true);
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

    private long fenceOut(List<LogMetadata.LedgerMetadata> ledgerIds) throws DurableDataLogException {
        int fenceCount = Math.min(ledgerIds.size(), Math.max(1, MAX_FENCE_LEDGER_COUNT));
        long lastOffset = Long.MIN_VALUE;
        for (int i = ledgerIds.size() - fenceCount; i < ledgerIds.size(); i++) {
            LogMetadata.LedgerMetadata lm = ledgerIds.get(i);
            LedgerHandle lh = openLedger(lm.getLedgerId(), this.bookKeeper, this.config);
            lastOffset = lm.getStartOffset() + lh.getLength();
            closeLedger(lh);
            log.info("{}: Fenced out Ledger {}.", this.traceObjectId, lm);
        }

        return lastOffset;
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
            if (this.ledgerRollover != null || this.writeLedger.getLength() < this.config.getBkLedgerMaxSize()) {
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
        //        if (metadata == null || !metadata.compareVersions(getMetadata())) {
        //            throw new DataLogWriterNotPrimaryException(
        //                    String.format("Metadata missing or version mismatch. Existing = '%s', Actual = '%s'.", getMetadata(), metadata));
        //        }

        log.debug("{}: Rollover: got metadata '{}'.", this.traceObjectId, metadata);

        // 2.Create new ledger.
        LedgerHandle newLedger = createNewLedger();
        log.debug("{}: Rollover: created new ledger {}.", this.traceObjectId, metadata, newLedger.getId());

        // 3. Update the metadata.
        metadata = updateMetadata(metadata, newLedger);
        log.debug("{}: Rollover: updated metadata '{}.", this.traceObjectId, metadata, metadata);

        // 4. Close the current ledger and update pointers to the new ledger and metadata.
        synchronized (this.ledgerLock) {
            closeLedger(this.writeLedger);
            this.writeLedger = newLedger;
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

    private LedgerHandle getWriteLedger() {
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

    //region LogReader

    @Slf4j
    @ThreadSafe
    private static class LogReader implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {
        private final BookKeeper bookKeeper;
        private final LogMetadata metadata;
        private final AtomicBoolean closed;
        private final BookKeeperConfig config;
        private LogMetadata.LedgerMetadata currentLedgerMetadata;
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
                openNextLedger(this.metadata.getTruncationAddress());
            }

            while (this.currentLedgerReader != null && !this.currentLedgerReader.hasNext()) {
                // We have reached the end of the current ledger. Find next one (the loop accounts for empty ledgers).
                closeLedger(this.currentLedger);
                openNextLedger(new LedgerAddress(0, this.currentLedger.getId() + 1, -1));
            }

            // Try to read from the current reader.
            if (this.currentLedgerReader == null) {
                return null;
            }

            val nextEntry = this.currentLedgerReader.next();

            // Sequence has to be exactly the same as the one we returned for append() for this entry. The value is the
            // offset of the last byte of this entry, which is conveniently calculated using the start offset for the
            // current ledger and the LedgerEntry.getLength() method, which returns the length of the ledger up to and
            // including this entry.
            val sequence = this.currentLedgerMetadata.getStartOffset() + nextEntry.getLength();
            byte[] payload = nextEntry.getEntry();// TODO: this also exposes an InputStream, which may be more efficient.
            return new ReadItem(payload, new LedgerAddress(sequence, nextEntry.getLedgerId(), nextEntry.getEntryId()));
        }

        private void openNextLedger(LedgerAddress afterAddress) throws DurableDataLogException {
            LogMetadata.LedgerMetadata metadata = findNextLedgerMetadata(afterAddress);
            if (metadata == null) {
                // None of the ledgers are after the truncation address, so we have nothing to read.
                close();
                return;
            }

            // Open the first ledger.
            this.currentLedgerMetadata = metadata;
            this.currentLedger = openLedger(metadata.getLedgerId(), this.bookKeeper, this.config);
            long firstEntryId = afterAddress.getEntryId() + 1;
            long lastEntryId = this.currentLedger.getLastAddConfirmed();
            if (lastEntryId < firstEntryId) {
                // This ledger is empty.
                closeLedger(this.currentLedger);
                return;
            }

            try {
                this.currentLedgerReader = Exceptions.handleInterrupted(() -> Iterators.forEnumeration(this.currentLedger.readEntries(firstEntryId, lastEntryId)));
            } catch (Exception ex) {
                close();
                throw new DurableDataLogException("Error while reading from BookKeeper.", ex);
            }
        }

        private LogMetadata.LedgerMetadata findNextLedgerMetadata(LedgerAddress afterAddress) {
            for (LogMetadata.LedgerMetadata lm : this.metadata.getLedgers()) {
                if (lm.getLedgerId() >= afterAddress.getLedgerId()) {
                    return lm;
                }
            }

            // If we get here, it means all ledgers have been truncated out. Nothing more we can do.
            return null;
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
