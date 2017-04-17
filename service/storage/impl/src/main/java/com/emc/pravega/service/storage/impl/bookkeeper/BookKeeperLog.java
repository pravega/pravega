/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.DataLogInitializationException;
import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
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
    /**
     * Maximum append length, as specified by BookKeeper (this is hardcoded inside BookKeeper's code).
     */
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 100;

    /**
     * How many ledgers to fence out (from the end of the list) when acquiring lock.
     */
    private static final int MAX_FENCE_LEDGER_COUNT = 2;
    private final String logNodePath;
    private final CuratorFramework curatorClient;
    private final BookKeeper bookKeeper;
    private final BookKeeperConfig config;
    private final ScheduledExecutorService executorService;
    private final AtomicBoolean closed;
    private final Object ledgerLock = new Object();
    private final String traceObjectId;
    @GuardedBy("ledgerLock")
    private LedgerHandle writeLedger;
    @GuardedBy("ledgerLock")
    private LogMetadata logMetadata;

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
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // TODO: Close here.
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
            Stat nodeStat = new Stat();
            LogMetadata metadata = getMetadata(nodeStat);

            // Fence out ledgers.
            if (metadata != null) {
                fenceOut(metadata.getLedgerIds());
            }

            // Create new ledger.
            LedgerHandle newLedger = createNewLedger();

            // Update node with new ledger.
            metadata = updateMetadata(metadata, newLedger, nodeStat);
            this.writeLedger = newLedger;
            this.logMetadata = metadata;
        }
    }

    @Override
    public CompletableFuture<LogAddress> append(InputStream data, Duration timeout) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "initialize");
        // TODO: use the async write API.
        return null;
    }

    @Override
    public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
        // TODO: see PDP.
        return null;
    }

    @Override
    public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
        // TODO: use the async reader API
        return null;
    }

    @Override
    public int getMaxAppendLength() {
        return MAX_APPEND_LENGTH;
    }

    @Override
    public long getLastAppendSequence() {
        return 0;
    }

    @Override
    public long getEpoch() {
        return 0;
    }

    //endregion

    private LedgerHandle createNewLedger() throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() ->
                    this.bookKeeper.createLedger(
                            this.config.getBkEnsembleSize(),
                            this.config.getBkWriteQuorumSize(),
                            this.config.getBkAckQuorumSize(),
                            BookKeeper.DigestType.MAC,
                            this.config.getBkPassword()));
        } catch (BKException bkEx) {
            throw new DataLogInitializationException(
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

    private void fenceOut(List<Long> ledgerIds) throws DurableDataLogException {
        int fenceCount = Math.min(ledgerIds.size(), Math.max(1, MAX_FENCE_LEDGER_COUNT));
        for (int i = ledgerIds.size() - fenceCount - 1; i < ledgerIds.size(); i++) {
            long ledgerId = ledgerIds.get(i);
            try {
                Exceptions.handleInterrupted(() -> this.bookKeeper.openLedger(ledgerId, BookKeeper.DigestType.MAC, this.config.getBkPassword()));
                log.info("{}: Fenced out Ledger %d.", this.traceObjectId, ledgerId);
            } catch (BKException bkEx) {
                throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", ledgerId), bkEx);
            }
        }
    }

    private LogMetadata updateMetadata(LogMetadata currentMetadata, LedgerHandle newLedger, Stat metadataNodeStat) throws DurableDataLogException {
        try {
            if (currentMetadata == null) {
                currentMetadata = new LogMetadata(newLedger.getId());
                byte[] serializedMetadata = currentMetadata.serialize();
                this.curatorClient.create()
                                  .forPath(this.logNodePath, serializedMetadata);
            } else {
                currentMetadata = currentMetadata.addLedger(newLedger.getId(), true);
                byte[] serializedMetadata = currentMetadata.serialize();
                this.curatorClient.setData()
                                  .withVersion(metadataNodeStat.getVersion())
                                  .forPath(this.logNodePath, serializedMetadata);
            }
        } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException keeperEx) {
            // We were fenced out. Clean up and throw appropriate exception.
            handleMetadataUpdateException(keeperEx, newLedger, ex -> new DataLogWriterNotPrimaryException(
                    String.format("Unable to acquire exclusive write lock for log (path = '%s').", this.logNodePath), ex));
        } catch (Exception generalEx) {
            // General exception. Clean up and rethrow appropriate exception.
            handleMetadataUpdateException(generalEx, newLedger, ex -> new DataLogInitializationException(
                    String.format("Unable to update Log Metadata in ZooKeeper to path '%s'.", this.logNodePath), ex));
        }

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

    private LogMetadata getMetadata(Stat storingStatIn) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = this.curatorClient.getData().storingStatIn(storingStatIn).forPath(this.logNodePath);
            return LogMetadata.deserialize(serializedMetadata);
        } catch (KeeperException.NoNodeException nne) {
            // Node does not exist: this is the first time we are accessing this log.
            log.warn("{}: No node exists in ZooKeeper for path '{}'.", this.traceObjectId, this.logNodePath, nne);
            return null;
        } catch (Exception ex) {
            throw new DataLogInitializationException(
                    String.format("Unable to load Log Metadata for path '%s'.", this.logNodePath), ex);
        }
    }

    private String getLogNodePath(String zkNamespace, int logId) {
        // TODO: implement some sort of hierarchical scheme here.
        return String.format("%s/%s", zkNamespace, logId);
    }
}
