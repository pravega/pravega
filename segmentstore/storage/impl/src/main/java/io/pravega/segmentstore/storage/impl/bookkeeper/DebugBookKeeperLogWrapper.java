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

import com.google.common.base.Preconditions;
import io.pravega.common.util.CloseableIterator;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.DataLogInitializationException;
import io.pravega.segmentstore.storage.DebugDurableDataLogWrapper;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.ReadOnlyLogMetadata;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.WriteSettings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import lombok.val;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.Handle;
import org.apache.curator.framework.CuratorFramework;

/**
 * Wrapper for a BookKeeperLog which only exposes methods that should be used for debugging/admin tools.
 * NOTE: this class is not meant to be used for regular, production code. It exposes operations that should only be executed
 * from the admin tools.
 */
@Slf4j
public class DebugBookKeeperLogWrapper implements DebugDurableDataLogWrapper {
    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final BookKeeperLog bkLog;
    private final BookKeeper bkClient;
    private final BookKeeperConfig config;
    private final AtomicBoolean initialized;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DebugLogWrapper class.
     *
     * @param logId      The Id of the BookKeeperLog to wrap.
     * @param zkClient   A pointer to the CuratorFramework client to use.
     * @param bookKeeper A pointer to the BookKeeper client to use.
     * @param config     BookKeeperConfig to use.
     * @param executor   An Executor to use for async operations.
     */
    DebugBookKeeperLogWrapper(int logId, CuratorFramework zkClient, BookKeeper bookKeeper, BookKeeperConfig config, ScheduledExecutorService executor) {
        this.bkLog = new BookKeeperLog(logId, zkClient, bookKeeper, config, executor);
        this.bkClient = bookKeeper;
        this.config = config;
        this.initialized = new AtomicBoolean();
        this.traceObjectId = String.format("DebugBookKeeperLogWrapper[%s]", this.bkLog.getLogId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.bkLog.close();
    }

    //endregion

    //region Operations

    @Override
    public DurableDataLog asReadOnly() throws DataLogInitializationException {
        return new ReadOnlyBooKeeperLog(this.bkLog.getLogId(), this.bkLog.loadMetadata());
    }

    /**
     * Loads a fresh copy BookKeeperLog Metadata from ZooKeeper, without doing any sort of fencing or otherwise modifying
     * it.
     *
     * @return A new instance of the LogMetadata class, or null if no such metadata exists (most likely due to this being
     * the first time accessing this log).
     * @throws DataLogInitializationException If an Exception occurred.
     */
    @Override
    public ReadOnlyBookkeeperLogMetadata fetchMetadata() throws DataLogInitializationException {
        return this.bkLog.loadMetadata();
    }

    /**
     * Opens a ledger for reading purposes (does not fence it).
     *
     * @param ledgerMetadata LedgerMetadata for the ledger to open.
     * @return A BookKeeper LedgerHandle representing the ledger.
     * @throws DurableDataLogException If an exception occurred.
     */
    public ReadHandle openLedgerNoFencing(LedgerMetadata ledgerMetadata) throws DurableDataLogException {
        return Ledgers.openRead(ledgerMetadata.getLedgerId(), this.bkClient, this.config);
    }

    /**
     * Updates the Metadata for this BookKeeperLog in ZooKeeper by setting its Enabled flag to true.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void enable() throws DurableDataLogException {
        this.bkLog.enable();
    }

    /**
     * Open-Fences the BookKeeperLog (initializes it), then updates the Metadata for it in ZooKeeper by setting its
     * Enabled flag to false.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void disable() throws DurableDataLogException {
        initialize();
        this.bkLog.disable();
    }

    /**
     * Disables the BookKeeperLog and updates the metadata in ZooKeeper by setting its Enabled flag to false.
     * It ignores fence-out and should be used wisely and for administration purpose only.
     * @throws DurableDataLogException If an exception occurred.
     */
    public void markAsDisabled() throws DurableDataLogException {
        val metadata = this.bkLog.loadMetadata();
        forceMetadataOverWrite(metadata.asDisabled());
    }

    /**
     * Performs a {@link BookKeeperLog}-{@link LedgerHandle} reconciliation for this {@link BookKeeperLog} subject to the
     * following rules:
     * - Any {@link LedgerHandle}s that list this {@link BookKeeperLog} as their owner will be added to this {@link BookKeeperLog}'s
     * list of ledgers (if they're non-empty and haven't been truncated out).
     * - Any {@link LedgerMetadata} instances in this {@link BookKeeperLog} that point to inexistent {@link LedgerHandle}s
     * will be removed.
     *
     * @param candidateLedgers A List of {@link LedgerHandle}s that contain all the Ledgers that this {@link BookKeeperLog}
     *                         should contain. This could be the list of all BookKeeper Ledgers or a subset, as long as
     *                         it contains all Ledgers that list this {@link BookKeeperLog} as their owner.
     * @return True if something changed (and the metadata is updated), false otherwise.
     * @throws IllegalStateException   If this BookKeeperLog is not disabled.
     * @throws DurableDataLogException If an exception occurred while updating the metadata.
     */
    public boolean reconcileLedgers(List<? extends ReadHandle> candidateLedgers) throws DurableDataLogException {
        // Load metadata and verify if disabled (metadata may be null if it doesn't exist).
        LogMetadata metadata = this.bkLog.loadMetadata();
        final long highestLedgerId;
        if (metadata != null) {
            Preconditions.checkState(!metadata.isEnabled(), "BookKeeperLog is enabled; cannot reconcile ledgers.");
            int ledgerCount = metadata.getLedgers().size();
            if (ledgerCount > 0) {
                // Get the highest Ledger id from the list of ledgers.
                highestLedgerId = metadata.getLedgers().get(ledgerCount - 1).getLedgerId();
            } else if (metadata.getTruncationAddress() != null) {
                // All Ledgers have been truncated out. Get it from the Truncation Address.
                highestLedgerId = metadata.getTruncationAddress().getLedgerId();
            } else {
                // No information.
                highestLedgerId = Ledgers.NO_LEDGER_ID;
            }
        } else {
            // No metadata.
            highestLedgerId = Ledgers.NO_LEDGER_ID;
        }

        // First, we filter out any Ledger that does not reference this Log as their owner or that are empty.
        candidateLedgers = candidateLedgers
                .stream()
                .filter(lh -> Ledgers.getBookKeeperLogId(lh) == this.bkLog.getLogId()
                        && lh.getLength() > 0)
                .collect(Collectors.toList());

        // Begin reconstructing the Ledger List by eliminating references to inexistent ledgers.
        val newLedgerList = new ArrayList<LedgerMetadata>();
        if (metadata != null) {
            val candidateLedgerIds = candidateLedgers.stream().map(Handle::getId).collect(Collectors.toSet());
            metadata.getLedgers().stream()
                    .filter(lm -> candidateLedgerIds.contains(lm.getLedgerId()))
                    .forEach(newLedgerList::add);
        }

        // Find ledgers that should be in the log but are not referenced. Only select ledgers which have their Id greater
        // than the Id of the last ledger used in this Log (Id are assigned monotonically increasing, and we don't want
        // to add already truncated out ledgers).
        val seq = new AtomicInteger(newLedgerList.isEmpty() ? 0 : newLedgerList.get(newLedgerList.size() - 1).getSequence());
        candidateLedgers
                .stream()
                .filter(lh -> lh.getId() > highestLedgerId)
                .forEach(lh -> newLedgerList.add(new LedgerMetadata(lh.getId(), seq.incrementAndGet())));

        // Make sure the ledgers are properly sorted.
        newLedgerList.sort(Comparator.comparingLong(LedgerMetadata::getLedgerId));

        // Determine if anything changed.
        boolean changed = metadata == null || metadata.getLedgers().size() != newLedgerList.size();
        if (!changed) {
            for (int i = 0; i < newLedgerList.size(); i++) {
                if (metadata.getLedgers().get(i).getLedgerId() != newLedgerList.get(i).getLedgerId()) {
                    changed = true;
                    break;
                }
            }
        }

        // Update metadata in ZooKeeper, but only if it has changed.
        if (changed) {
            val newMetadata = LogMetadata
                    .builder()
                    .enabled(false)
                    .epoch(getOrDefault(metadata, LogMetadata::getEpoch, LogMetadata.INITIAL_EPOCH) + 1)
                    .truncationAddress(getOrDefault(metadata, LogMetadata::getTruncationAddress, LogMetadata.INITIAL_TRUNCATION_ADDRESS))
                    .updateVersion(getOrDefault(metadata, LogMetadata::getUpdateVersion, LogMetadata.INITIAL_VERSION))
                    .ledgers(newLedgerList)
                    .build();
            this.bkLog.overWriteMetadata(newMetadata);
        }

        return changed;
    }

    /**
     * Allows to overwrite the metadata of a BookkeeperLog. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged BookkeeperLog).
     *
     * @param metadata New metadata to set in the original BookkeeperLog metadata path.
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void forceMetadataOverWrite(ReadOnlyLogMetadata metadata) throws DurableDataLogException {
        try {
            byte[] serializedMetadata = LogMetadata.SERIALIZER.serialize((LogMetadata) metadata).getCopy();
            this.bkLog.getZkClient().setData().forPath(this.bkLog.getLogNodePath(), serializedMetadata);
        } catch (Exception e) {
            throw new DurableDataLogException("Problem overwriting Bookkeeper Log metadata.", e);
        }
    }

    /**
     * Gets the current log metadata and overrides the epoch in it with the passed epoch.
     *
     * @param epoch Epoch to be overridden
     * @throws DurableDataLogException thrown in case of issues persisting the metadata.
     */
    public void overrideEpochInMetadata(long epoch) throws DurableDataLogException {
        LogMetadata metadata = this.bkLog.loadMetadata();
        val newMetadata = LogMetadata
                .builder()
                .enabled(metadata.isEnabled())
                .epoch(epoch)
                .truncationAddress(getOrDefault(metadata, LogMetadata::getTruncationAddress, LogMetadata.INITIAL_TRUNCATION_ADDRESS))
                .updateVersion(getOrDefault(metadata, LogMetadata::getUpdateVersion, LogMetadata.INITIAL_VERSION))
                .ledgers(getOrDefault(metadata, LogMetadata::getLedgers, new ArrayList<>()))
                .build();
        forceMetadataOverWrite(newMetadata);
    }

    /**
     * Delete the metadata of the BookkeeperLog in Zookeeper. CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged BookkeeperLog).
     *
     * @throws DurableDataLogException in case there is a problem managing metadata from Zookeeper.
     */
    @Override
    public void deleteDurableLogMetadata() throws DurableDataLogException {
        try {
            this.bkLog.getZkClient().delete().forPath(this.bkLog.getLogNodePath());
        } catch (Exception e) {
            throw new DurableDataLogException("Problem deleting Bookkeeper Log metadata.", e);
        }
    }

    /**
     * Deletes all the ledgers of the log starting (including) given starting ledger id.
     * CAUTION: This is a destructive operation and should be
     * used wisely for administration purposes (e.g., repair a damaged BookkeeperLog).
     *
     * @param startId It refers to the ledger id from where deletion happens.
     * @return The total number of ledgers deleted.
     * @throws DurableDataLogException in case there is a problem while deleting ledger.
     */
    public int deleteLedgersStartingWithId(long startId) throws DurableDataLogException {
        LogMetadata metadata = this.bkLog.loadMetadata();
        List<LedgerMetadata> ledgers = metadata.getLedgers();
        List<Long> ids = ledgers.stream()
                .map(ledgerMetadata -> ledgerMetadata.getLedgerId())
                .collect(Collectors.toList());
        if (!ids.contains(startId)) {
            throw new DurableDataLogException(String.format("No such ledger exist with ledger id: %d.", this.bkLog.getLogId()));
        }
        // Start deleting the ledgers with starting ledger id
        AtomicInteger count = new AtomicInteger(0);
        val ledgersToDelete = ids.stream()
                .filter(ledgerId -> ledgerId >= startId )
                .collect(Collectors.toList());

        ledgersToDelete.forEach(id -> {
            try {
                Ledgers.delete(id, this.bkClient);
                log.info("{}: Deleted ledger {}.", this.traceObjectId, id);
                count.incrementAndGet();
            } catch (DurableDataLogException ex) {
                log.error("{}: Unable to delete ledger {}.", this.traceObjectId, id, ex);
            }
        });
        return count.get();
    }

    private void initialize() throws DurableDataLogException {
        if (this.initialized.compareAndSet(false, true)) {
            try {
                this.bkLog.initialize(DEFAULT_TIMEOUT);
            } catch (Exception ex) {
                this.initialized.set(false);
                throw ex;
            }
        }
    }

    private <T> T getOrDefault(LogMetadata metadata, Function<LogMetadata, T> getter, T defaultValue) {
        return metadata == null ? defaultValue : getter.apply(metadata);
    }

    //endregion

    //region ReadOnlyBookKeeperLog

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private class ReadOnlyBooKeeperLog implements DurableDataLog {
        private final int logId;
        private final LogMetadata logMetadata;

        @Override
        public void close() {
            // Nothing to do.
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader() {
            return new LogReader(this.logId, this.logMetadata, DebugBookKeeperLogWrapper.this.bkClient, DebugBookKeeperLogWrapper.this.config);
        }

        @Override
        public WriteSettings getWriteSettings() {
            return new WriteSettings(BookKeeperConfig.MAX_APPEND_LENGTH,
                    Duration.ofMillis(BookKeeperConfig.BK_WRITE_TIMEOUT.getDefaultValue()),
                    BookKeeperConfig.MAX_OUTSTANDING_BYTES.getDefaultValue());
        }

        @Override
        public ReadOnlyLogMetadata loadMetadata() throws DataLogInitializationException {
            throw new DataLogInitializationException("Unsupported operation");
        }

        @Override
        public long getEpoch() {
            return this.logMetadata.getEpoch();
        }

        @Override
        public void overrideEpoch(long epoch) throws DurableDataLogException {
            throw new DataLogInitializationException("Unsupported operation");
        }

        @Override
        public QueueStats getQueueStatistics() {
            return null;
        }

        @Override
        public void registerQueueStateChangeListener(ThrottleSourceListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initialize(Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disable() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public CompletableFuture<LogAddress> append(CompositeArrayView data, Duration timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncate(LogAddress upToAddress, Duration timeout) {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}
