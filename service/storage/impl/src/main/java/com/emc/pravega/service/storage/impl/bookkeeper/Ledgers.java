/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.DataLogNotAvailableException;
import com.emc.pravega.service.storage.DurableDataLogException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * General utilities pertaining to BookKeeper Ledgers.
 */
@Slf4j
final class Ledgers {
    /**
     * How many ledgers to fence out (from the end of the list) when acquiring lock.
     */
    private static final int MIN_FENCE_LEDGER_COUNT = 2;
    private static final BookKeeper.DigestType LEDGER_DIGEST_TYPE = BookKeeper.DigestType.MAC;

    /**
     * Creates a new Ledger in BookKeeper.
     *
     * @return A LedgerHandle for the new ledger.
     * @throws DataLogNotAvailableException If BookKeeper is unavailable or the ledger could not be created because an
     *                                      insufficient number of Bookies are available. The causing exception is wrapped
     *                                      inside it.
     * @throws DurableDataLogException      If another exception occurred. The causing exception is wrapped inside it.
     */
    static LedgerHandle create(BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() ->
                    bookKeeper.createLedger(
                            config.getBkEnsembleSize(),
                            config.getBkWriteQuorumSize(),
                            config.getBkAckQuorumSize(),
                            LEDGER_DIGEST_TYPE,
                            config.getBkPassword()));
        } catch (BKException.BKNotEnoughBookiesException bkEx) {
            throw new DataLogNotAvailableException("Unable to create new BookKeeper Ledger.", bkEx);
        } catch (BKException bkEx) {
            throw new DurableDataLogException("Unable to create new BookKeeper Ledger.", bkEx);
        }
    }

    /**
     * Opens a ledger. This operation also fences out the ledger in case anyone else was writing to it.
     *
     * @param ledgerId   The Id of the Ledger to open.
     * @param bookKeeper A references to the BookKeeper client to use.
     * @param config     Configuration to use.
     * @return A LedgerHandle for the newly opened ledger.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static LedgerHandle openFence(long ledgerId, BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() -> bookKeeper.openLedger(ledgerId, LEDGER_DIGEST_TYPE, config.getBkPassword()));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", ledgerId), bkEx);
        }
    }

    /**
     * Opens a ledger for reading. This operation does not fence out the ledger.
     *
     * @param ledgerId   The Id of the Ledger to open.
     * @param bookKeeper A references to the BookKeeper client to use.
     * @param config     Configuration to use.
     * @return A LedgerHandle for the newly opened ledger.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static LedgerHandle openRead(long ledgerId, BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() -> bookKeeper.openLedgerNoRecovery(ledgerId, LEDGER_DIGEST_TYPE, config.getBkPassword()));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", ledgerId), bkEx);
        }
    }

    /**
     * Closes the given LedgerHandle.
     *
     * @param handle The LedgerHandle to close.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void close(LedgerHandle handle) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(handle::close);
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", handle.getId()), bkEx);
        }
    }

    /**
     * Deletes the Ledger with given LedgerId.
     *
     * @param ledgerId   The Id of the Ledger to delete.
     * @param bookKeeper A reference to the BookKeeper client to use.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static void delete(long ledgerId, BookKeeper bookKeeper) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(() -> bookKeeper.deleteLedger(ledgerId));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to delete Ledger %d.", ledgerId), bkEx);
        }
    }

    /**
     * Fences out a Log made up of the given ledgers.
     *
     * @param ledgerIds     An ordered list of LedgerMetadata objects representing all the Ledgers in the log.
     * @param bookKeeper    A reference to the BookKeeper client to use.
     * @param config        Configuration to use.
     * @param traceObjectId Used for logging.
     * @return A LedgerAddress representing the address of the last written entry in the Log. This value is null if the log
     * is empty.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static LedgerAddress fenceOut(List<LedgerMetadata> ledgerIds, BookKeeper bookKeeper, BookKeeperConfig config, String traceObjectId) throws DurableDataLogException {
        // Fence out the last few ledgers, in descending order. We need to fence out at least MIN_FENCE_LEDGER_COUNT,
        // but we also need to find the LedgerId & EntryID of the last written entry (it's possible that the last few
        // ledgers are empty, so we need to look until we find one).
        int count = 0;
        val iterator = ledgerIds.listIterator(ledgerIds.size());
        LedgerAddress lastAddress = null;
        while (iterator.hasPrevious() && (count < MIN_FENCE_LEDGER_COUNT || lastAddress == null)) {
            LedgerMetadata ledgerMetadata = iterator.previous();
            LedgerHandle handle = openFence(ledgerMetadata.getLedgerId(), bookKeeper, config);
            if (lastAddress == null && handle.getLastAddConfirmed() >= 0) {
                lastAddress = new LedgerAddress(ledgerMetadata, handle.getLastAddConfirmed());
            }

            close(handle);
            log.info("{}: Fenced out Ledger {}.", traceObjectId, ledgerMetadata);
            count++;
        }

        return lastAddress;
    }
}
