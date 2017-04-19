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
 * Created by andrei on 4/19/17.
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
     *                                      insufficient number of Bookies are available.
     * @throws DurableDataLogException      If another exception occurred.
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

    static LedgerHandle open(long ledgerId, BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        try {
            return Exceptions.handleInterrupted(() -> bookKeeper.openLedger(ledgerId, LEDGER_DIGEST_TYPE, config.getBkPassword()));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", ledgerId), bkEx);
        }
    }

    static void close(LedgerHandle handle) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(handle::close);
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-fence ledger %d.", handle.getId()), bkEx);
        }
    }

    static void delete(long ledgerId, BookKeeper bookKeeper) throws DurableDataLogException {
        try {
            Exceptions.handleInterrupted(() -> bookKeeper.deleteLedger(ledgerId));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to delete Ledger %d.", ledgerId), bkEx);
        }
    }

    static LedgerAddress fenceOut(List<LedgerMetadata> ledgerIds, BookKeeper bookKeeper, BookKeeperConfig config, String traceObjectId) throws DurableDataLogException {
        // Fence out the last few ledgers, in descending order. We need to fence out at least MIN_FENCE_LEDGER_COUNT,
        // but we also need to find the LedgerId & EntryID of the last written entry (it's possible that the last few
        // ledgers are empty, so we need to look until we find one).
        int count = 0;
        val iterator = ledgerIds.listIterator(ledgerIds.size());
        LedgerAddress lastAddress = null;
        while (iterator.hasPrevious() && (count < MIN_FENCE_LEDGER_COUNT || lastAddress == null)) {
            LedgerMetadata ledgerMetadata = iterator.previous();
            LedgerHandle handle = open(ledgerMetadata.getLedgerId(), bookKeeper, config);
            if (lastAddress == null && handle.getLastAddConfirmed() >= 0) {
                lastAddress = new LedgerAddress(ledgerMetadata.getSequence(), handle.getId(), handle.getLastAddConfirmed());
            }

            close(handle);
            log.info("{}: Fenced out Ledger {}.", traceObjectId, ledgerMetadata);
            count++;
        }

        return lastAddress;
    }
}
