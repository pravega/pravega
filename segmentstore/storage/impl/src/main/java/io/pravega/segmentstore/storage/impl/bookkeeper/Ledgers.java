/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DataLogNotAvailableException;
import io.pravega.segmentstore.storage.DurableDataLogException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    static final Charset CUSTOM_PROPERTY_CHARSET = Charsets.US_ASCII;
    static final String PROPERTY_APPLICATION = "application";
    static final String PROPERTY_VALUE_APPLICATION = "Pravega";
    static final String PROPERTY_LOG_ID = "BookKeeperLogId";
    static final int NO_LOG_ID = -1;
    static final long NO_LEDGER_ID = LedgerHandle.INVALID_LEDGER_ID;
    static final long NO_ENTRY_ID = LedgerHandle.INVALID_ENTRY_ID;
    /**
     * How many ledgers to fence out (from the end of the list) when acquiring lock.
     */
    static final int MIN_FENCE_LEDGER_COUNT = 2;

    /**
     * Creates a new Ledger in BookKeeper.
     * @param bookKeeper A {@link BookKeeper} client to use.
     * @param config     The {@link BookKeeperConfig} to use.
     * @param logId      The Id of the {@link BookKeeperLog} that owns this ledgers. This will be codified in the new
     *                   ledger's metadata so that it may be recovered in case we need to reconstruct the {@link BookKeeperLog}
     *                   in the future.
     *
     * @return A LedgerHandle for the new ledger.
     * @throws DataLogNotAvailableException If BookKeeper is unavailable or the ledger could not be created because an
     *                                      insufficient number of Bookies are available. The causing exception is wrapped
     *                                      inside it.
     * @throws DurableDataLogException      If another exception occurred. The causing exception is wrapped inside it.
     */
    static LedgerHandle create(BookKeeper bookKeeper, BookKeeperConfig config, int logId) throws DurableDataLogException {
        try {

            return Exceptions.handleInterruptedCall(() ->
                    bookKeeper.createLedger(
                            config.getBkEnsembleSize(),
                            config.getBkWriteQuorumSize(),
                            config.getBkAckQuorumSize(),
                            config.getDigestType(),
                            config.getBKPassword(),
                            createLedgerCustomMetadata(logId)));
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
            return Exceptions.handleInterruptedCall(
                    () -> bookKeeper.openLedger(ledgerId, config.getDigestType(), config.getBKPassword()));
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
            return Exceptions.handleInterruptedCall(
                    () -> bookKeeper.openLedgerNoRecovery(ledgerId, config.getDigestType(), config.getBKPassword()));
        } catch (BKException bkEx) {
            throw new DurableDataLogException(String.format("Unable to open-read ledger %d.", ledgerId), bkEx);
        }
    }

    /**
     * Reliably retrieves the LastAddConfirmed for the Ledger with given LedgerId, by opening the Ledger in fencing mode
     * and getting the value. NOTE: this open-fences the Ledger which will effectively stop any writing action on it.
     *
     * @param ledgerId   The Id of the Ledger to query.
     * @param bookKeeper A references to the BookKeeper client to use.
     * @param config     Configuration to use.
     * @return The LastAddConfirmed for the given LedgerId.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static long readLastAddConfirmed(long ledgerId, BookKeeper bookKeeper, BookKeeperConfig config) throws DurableDataLogException {
        LedgerHandle h = null;
        try {
            // Here we open the Ledger WITH recovery, to force BookKeeper to reconcile any appends that may have been
            // interrupted and not properly acked. Otherwise there is no guarantee we can get an accurate value for
            // LastAddConfirmed.
            h = openFence(ledgerId, bookKeeper, config);
            return h.getLastAddConfirmed();
        } finally {
            if (h != null) {
                close(h);
            }
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
            throw new DurableDataLogException(String.format("Unable to close ledger %d.", handle.getId()), bkEx);
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
     * @param ledgers       An ordered list of LedgerMetadata objects representing all the Ledgers in the log.
     * @param bookKeeper    A reference to the BookKeeper client to use.
     * @param config        Configuration to use.
     * @param traceObjectId Used for logging.
     * @return A Map of LedgerId to LastAddConfirmed for those Ledgers that were fenced out and had a different
     * LastAddConfirmed than what their LedgerMetadata was indicating.
     * @throws DurableDataLogException If an exception occurred. The causing exception is wrapped inside it.
     */
    static Map<Long, Long> fenceOut(List<LedgerMetadata> ledgers, BookKeeper bookKeeper, BookKeeperConfig config, String traceObjectId) throws DurableDataLogException {
        // Fence out the ledgers, in descending order. During the process, we need to determine whether the ledgers we
        // fenced out actually have any data in them, and update the LedgerMetadata accordingly.
        // We need to fence out at least MIN_FENCE_LEDGER_COUNT ledgers that are not empty to properly ensure we fenced
        // the log correctly and identify any empty ledgers (Since this algorithm is executed upon every recovery, any
        // empty ledgers should be towards the end of the Log).
        int nonEmptyCount = 0;
        val result = new HashMap<Long, Long>();
        val iterator = ledgers.listIterator(ledgers.size());
        while (iterator.hasPrevious() && (nonEmptyCount < MIN_FENCE_LEDGER_COUNT)) {
            LedgerMetadata ledgerMetadata = iterator.previous();
            LedgerHandle handle = openFence(ledgerMetadata.getLedgerId(), bookKeeper, config);
            if (handle.getLastAddConfirmed() != NO_ENTRY_ID) {
                // Non-empty.
                nonEmptyCount++;
            }

            if (ledgerMetadata.getStatus() == LedgerMetadata.Status.Unknown) {
                // We did not know the status of this Ledger before, but now we do.
                result.put(ledgerMetadata.getLedgerId(), handle.getLastAddConfirmed());
            }

            close(handle);
            log.info("{}: Fenced out Ledger {}.", traceObjectId, ledgerMetadata);
        }

        return result;
    }

    /**
     * Gets the {@link #PROPERTY_LOG_ID} value from the given {@link LedgerHandle}, as stored in its custom metadata.
     *
     * @param handle The {@link LedgerHandle} to query.
     * @return The Log Id stored in {@link #PROPERTY_LOG_ID}, or {@link #NO_LOG_ID} if not defined (i.e., due to an upgrade
     * from a version that did not store this information).
     */
    static int getBookKeeperLogId(LedgerHandle handle) {
        String appName = getPropertyValue(PROPERTY_APPLICATION, handle);
        if (appName == null || !appName.equalsIgnoreCase(PROPERTY_VALUE_APPLICATION)) {
            log.warn("Property '{}' on Ledger {} does not match expected value '{}' (actual '{}'). This is OK if this ledger was created prior to Pravega 0.7.1.",
                    PROPERTY_APPLICATION, handle.getId(), PROPERTY_VALUE_APPLICATION, appName);
            return NO_LOG_ID;
        }

        String deserialized = getPropertyValue(PROPERTY_LOG_ID, handle);
        if (deserialized == null) {
            log.warn("No property '{}' found on Ledger {}. This is OK if this ledger was created prior to Pravega 0.7.1.",
                    PROPERTY_LOG_ID, handle.getId());
            return NO_LOG_ID;
        }

        try {
            return Integer.parseInt(deserialized);
        } catch (NumberFormatException ex) {
            log.error("Property '{}' Ledger {} has invalid value '{}'. Returning default value.", PROPERTY_LOG_ID, handle.getId(), deserialized);
            return NO_LOG_ID;
        }
    }

    private static String getPropertyValue(String property, LedgerHandle handle) {
        byte[] s = handle.getCustomMetadata().getOrDefault(property, null);
        return s == null ? null : new String(s, CUSTOM_PROPERTY_CHARSET);
    }

    /**
     * Creates a new {@link Map} that can be set as a Ledger's Custom Metadata.
     *
     * @param logId The {@link BookKeeperLog} Id to encode.
     * @return An immutable {@link Map} containing the encoded Ledger's Custom Metadata.
     */
    @VisibleForTesting
    static Map<String, byte[]> createLedgerCustomMetadata(int logId) {
        return ImmutableMap.<String, byte[]>builder()
                .put(PROPERTY_APPLICATION, PROPERTY_VALUE_APPLICATION.getBytes(CUSTOM_PROPERTY_CHARSET))
                .put(PROPERTY_LOG_ID, Integer.toString(logId).getBytes(CUSTOM_PROPERTY_CHARSET))
                .build();
    }
}
