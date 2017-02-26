/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.timeout;

import com.emc.pravega.controller.stream.api.v1.PingStatus;
import com.google.common.util.concurrent.Service;

import java.util.UUID;

/**
 * Timeout manager interface.
 */
public interface TimeoutService extends Service {

    /**
     * Start tracking timeout for the specified transaction.
     *
     * @param scope                  Scope name.
     * @param stream                 Stream name.
     * @param txnId                  Transaction id.
     * @param version                Version of transaction data node in the underlying store.
     * @param lease                  Amount of time for which to keep the transaction in open state.
     * @param maxExecutionTimeExpiry Timestamp beyond which transaction lease cannot be increased.
     * @param scaleGracePeriod       Maximum amount of time by which transaction lease can be increased
     *                               once a scale operation starts on the transaction stream.
     */
    void addTx(final String scope, final String stream, final UUID txnId, final int version,
                      final long lease, final long maxExecutionTimeExpiry, final long scaleGracePeriod);

    /**
     * This method increases the txn timeout by lease amount of milliseconds.
     * <p>
     * If this object is in stopped state, pingTx returns DISCONNECTED status.
     * If increasing txn timeout causes the time for which txn is open to exceed
     * max execution time, pingTx returns MAX_EXECUTION_TIME_EXCEEDED. If metadata
     * about specified txn is not present in the map, it throws IllegalStateException.
     * Otherwise pingTx returns OK status.
     *
     * @param scope  Scope name.
     * @param stream Stream name.
     * @param txnId  Transaction id.
     * @param lease  Additional amount of time for the transaction to be in open state.
     * @return Ping status
     */
    PingStatus pingTx(final String scope, final String stream, final UUID txnId, long lease);

    /**
     * This method returns a boolean indicating whether it manages timeout for the specified transaction.
     *
     * @param scope  Scope name.
     * @param stream Stream name.
     * @param txnId  Transaction id.
     * @return A boolean indicating whether this class manages timeout for specified transaction.
     */
    boolean containsTx(final String scope, final String stream, final UUID txnId);
}
