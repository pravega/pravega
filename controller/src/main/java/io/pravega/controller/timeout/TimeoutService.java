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
package io.pravega.controller.timeout;

import io.pravega.controller.store.Version;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.google.common.util.concurrent.Service;

import java.util.UUID;

/**
 * Timeout management service interface.
 * This service maintains following in-memory data structures for managing transaction timeouts in controller.
 * 1. An in-memory map of transactions whose timeout it manages, whose key is /scope/stream/txId and value is
 *    the tuple (version, timeoutTimestamp, maxExpiryTimestamp), where version is the version of the transaction
 *    metadata node in underlying store.
 * 2. A HashedTimerWheel that tracks transaction timeouts and attempts to automatically abort timed out transactions.
 *
 * Pravega client creates a transaction by calling the createTransaction controller API. In response to this API,
 * the controller instance (1) creates transaction node in the peristent store, and (2) Starts tracking the transaction
 * timeout in in-memory data structures managed by TimeoutService by invoking {@link TimeoutService#addTxn} method.
 *
 * Subsequently, the client may renew transaction lease by calling pingTransaction API on the same controller
 * instance. On receiving a ping request for a known transaction, the controller instance updates the in-memory
 * data structures of TimeoutService by invoking {@link TimeoutService#pingTxn} method. The controller instance does
 * not update the persisted transaction data on receiving ping request about a known transaction in order to reduce
 * latency of ping requests. Eventually, the client may commit or abort a transaction by calling commitTransaction
 * or abortTransaction controller API, respectively. On receiving these requests, the controller instance
 * updates the transaction state in the persistent store and removes information about that transaction from
 * TimeoutService by calling {@link TimeoutService#removeTxn} method. If a controller instance does not receive a ping
 * request for a transaction before its lease expiry, the controller assumes that the client that initiated the
 * transaction has failed and hence it automatically aborts the transaction, and removes its traces from its in-memory
 * data structures.
 *
 * This scheme works as long as the client can reach the same controller instance on which it created the transaction
 * for sending ping requests. However, if that controller instance crashes, or is partitioned from the client, the
 * client attempts to contact another controller instance and renew its lease before expiry. The controller instance
 * can check whether its TimeoutService is managing timeouts for a given transaction by invoking
 * {@link TimeoutService#containsTxn} method. If TimeoutService in a controller instance does not manage timeouts of a
 * transaction then that controller instance first fetches the transaction metadata from persistent store and updates
 * its version in the underlying persistent store. It then updates in-memory data structures of TimeoutService by
 * calling {@link TimeoutService#addTxn} method. Updating transaction node version in the persistent store prevents the old
 * controller instance from automatically aborting that transaction, thus acting as a fencing mechanism.
 *
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
     */
    void addTxn(final String scope, final String stream, final UUID txnId, final Version version,
                final long lease, final long maxExecutionTimeExpiry);

    /**
     * Remove information about the specified transaction.
     *
     * @param scope                  Scope name.
     * @param stream                 Stream name.
     * @param txnId                  Transaction id.
     */
    void removeTxn(final String scope, final String stream, final UUID txnId);

    /**
     * This method increases the txn timeout by lease amount of milliseconds.
     * <p>
     * If this object is in stopped state, pingTxn returns DISCONNECTED status.
     * If increasing txn timeout causes the time for which txn is open to exceed
     * max execution time, pingTxn returns MAX_EXECUTION_TIME_EXCEEDED. If metadata
     * about specified txn is not present in the map, it throws IllegalStateException.
     * Otherwise pingTxn returns OK status.
     *
     * @param scope  Scope name.
     * @param stream Stream name.
     * @param txnId  Transaction id.
     * @param version Transaction version.
     *@param lease  Additional amount of time for the transaction to be in open state.  @return Ping status
     */
    PingTxnStatus pingTxn(final String scope, final String stream, final UUID txnId, Version version, long lease);

    /**
     * This method returns a boolean indicating whether it manages timeout for the specified transaction.
     *
     * @param scope  Scope name.
     * @param stream Stream name.
     * @param txnId  Transaction id.
     * @return A boolean indicating whether this class manages timeout for specified transaction.
     */
    boolean containsTxn(final String scope, final String stream, final UUID txnId);

    /**
     * Returns the maximum allowed lease value.
     *
     * @return maximum allowed lease value.
     */
    long getMaxLeaseValue();
}
