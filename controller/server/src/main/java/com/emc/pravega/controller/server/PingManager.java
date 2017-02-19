/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.stream.api.v1.PingStatus;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.util.concurrent.AbstractService;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Transaction ping manager. It maintains a local hashed timer wheel to manage txn timeouts.
 * It provides the following two methods.
 * 1. Set initial timeout.
 * 2. Increase timeout.
 */
@Slf4j
public class PingManager extends AbstractService {

    private static final long TICK_DURATION = 1000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final HashedWheelTimer hashedWheelTimer;
    private final Map<String, TxnTimeoutTask> map;

    public PingManager(StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
        this.hashedWheelTimer = new HashedWheelTimer(TICK_DURATION, TIME_UNIT);
        map = new HashMap<>();
    }

    @AllArgsConstructor
    private class TxnTimeoutTask implements TimerTask {

        private final String scope;
        private final String stream;
        private final UUID txnId;
        private final int version;
        @Getter
        private final long maxExecutionTimeExpiry;

        @Override
        public void run(Timeout timeout) throws Exception {
            streamTransactionMetadataTasks.abortTx(scope, stream, txnId, Optional.of(version))
                    .handle((ok, ex) -> {
                        // If abort attempt fails because of (1) version mismatch, or (2) node not found,
                        // ignore the timeout task.
                        // In other cases, esp. because lock attempt fails, we reschedule this task for execution
                        // at a later point of time.
                        String message;
                        if (ex != null) {
                            Throwable error = getRealCause(ex);
                            if (error instanceof WriteConflictException || error instanceof DataNotFoundException) {
                                message = String.format("Timeout task for tx %s failed because of %s. " +
                                        "Ignoring timeout task.", txnId, ex.getClass().getName());
                                log.debug(message);
                                map.remove(getKey(scope, stream, txnId));
                            } else {
                                message = String.format("Rescheduling timeout task for tx %s because " +
                                        "of transient or unknown error", txnId);
                                log.warn(message, ex);
                                hashedWheelTimer.newTimeout(this, 2 * TICK_DURATION, TIME_UNIT);
                            }
                        } else {
                            message = String.format("Successfully executed abort on tx %s ", txnId);
                            log.debug(message);
                            map.remove(getKey(scope, stream, txnId));
                        }
                        return null;
                    })
                    .join();
        }
    }

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
    public void addTx(final String scope, final String stream, final UUID txnId, final int version,
                      final long lease, final long maxExecutionTimeExpiry, final long scaleGracePeriod) {

        if (this.state() == State.RUNNING) {
            final String key = getKey(scope, stream, txnId);
            final TxnTimeoutTask task = new TxnTimeoutTask(scope, stream, txnId, version, maxExecutionTimeExpiry);
            hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
            map.put(key, task);
        }

    }

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
    public PingStatus pingTx(final String scope, final String stream, final UUID txnId, long lease) {

        if (this.state() != State.RUNNING) {
            return PingStatus.DISCONNECTED;
        }

        final String key = getKey(scope, stream, txnId);

        if (map.containsKey(key)) {

            final TxnTimeoutTask task = map.get(key);

            if (System.currentTimeMillis() + lease > task.getMaxExecutionTimeExpiry()) {

                return PingStatus.MAX_EXECUTION_TIME_EXCEEDED;

            } else {

                hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
                return PingStatus.OK;

            }

        } else {
            throw new IllegalStateException(key);
        }
    }

    public boolean contains(final String scope, final String stream, final UUID txnId) {
        return map.containsKey(getKey(scope, stream, txnId));
    }

    /**
     * Start the ping manager. This method starts the hashed wheel timer.
     */
    @Override
    protected void doStart() {
        hashedWheelTimer.start();
        notifyStarted();
    }

    /**
     * Stop the ping manager. This method stops the hashed wheel timer and clears the map.
     * It may be called on (a) service stop, or (b) when the process gets disconnected from cluster.
     * If this object receives a ping in stopped state, it will send DISCONNECTED status.
     */
    @Override
    protected void doStop() {
        hashedWheelTimer.stop();
        map.clear();
        notifyStopped();
    }

    private Throwable getRealCause(Throwable e) {
        if ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            return e.getCause();
        } else {
            return e;
        }
    }

    private String getKey(final String scope, final String stream, final UUID txid) {
        return scope + "/" + stream + "/" + txid;
    }
}
