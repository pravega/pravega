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
package com.emc.pravega.controller.timeout;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.stream.api.v1.PingStatus;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.util.concurrent.AbstractService;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AllArgsConstructor;
import lombok.Data;
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
public class TimerWheelTimeoutService extends AbstractService implements TimeoutService {

    private static final long TICK_DURATION = 1000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final HashedWheelTimer hashedWheelTimer;
    private final Map<String, TxnData> map;

    @AllArgsConstructor
    private class TxnTimeoutTask implements TimerTask {

        private final String scope;
        private final String stream;
        private final UUID txnId;

        @Override
        public void run(Timeout timeout) throws Exception {

            String key = getKey(scope, stream, txnId);
            TxnData txnData = map.get(key);

            if (System.currentTimeMillis() < txnData.getExpiryTimestamp()) {
                log.debug("Ignoring timeout task for txn {}", key);
                return;
            }

            log.debug("Executing timeout task for txn {}", key);
            streamTransactionMetadataTasks.abortTx(scope, stream, txnId, Optional.of(txnData.getVersion()))
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
                                        "Ignoring timeout task.", key, error.getClass().getName());
                                log.debug(message);
                                map.remove(key);
                            } else {
                                message = String.format("Rescheduling timeout task for tx %s because " +
                                        "of transient or unknown error", key);
                                log.warn(message, ex);
                                hashedWheelTimer.newTimeout(this, 2 * TICK_DURATION, TIME_UNIT);
                            }
                        } else {
                            message = String.format("Successfully executed abort on tx %s ", key);
                            log.debug("Successfully executed abort on tx {} ", key);
                            map.remove(key);
                        }
                        return null;
                    })
                    .join();
        }
    }

    @Data
    @AllArgsConstructor
    private static class TxnData {
        private final int version;
        private long expiryTimestamp;
        private final long maxExecutionTimeExpiry;
        private final long scaleGracePeriod;
        private TxnTimeoutTask task;
    }

    public TimerWheelTimeoutService(StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
        this.hashedWheelTimer = new HashedWheelTimer();
        map = new HashMap<>();
        this.startAsync();
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

    @Override
    public void addTx(final String scope, final String stream, final UUID txnId, final int version,
                      final long lease, final long maxExecutionTimeExpiry, final long scaleGracePeriod) {

        if (this.state() == State.RUNNING) {
            final String key = getKey(scope, stream, txnId);
            final long expiry = System.currentTimeMillis() + lease;
            final TxnTimeoutTask task = new TxnTimeoutTask(scope, stream, txnId);
            hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
            map.put(key, new TxnData(version, expiry, maxExecutionTimeExpiry, scaleGracePeriod, task));
        }

    }

    @Override
    public PingStatus pingTx(final String scope, final String stream, final UUID txnId, long lease) {

        if (this.state() != State.RUNNING) {
            return PingStatus.DISCONNECTED;
        }

        final String key = getKey(scope, stream, txnId);

        if (map.containsKey(key)) {

            final TxnData txnData = map.get(key);
            final long current = System.currentTimeMillis();

            if (current + lease > txnData.getMaxExecutionTimeExpiry()) {

                return PingStatus.MAX_EXECUTION_TIME_EXCEEDED;

            } else {

                txnData.setExpiryTimestamp(current + lease);
                hashedWheelTimer.newTimeout(txnData.getTask(), lease, TimeUnit.MILLISECONDS);
                return PingStatus.OK;

            }

        } else {
            throw new IllegalStateException(key);
        }
    }

    @Override
    public boolean containsTx(final String scope, final String stream, final UUID txnId) {
        return map.containsKey(getKey(scope, stream, txnId));
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
