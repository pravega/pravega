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

import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.stream.api.v1.PingStatus;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
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
public class PingManager {

    private static final long TICK_DURATION = 1000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final HashedWheelTimer hashedWheelTimer;
    private final Map<String, TxnTimeoutTask> map;

    public PingManager(StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
        this.hashedWheelTimer = new HashedWheelTimer(TICK_DURATION, TIME_UNIT);
        map = new HashMap<>();
        this.hashedWheelTimer.start();
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
                        // If abort attempt fails because lock could not be obtained, reschedule this task
                        // In all other failure scenarios, like
                        // 1. Tx node version mismatch
                        // 2. Tx or Stream node not found
                        // Ignore the timeout task
                        if (getRealCause(ex) instanceof LockFailedException) {
                            log.warn(String.format("Rescheduling timeout task for tx %s because of lock failure", txnId));
                            hashedWheelTimer.newTimeout(this, 2 * TICK_DURATION, TIME_UNIT);
                        } else {
                            String message =
                                    String.format("Timeout task for tx %s failed because of %s. Ignoring timeout task.",
                                            txnId, ex.getClass().getName());
                            log.warn(message, ex);
                        }
                        return null;
                    })
                    .join();
        }
    }

    public void addTx(final String scope, final String stream, final UUID txnId, final int version,
                      final long lease, final long maxExecutionTimeExpiry, final long scaleGracePeriod) {
        final String key = getKey(scope, stream, txnId);
        final TxnTimeoutTask task = new TxnTimeoutTask(scope, stream, txnId, version, maxExecutionTimeExpiry);
        hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
        map.put(key, task);
    }

    public PingStatus pingTx(final String scope, final String stream, final UUID txnId, long lease) {
        // todo: initially check if stopped, return DISCONNECTED in that case.
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
            // We do not know about this txn.
            // todo: validate if it is ok to return disconnected status.
            return PingStatus.DISCONNECTED;
        }
    }

    public boolean contains(final String scope, final String stream, final UUID txnId) {
        return map.containsKey(getKey(scope, stream, txnId));
    }

    public void stop() {
        hashedWheelTimer.stop();
        map.clear();
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
