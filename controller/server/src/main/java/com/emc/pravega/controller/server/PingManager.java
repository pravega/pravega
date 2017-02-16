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

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Transaction ping manager. It maintains a local hashed timer wheel to manage txn timeouts.
 * It provides the following two methods.
 * 1. Set initial timeout.
 * 2. Increase timeout
 */
public class PingManager {

    private static final long TICK_DURATION = 1000;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final ControllerService controllerService;
    private final HashedWheelTimer hashedWheelTimer;
    private final Map<String, TimerTask> map;

    public PingManager(ControllerService controllerService) {
        this.controllerService = controllerService;
        this.hashedWheelTimer = new HashedWheelTimer(TICK_DURATION, TIME_UNIT);
        map = new HashMap<>();
    }

    @AllArgsConstructor
    private class TxnTimeoutTask implements TimerTask {

        private final String scope;
        private final String stream;
        private final TxnId txnId;

        @Override
        public void run(Timeout timeout) throws Exception {
            controllerService.abortTransaction(scope, stream, txnId);
        }
    }

    public void ping(final String scope, final String stream, final TxnId txnId, long lease) {
        final String key = getKey(scope, stream, txnId);
        if (map.containsKey(key)) {
            final TimerTask task = map.get(key);
            hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
        } else {
            final TimerTask task = new TxnTimeoutTask(scope, stream, txnId);
            hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
            map.put(key, task);
        }
    }

    private String getKey(final String scope, final String stream, final TxnId txid) {
        return scope + "/" + stream + "/" + txid;
    }

    public void start() {
    }

    public void stop() {
        hashedWheelTimer.stop();
        map.clear();
    }
}
