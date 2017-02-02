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
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.TxTimeoutRequest;
import com.emc.pravega.controller.task.Stream.TxTimeOutScheduler;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TxTimeoutStreamScheduler implements TxTimeOutScheduler {
    private final EventStreamWriter<TxTimeoutRequest> writer;

    public TxTimeoutStreamScheduler() {
        // controller is localhost.
        ClientFactory clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE,
                URI.create(String.format("tcp://localhost:%d", Config.SERVER_PORT)));

        writer = clientFactory.createEventWriter(Config.TXN_TIMER_STREAM_NAME,
                new JavaSerializer<>(),
                new EventWriterConfig(null));
    }

    @Override
    public CompletableFuture<Void> scheduleTimeOut(String scope, String stream, UUID txid, long timeoutPeriod) {
        TxTimeoutRequest txRequest = new TxTimeoutRequest(scope, stream, txid.toString(), System.currentTimeMillis() + timeoutPeriod);

        return Retry.withExpBackoff(100, 10, 3, 1000)
                .retryingOn(RetryableException.class)
                .throwingOn(RuntimeException.class)
                .run(() -> CompletableFuture.runAsync(() -> {
                    try {
                        writer.writeEvent(txRequest.getKey(), txRequest).get();
                    } catch (Exception e) {
                        throw new RetryableException("scheduling timeout failed. Try again");
                    }
                }));
    }
}
