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
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class is responsible for scheduling timeout tasks for txns once they are created.
 * Right after a txn is created and passed to this scheduler, tt posts the txnTimeoutRequest
 * into a special stream.
 * TxnTimeout request has the timetodrop timestamp and txnid.
 */
@Slf4j
public class TxTimeoutStreamScheduler implements TxTimeOutScheduler {

    private static AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();
    private EventStreamWriter<TxTimeoutRequest> writer;

    public TxTimeoutStreamScheduler() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture<Void> promise = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> createWriter(executor, promise), executor);
    }

    @Override
    public CompletableFuture<Void> scheduleTimeOut(String scope, String stream, UUID txid, long timeoutPeriod) {
        TxTimeoutRequest txRequest = new TxTimeoutRequest(scope, stream, txid.toString(), System.currentTimeMillis() + timeoutPeriod);

        return Retry.withExpBackoff(100, 10, 3, 1000)
                .retryingOn(RetryableException.class)
                .throwingOn(RuntimeException.class)
                .run(() -> CompletableFuture.runAsync(() -> {
                    try {
                        if (writer != null) {
                            writer.writeEvent(txRequest.getKey(), txRequest).get();
                        } else {
                            log.warn("TxnScheduler not yet initialized.");
                        }
                    } catch (Exception e) {
                        throw new RetryableException("scheduling timeout failed. Try again");
                    }
                }));
    }

    private void createWriter(ScheduledExecutorService executor, CompletableFuture<Void> promise) {
        retry(() -> {
            if (clientFactory.get() == null) {
                clientFactory.compareAndSet(null, new ClientFactoryImpl(Config.INTERNAL_SCOPE,
                        URI.create(String.format("tcp://localhost:%d", Config.SERVER_PORT))));
            }

            writer = clientFactory.get().createEventWriter(Config.TXN_TIMER_STREAM_NAME,
                    new JavaSerializer<>(),
                    new EventWriterConfig(null));
            return null;
        }, executor, promise);
    }

    private static void retry(Supplier<Void> supplier, ScheduledExecutorService executor, CompletableFuture<Void> promise) {
        try {
            supplier.get();
            promise.complete(null);
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            executor.schedule(() -> retry(supplier, executor, promise), 10, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    public static void setClientFactory(ClientFactory cf) {
        clientFactory.set(cf);
    }
}
