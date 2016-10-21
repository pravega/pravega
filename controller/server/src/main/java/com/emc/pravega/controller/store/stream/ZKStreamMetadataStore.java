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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.TxnId;
import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ZK stream metadata store
 */
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;
    private static final long TIMEOUT = 60 * 60 * 1000;
    private static final ScheduledExecutorService EXEC_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private final LoadingCache<String, ZKStream> zkStreams;

    public ZKStreamMetadataStore(StoreConfiguration storeConfiguration) {

        ZKStream.initialize(storeConfiguration.getConnectionString());
        EXEC_SERVICE.scheduleAtFixedRate(() -> {
            // find completed transactions to be gc'd
            try {
                final long currentTime = System.currentTimeMillis();

                ZKStream.getAllCompletedTx().get().entrySet().stream()
                        .forEach(x -> {
                            if (currentTime - x.getValue().getCompleteTime() > TIMEOUT) {
                                try {
                                    ZKStream.deletePath(x.getKey());
                                } catch (Exception e) {
                                    // TODO: log and ignore
                                }
                            }
                        });
            } catch (Exception e) {
                // TODO: log!
            }
            // find completed transactions to be gc'd
        }, INITIAL_DELAY, PERIOD, TimeUnit.HOURS);


        zkStreams = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, ZKStream>() {
                    public void onRemoval(RemovalNotification<String, ZKStream> removal) {
                        removal.getValue().tearDown();
                    }
                })
                .build(
                        new CacheLoader<String, ZKStream>() {
                            public ZKStream load(String key) {
                                ZKStream zkStream = new ZKStream(key);
                                zkStream.init();
                                return zkStream;
                            }
                        });
    }

    @Override
    ZKStream getStream(String name) {
        return zkStreams.getUnchecked(name);
    }

    @Override
    public CompletableFuture<Boolean> createStream(String name, StreamConfiguration configuration, long createTimestamp) {
        return CompletableFuture.supplyAsync(() -> zkStreams.getUnchecked(name)).thenCompose(x -> x.create(configuration, createTimestamp));
    }

    @Override
    public CompletableFuture<Map<TxnId, ActiveTxRecord>> getAllActiveTx() {
        return ZKStream.getAllActiveTx()
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(y -> convertPathToTxnId(y.getKey()), Map.Entry::getValue)));
    }

    @Override
    public CompletableFuture<Map<TxnId, CompletedTxRecord>> getAllCompletedTx() {
        return ZKStream.getAllCompletedTx()
                .thenApply(x -> x.entrySet().stream()
                        .collect(Collectors.toMap(y -> convertPathToTxnId(y.getKey()), Map.Entry::getValue)));

    }

    private TxnId convertPathToTxnId(String key) {
        String[] x = key.split("/");
        return new TxnId(x[0], x[1], UUID.fromString(x[2]));
    }
}
