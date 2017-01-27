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

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ZK stream metadata store.
 */
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;
    private static final long TIMEOUT = 60 * 60 * 1000;
    private final ScheduledExecutorService executor;

    public ZKStreamMetadataStore(ScheduledExecutorService executor) {
        this.executor = executor;
        initialize(ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient());
    }

    @VisibleForTesting
    public ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        this.executor = executor;
        initialize(client);
    }

    private void initialize(CuratorFramework client) {

        // Garbage collector for completed transactions
        ZKStream.initialize(client);

        this.executor.scheduleAtFixedRate(() -> {
            // find completed transactions to be gc'd
            try {
                final long currentTime = System.currentTimeMillis();

                ZKStream.getAllCompletedTx().get().entrySet().stream()
                        .forEach(x -> {
                            CompletedTxRecord completedTxRecord = CompletedTxRecord.parse(x.getValue().getData());
                            if (currentTime - completedTxRecord.getCompleteTime() > TIMEOUT) {
                                try {
                                    ZKStream.deletePath(x.getKey(), true);
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
    }

    @Override
    ZKStream newStream(final String name) {
        return new ZKStream(name);
    }

    @Override
    public CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        return ZKStream.getAllActiveTx();
    }

    @Override
    public synchronized CompletableFuture<Boolean> createScope(String scope) {

        return CompletableFuture.completedFuture(false);
    }
}
