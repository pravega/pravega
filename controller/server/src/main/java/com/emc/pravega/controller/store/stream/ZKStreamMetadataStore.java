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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ZK stream metadata store
 */
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;
    private static final long TIMEOUT = 60 * 60 * 1000;
    private static final ScheduledExecutorService EXEC_SERVICE = Executors.newSingleThreadScheduledExecutor();

    public ZKStreamMetadataStore(final StoreConfiguration storeConfiguration) {

        // TODO: get common curator client
        CuratorFramework client = CuratorFrameworkFactory.newClient(storeConfiguration.getConnectionString(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        // Garbage collector for completed transactions
        ZKStream.initialize(client);

        EXEC_SERVICE.scheduleAtFixedRate(() -> {
            // find completed transactions to be gc'd
            try {
                final long currentTime = System.currentTimeMillis();

                ZKStream.getAllCompletedTx().get().entrySet().stream().forEach(x -> {
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
}
