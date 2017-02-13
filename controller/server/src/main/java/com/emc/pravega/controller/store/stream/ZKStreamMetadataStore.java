/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.metrics.MetricsConfig;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ZK stream metadata store.
 */
@Slf4j
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {
    private static final long INITIAL_DELAY = 1;
    private static final long PERIOD = 1;
    private static final long TIMEOUT = 60 * 60 * 1000;
    private final ScheduledExecutorService executor;

    public ZKStreamMetadataStore(ScheduledExecutorService executor) {
        this.executor = executor;
        initialize(ZKUtils.getCuratorClient(), ZKUtils.getMetricsConfig());
    }

    @VisibleForTesting
    public ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        this.executor = executor;
        initialize(client, ZKUtils.getMetricsConfig());
    }

    private void initialize(CuratorFramework client, MetricsConfig metricsConfig) {

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
                log.error("Caught exception while attempting to find completed transactions in zookeeper store.", e);
            }
            // find completed transactions to be gc'd
        }, INITIAL_DELAY, PERIOD, TimeUnit.HOURS);
        if (metricsConfig != null) {
            METRICS_PROVIDER.start(metricsConfig);
        }
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
