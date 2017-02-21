/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;

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
        initialize(ZKUtils.getCuratorClient());
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
                log.error("Caught exception while attempting to find completed transactions in zookeeper store.", e);
            }
            // find completed transactions to be gc'd
        }, INITIAL_DELAY, PERIOD, TimeUnit.HOURS);
        METRICS_PROVIDER.start();
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
