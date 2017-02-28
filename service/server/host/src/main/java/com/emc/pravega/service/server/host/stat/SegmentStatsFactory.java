/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Data
public class SegmentStatsFactory implements AutoCloseable {

    private static final int CORE_POOL_SIZE = 1;
    private static final int MAXIMUM_POOL_SIZE = 10;
    private static final int TASK_QUEUE_CAPACITY = 100000;
    private static final long KEEP_ALIVE_TIME_IN_SECONDS = 10L;

    private final ScheduledExecutorService maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("cache-maintenance-%d").build());

    private final ExecutorService executor = new ThreadPoolExecutor(CORE_POOL_SIZE, // core size
            MAXIMUM_POOL_SIZE, // max size
            KEEP_ALIVE_TIME_IN_SECONDS, // idle timeout
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(TASK_QUEUE_CAPACITY),
            new ThreadFactoryBuilder().setNameFormat("stat-background-%d").build()); // queue with a size

    @VisibleForTesting
    public SegmentStatsRecorder createSegmentStatsRecorder(StreamSegmentStore store,
                                                           String scope,
                                                           String requestStream,
                                                           ClientFactory clientFactory,
                                                           Duration cooldown,
                                                           Duration mute,
                                                           Duration cacheCleanup,
                                                           Duration cacheExpiry) {
        AutoScalerConfig configuration = new AutoScalerConfig(cooldown, mute, cacheCleanup, cacheExpiry,
                scope, requestStream, null);
        AutoScaleProcessor monitor = new AutoScaleProcessor(configuration, clientFactory,
                executor, maintenanceExecutor);
        return new SegmentStatsRecorderImpl(monitor, store,
                executor, maintenanceExecutor);
    }

    public SegmentStatsRecorder createSegmentStatsRecorder(StreamSegmentStore store,
                                                           String scope,
                                                           String requestStream,
                                                           URI controllerUri) {
        AutoScalerConfig configuration = new AutoScalerConfig(scope, requestStream, controllerUri);
        AutoScaleProcessor monitor = new AutoScaleProcessor(configuration, executor, maintenanceExecutor);
        return new SegmentStatsRecorderImpl(monitor, store, executor, maintenanceExecutor);
    }

    @Override
    public void close() {
        executor.shutdown();
        maintenanceExecutor.shutdown();
    }
}
