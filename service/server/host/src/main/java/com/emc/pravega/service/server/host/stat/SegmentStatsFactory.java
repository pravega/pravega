/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.monitor.MonitorFactory;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Synchronized;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class SegmentStatsFactory {

    public static final AtomicReference<SegmentStatsRecorder> STAT_RECORDER = new AtomicReference<>();
    private static final ScheduledExecutorService CACHE_MAINTENANCE_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private static final ExecutorService EXECUTOR = new ThreadPoolExecutor(1, // core size
            10, // max size
            60L, // idle timeout
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(100000)); // queue with a size

    @Synchronized
    public static void createSegmentStatsRecorder(StreamSegmentStore store) {
        if (STAT_RECORDER.get() == null) {
            List<SegmentTrafficMonitor> monitors = Lists.newArrayList(
                    MonitorFactory.createMonitor(MonitorFactory.MonitorType.AutoScaleMonitor, EXECUTOR, CACHE_MAINTENANCE_EXECUTOR));
            STAT_RECORDER.compareAndSet(null, new SegmentStatsRecorderImpl(monitors, store, EXECUTOR, CACHE_MAINTENANCE_EXECUTOR));
        }
    }

    public static Optional<SegmentStatsRecorder> getSegmentStatsRecorder() {
        return Optional.ofNullable(STAT_RECORDER.get());
    }

}
