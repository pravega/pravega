/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.service.contracts.Attributes;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Maintain two levels of cache - one in-memory and another that is disk backed
 * We will use RocksDb for our disk backed cache.
 * The idea is as follows: segments writes are happening, we will keep writing their
 * data into the cache.
 * If an entry is evicted from the cache we move it to disk backed cache.
 * The idea is we only use one copy of data, either in in-memory cache or disk backed cache.
 */
@Slf4j
public class SegmentStatsRecorderImpl implements SegmentStatsRecorder {
    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final int INITIAL_CAPACITY = 1000;
    private static final int MAX_CACHE_SIZE = 100000; // 100k segment records in memory.

    private final List<SegmentTrafficMonitor> monitors;

    private final Set<String> pendingCacheLoads;
    private final Cache<String, SegmentAggregates> cache;
    private final long reportingDuration;
    private final StreamSegmentStore store;
    private final Executor executor;

    SegmentStatsRecorderImpl(List<SegmentTrafficMonitor> monitors, StreamSegmentStore store,
                             ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        this(monitors, store, TWO_MINUTES, executor, maintenanceExecutor);
    }

    @VisibleForTesting
    SegmentStatsRecorderImpl(List<SegmentTrafficMonitor> monitors, StreamSegmentStore store,
                             long reportingDuration, ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        Preconditions.checkNotNull(monitors);
        Preconditions.checkNotNull(executor);
        this.monitors = monitors;
        this.executor = executor;
        this.pendingCacheLoads = new HashSet<>();

        this.cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .build();

        // Dedicated thread for cache clean up scheduled periodically. This ensures that read and write
        // on cache are not used for cache maintenance activities.
        maintenanceExecutor.scheduleAtFixedRate(cache::cleanUp, Duration.ofMinutes(5).toMillis(), 2, TimeUnit.MINUTES);
        this.reportingDuration = reportingDuration;
        this.store = store;
    }

    private SegmentAggregates getSegmentAggregate(String streamSegmentName) {
        SegmentAggregates aggregates = cache.getIfPresent(streamSegmentName);

        if (aggregates == null &&
                StreamSegmentNameUtils.getParentStreamSegmentName(streamSegmentName) != null) {
            loadAsynchronously(streamSegmentName);
        }

        return aggregates;
    }

    private void loadAsynchronously(String streamSegmentName) {

        if (!pendingCacheLoads.contains(streamSegmentName)) {
            pendingCacheLoads.add(streamSegmentName);
            Optional.ofNullable(store).ifPresent(s -> s.getStreamSegmentInfo(streamSegmentName, false, Duration.ofMinutes(1))
                    .thenAccept(prop -> {
                        if (prop != null) {
                            byte type = prop.getAttributes().get(Attributes.SCALE_POLICY_TYPE).byteValue();
                            int rate = prop.getAttributes().get(Attributes.SCALE_POLICY_TYPE).intValue();
                            cache.put(streamSegmentName, new SegmentAggregates(type, rate));
                        }
                        pendingCacheLoads.remove(streamSegmentName);
                    }));
        }
    }

    @Override
    public void createSegment(String streamSegmentName, byte type, int targetRate) {
        cache.put(streamSegmentName, new SegmentAggregates(type, targetRate));
        if (type != WireCommands.CreateSegment.NO_SCALE) {
            monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentCreated));
        }
    }

    @Override
    public void sealSegment(String streamSegmentName) {
        if (getSegmentAggregate(streamSegmentName) != null) {
            cache.invalidate(streamSegmentName);
            monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentSealed));
        }
    }

    @Override
    public void policyUpdate(String segmentStreamName, byte type, int targetRate) {
        SegmentAggregates aggregates = getSegmentAggregate(segmentStreamName);
        if (aggregates != null) {
            aggregates.setTargetRate(targetRate);
            aggregates.setScaleType(type);
        }
    }

    /**
     * Updates segment specific aggregates.
     * Then if two minutes have elapsed between last report
     * of aggregates for this segment, send a new update to the monitor.
     * This update to the monitor is processed by monitor asynchronously.
     *
     * @param streamSegmentName stream segment name
     * @param dataLength        length of data that was written
     * @param numOfEvents       number of events that were written
     */
    @Override
    public void record(String streamSegmentName, long dataLength, int numOfEvents) {
        try {
            SegmentAggregates aggregates = getSegmentAggregate(streamSegmentName);
            // Note: we could get stats for a transaction segment. We will simply ignore this as we
            // do not maintain intermittent txn segment stats. Txn stats will be accounted for
            // only upon txn commit. This is done via merge method. So here we can get a txn which
            // we do not know about and hence we can get null and ignore.

            if (aggregates != null) {
                if (aggregates.getScaleType() != WireCommands.CreateSegment.NO_SCALE) {
                    aggregates.update(dataLength, numOfEvents);

                    if (System.currentTimeMillis() - aggregates.getLastReportedTime() > reportingDuration) {
                        try {
                            CompletableFuture.runAsync(() -> monitors.forEach(monitor -> monitor.process(streamSegmentName,
                                    aggregates.getTargetRate(), aggregates.getScaleType(), aggregates.getStartTime(),
                                    aggregates.getTwoMinuteRate(), aggregates.getFiveMinuteRate(),
                                    aggregates.getTenMinuteRate(), aggregates.getTwentyMinuteRate())), executor);
                            aggregates.setLastReportedTime(System.currentTimeMillis());
                        } catch (RejectedExecutionException e) {
                            // We will not keep posting indefinitely and let the queue grow. We will only post optimistically
                            // and ignore any rejected execution exceptions.
                            log.error("Over 100k requests pending to monitor. We will report this when pending work clears up. StreamSegmentName: {}", streamSegmentName);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Record statistic for {} for data: {} and events:{} threw exception", streamSegmentName, dataLength, numOfEvents, e);
        }
    }

    /**
     * Method called with txn stats whenever a txn is committed.
     *
     * @param streamSegmentName parent segment name
     * @param dataLength        length of data written in txn
     * @param numOfEvents       number of events written in txn
     * @param txnCreationTime   time when txn was created
     */
    @Override
    public void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
        SegmentAggregates aggregates = getSegmentAggregate(streamSegmentName);
        // This should never be null as a txn commit cannot happen on a sealed segment.
        // However, if a diskbacked cache is not present, this may be null as it may have been evicted from cache.
        if (aggregates != null) {
            aggregates.updateTx(dataLength, numOfEvents, txnCreationTime);
        }
    }

    @VisibleForTesting
    SegmentAggregates getSegmentAggregates(String streamSegmentName) {
        return getSegmentAggregate(streamSegmentName);
    }

    private class Key extends com.emc.pravega.service.storage.Cache.Key {
        private final String key;

        private Key(String key) {
            this.key = key;
        }

        @Override
        public byte[] getSerialization() {
            return key.getBytes();
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj != null && obj instanceof Key && obj.equals(this);
        }
    }
}
