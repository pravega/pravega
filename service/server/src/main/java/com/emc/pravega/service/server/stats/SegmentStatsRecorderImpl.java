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

package com.emc.pravega.service.server.stats;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import com.emc.pravega.stream.Serializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    private static final int INITIAL_CAPACITY = 1000;
    private static final int MAX_CACHE_SIZE = 100000;

    private final List<SegmentTrafficMonitor> monitors;

    private final Cache<String, SegmentAggregates> cache;
    private final Optional<com.emc.pravega.service.storage.Cache> diskBackedCache;
    private final Serializer<SegmentAggregates> serializer;

    private final long reportingDuration;

    SegmentStatsRecorderImpl(List<SegmentTrafficMonitor> monitors, com.emc.pravega.service.storage.Cache diskCache, Serializer<SegmentAggregates> serializer) {
        this(monitors, diskCache, serializer, TWO_MINUTES);
    }

    SegmentStatsRecorderImpl(List<SegmentTrafficMonitor> monitors, com.emc.pravega.service.storage.Cache diskCache, Serializer<SegmentAggregates> serializer, long reportingDuration) {
        this.serializer = serializer;
        this.monitors = Lists.newArrayList(monitors);

        diskBackedCache = Optional.ofNullable(diskCache);

        this.cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(20, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, SegmentAggregates>) notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED) ||
                            notification.getCause().equals(RemovalCause.SIZE)) {
                        // move to disk backed cache
                        diskBackedCache.ifPresent(x -> x.insert(new Key(notification.getKey()), serializer.serialize(notification.getValue()).array()));
                    } else if (notification.getCause().equals(RemovalCause.EXPLICIT)) {
                        // remove from disk backed cache
                        diskBackedCache.ifPresent(x -> x.remove(new Key(notification.getKey())));
                    }
                })
                .build();
        this.reportingDuration = reportingDuration;
    }

    private SegmentAggregates getSegmentAggregate(String streamSegmentName) {
        try {
            return cache.get(streamSegmentName, () -> {

                final byte[] data = diskBackedCache.map(x -> x.get(new Key(streamSegmentName))).orElse(null);
                if (data == null) {
                    return null;
                } else {
                    SegmentAggregates aggregates = serializer.deserialize(ByteBuffer.wrap(data));
                    // if we have loaded from diskbacked cache into main cache, we can remove it from disk backed as
                    // there is no point in keeping two copies of data.
                    diskBackedCache.ifPresent(x -> x.remove(new Key(streamSegmentName)));
                    return aggregates;
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createSegment(String streamSegmentName, byte type, long targetRate) {
        cache.put(streamSegmentName, new SegmentAggregates(targetRate, type));
        monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentCreated));
    }

    @Override
    public void sealSegment(String streamSegmentName) {
        if (getSegmentAggregate(streamSegmentName) != null) {
            cache.invalidate(streamSegmentName);
            monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentSealed));
        }
    }

    @Override
    public void policyUpdate(String segmentStreamName, byte type, long targetRate) {
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
            if (aggregates != null) {
                if (aggregates.getScaleType() != WireCommands.CreateSegment.NO_SCALE) {
                    aggregates.update(dataLength, numOfEvents);

                    if (System.currentTimeMillis() - aggregates.getLastReportedTime() > reportingDuration) {
                        CompletableFuture.runAsync(() -> monitors.forEach(monitor -> monitor.process(streamSegmentName,
                                aggregates.getTargetRate(), aggregates.getScaleType(), aggregates.getStartTime(),
                                aggregates.getTwoMinuteRate(), aggregates.getFiveMinuteRate(),
                                aggregates.getTenMinuteRate(), aggregates.getTwentyMinuteRate())), EXECUTOR);
                        aggregates.setLastReportedTime(System.currentTimeMillis());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Record statistic for {} for data: {} and events:{} threw exception", streamSegmentName, dataLength, numOfEvents, e.getMessage());
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
            return obj instanceof String && key.equals(obj);
        }
    }
}
