/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.stat;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.common.util.SimpleCache;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.NameUtils;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.segment.ScaleType;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static io.pravega.shared.MetricsNames.SEGMENT_APPEND_SIZE;
import static io.pravega.shared.MetricsNames.SEGMENT_CREATE_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENT_READ_SIZE;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_BYTES;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_EVENTS;
import static io.pravega.shared.MetricsNames.SEGMENT_WRITE_LATENCY;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.segmentTags;

@Slf4j
class SegmentStatsRecorderImpl implements SegmentStatsRecorder {
    private static final Duration DEFAULT_REPORTING_DURATION = Duration.ofMinutes(2);
    private static final Duration DEFAULT_EXPIRY_DURATION = Duration.ofMinutes(20);
    private static final Duration CACHE_CLEANUP_INTERVAL = Duration.ofMinutes(2);
    private static final int MAX_CACHE_SIZE = 100000; // 100k segment records in memory.
    // At 100k * with each aggregate approximately ~80 bytes = 8 Mb of memory foot print.
    // Assuming 32 bytes for streamSegmentName used as the key in the cache = 3Mb
    // So this can handle 100k concurrently active stream segments with about 11-12 Mb footprint.
    // If cache overflows beyond this, entries will be evicted in order of last accessed.
    // So we will lose relevant traffic history if we have 100k active 'stream segments' across containers
    // where traffic is flowing concurrently.

    private static final int MAX_APPEND_QUEUE_PROCESS_BATCH_SIZE = 10000;
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    private final Counter globalSegmentWriteBytes = STATS_LOGGER.createCounter(globalMetricName(SEGMENT_WRITE_BYTES));
    private final Counter globalSegmentWriteEvents = STATS_LOGGER.createCounter(globalMetricName(SEGMENT_WRITE_EVENTS));
    private final Counter globalSegmentReadBytes = STATS_LOGGER.createCounter(globalMetricName(SEGMENT_READ_BYTES));
    private final OpStatsLogger createStreamSegment = STATS_LOGGER.createStats(SEGMENT_CREATE_LATENCY);
    private final OpStatsLogger readStreamSegment = STATS_LOGGER.createStats(SEGMENT_READ_LATENCY);
    private final OpStatsLogger writeStreamSegment = STATS_LOGGER.createStats(SEGMENT_WRITE_LATENCY);
    private final OpStatsLogger appendSizeDistribution = STATS_LOGGER.createStats(SEGMENT_APPEND_SIZE);
    private final OpStatsLogger readSizeDistribution = STATS_LOGGER.createStats(SEGMENT_READ_SIZE);
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    private final Set<String> pendingCacheLoads;
    private final SimpleCache<String, SegmentWriteContext> cache;
    private final Duration reportingDuration;
    private final AutoScaleProcessor reporter;
    private final StreamSegmentStore store;
    private final ScheduledFuture<?> cacheCleanup;
    private final ScheduledExecutorService executor;
    private final BlockingDrainingQueue<AppendInfo> appendQueue;

    SegmentStatsRecorderImpl(AutoScaleProcessor reporter, StreamSegmentStore store, ScheduledExecutorService executor) {
        this(reporter, store, DEFAULT_REPORTING_DURATION, DEFAULT_EXPIRY_DURATION, executor);
    }

    @VisibleForTesting
    SegmentStatsRecorderImpl(@NonNull AutoScaleProcessor reporter, @NonNull StreamSegmentStore store,
                             @NonNull Duration reportingDuration, @NonNull Duration expiryDuration, @NonNull ScheduledExecutorService executor) {
        this.executor = executor;
        this.pendingCacheLoads = Collections.synchronizedSet(new HashSet<>());

        this.cache = new SimpleCache<>(MAX_CACHE_SIZE, expiryDuration, (segment, context) -> context.close());

        this.cacheCleanup = executor.scheduleAtFixedRate(cache::cleanUp, CACHE_CLEANUP_INTERVAL.toMillis(), 2, TimeUnit.MINUTES);
        this.reportingDuration = reportingDuration;
        this.store = store;
        this.reporter = reporter;
        this.appendQueue = new BlockingDrainingQueue<>();
        startAppendQueueProcessor();
    }

    private void startAppendQueueProcessor() {
        // We begin an async loop. We do not need to hold on to it or even tell it when to stop. When we invoke
        // SegmentStatsRecorderImpl.close(), the appendQueue will also close, so an invocation to take() will throw
        // an ObjectClosedException, thus ending this loop.
        Futures.loop(() -> true,
                () -> this.appendQueue.take(MAX_APPEND_QUEUE_PROCESS_BATCH_SIZE)
                        .thenAcceptAsync(this::processAppendInfo, this.executor),
                this.executor)
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    // Log the error, unless it's because we're shutting down.
                    if (!(ex instanceof ObjectClosedException)
                            && !(ex instanceof CancellationException)) {
                        log.error("SegmentStatsRecorder append queue processor failed. ", ex);
                    }
                    return null;
                });
    }

    @Override
    public void close() {
        this.appendQueue.close();
        this.cacheCleanup.cancel(true);
        this.createStreamSegment.close();
        this.readStreamSegment.close();
        this.writeStreamSegment.close();
        this.appendSizeDistribution.close();
        this.readSizeDistribution.close();
        this.globalSegmentWriteEvents.close();
        this.globalSegmentWriteBytes.close();
        this.globalSegmentReadBytes.close();
    }

    private SegmentWriteContext getWriteContext(String streamSegmentName) {
        SegmentWriteContext aggregates = cache.get(streamSegmentName);

        if (aggregates == null &&
                !NameUtils.isTransactionSegment(streamSegmentName)) {
            loadAsynchronously(streamSegmentName);
        }

        return aggregates;
    }

    @VisibleForTesting
    protected CompletableFuture<Void> loadAsynchronously(String streamSegmentName) {
        if (store == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (pendingCacheLoads.add(streamSegmentName)) {
            return store.getStreamSegmentInfo(streamSegmentName, TIMEOUT)
                    .thenAcceptAsync(prop -> {
                        long policyType = prop.getAttributes().getOrDefault(Attributes.SCALE_POLICY_TYPE, Long.MIN_VALUE);
                        long policyRate = prop.getAttributes().getOrDefault(Attributes.SCALE_POLICY_RATE, Long.MIN_VALUE);
                        if (policyType >= 0 && policyRate >= 0) {
                            val sa = SegmentAggregates.forPolicy(ScaleType.fromValue((byte) policyType), (int) policyRate);
                            cache.put(streamSegmentName, new SegmentWriteContext(streamSegmentName, sa));
                        }
                        pendingCacheLoads.remove(streamSegmentName);
                    }, executor);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void createSegment(String streamSegmentName, byte type, int targetRate, Duration elapsed) {
        this.createStreamSegment.reportSuccessEvent(elapsed);
        SegmentAggregates sa = SegmentAggregates.forPolicy(ScaleType.fromValue(type), targetRate);
        if (!NameUtils.isTransactionSegment(streamSegmentName)) {
            cache.put(streamSegmentName, new SegmentWriteContext(streamSegmentName, sa));
        }
        if (sa.isScalingEnabled()) {
            reporter.notifyCreated(streamSegmentName);
        }
    }

    @Override
    public void deleteSegment(String streamSegmentName) {
        // Do not close the counter of parent segment when deleting transaction segment.
        if (!NameUtils.isTransactionSegment(streamSegmentName)) {
            segmentClosedForWrites(streamSegmentName);
            this.dynamicLogger.freezeCounter(SEGMENT_READ_BYTES, segmentTags(streamSegmentName));
        }
    }

    @Override
    public void sealSegment(String streamSegmentName) {
        // Do not close the counter of parent segment when sealing transaction segment.
        if (!NameUtils.isTransactionSegment(streamSegmentName)) {
            segmentClosedForWrites(streamSegmentName);
        }

        cache.remove(streamSegmentName);
        reporter.notifySealed(streamSegmentName);
    }

    private void segmentClosedForWrites(String streamSegmentName) {
        val context = this.cache.remove(streamSegmentName);
        if (context != null) {
            context.close();
        }
    }

    @Override
    public void policyUpdate(String streamSegmentName, byte type, int targetRate) {
        SegmentWriteContext context = getWriteContext(streamSegmentName);
        if (context != null) {
            val aggregates = context.getSegmentAggregates();
            // if there is a scale type change, discard the old object and create a new object
            if (aggregates.getScaleType().getValue() != type) {
                context.setSegmentAggregates(SegmentAggregates.forPolicy(ScaleType.fromValue(type), targetRate));
            } else {
                aggregates.setTargetRate(targetRate);
            }
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
     * @param elapsed           elapsed time for the append
     */
    @Override
    public void recordAppend(String streamSegmentName, long dataLength, int numOfEvents, Duration elapsed) {
        this.appendQueue.add(new AppendInfo(streamSegmentName, dataLength, numOfEvents, elapsed));
    }

    private void processAppendInfo(Queue<AppendInfo> appendInfoQueue) {
        val bySegment = new HashMap<String, SegmentWrite>();
        long totalBytes = 0;
        int totalEvents = 0;
        try {
            while (!appendInfoQueue.isEmpty()) {
                val a = appendInfoQueue.poll();
                totalBytes += a.getDataLength();
                totalEvents += a.getNumOfEvents();
                this.writeStreamSegment.reportSuccessEvent(a.getElapsed());
                this.appendSizeDistribution.reportSuccessValue(a.getDataLength());

                if (!NameUtils.isTransactionSegment(a.getStreamSegmentName())) {
                    //Don't report segment specific metrics if segment is a transaction
                    //The parent segment metrics will be updated once the transaction is merged
                    SegmentWrite si = bySegment.getOrDefault(a.getStreamSegmentName(), null);
                    if (si == null) {
                        si = new SegmentWrite();
                        bySegment.put(a.getStreamSegmentName(), si);
                    }

                    si.bytes += a.getDataLength();
                    si.events += a.getNumOfEvents();
                }
            }

            this.globalSegmentWriteBytes.add(totalBytes);
            this.globalSegmentWriteEvents.add(totalEvents);
            for (val e : bySegment.entrySet()) {
                String segmentName = e.getKey();
                SegmentWrite si = e.getValue();
                val context = getWriteContext(e.getKey());
                if (context != null) {
                    context.recordWrite(si.bytes, si.events);
                    // Note: we could get stats for a transaction segment. We will simply ignore this as we
                    // do not maintain intermittent txn segment stats. Txn stats will be accounted for
                    // only upon txn commit. This is done via merge method. So here we can get a txn which
                    // we do not know about and hence we can get null and ignore.
                    val aggregates = context.getSegmentAggregates();
                    if (aggregates.update(si.bytes, si.events)) {
                        reportIfNeeded(segmentName, aggregates);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Record statistic failed", e);
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
        SegmentWriteContext context = getWriteContext(streamSegmentName);
        if (context != null) {
            context.recordWrite(dataLength, numOfEvents);
            val aggregates = context.getSegmentAggregates();
            if (aggregates.updateTx(dataLength, numOfEvents, txnCreationTime)) {
                reportIfNeededAsync(streamSegmentName, aggregates);
            }
        }
    }

    @Override
    public void readComplete(Duration elapsed) {
        this.readStreamSegment.reportSuccessEvent(elapsed);
    }

    @Override
    public void read(String segment, int length) {
        this.globalSegmentReadBytes.add(length);
        this.dynamicLogger.incCounterValue(SEGMENT_READ_BYTES, length, segmentTags(segment));
        this.readSizeDistribution.reportSuccessValue(length);
    }

    private void reportIfNeededAsync(String streamSegmentName, SegmentAggregates aggregates) {
        if (aggregates.reportIfNeeded(reportingDuration)) {
            this.executor.execute(() -> report(streamSegmentName, aggregates));
        }
    }

    private void reportIfNeeded(String streamSegmentName, SegmentAggregates aggregates) {
        if (aggregates.reportIfNeeded(reportingDuration)) {
            report(streamSegmentName, aggregates);
        }
    }

    private void report(String streamSegmentName, SegmentAggregates aggregates) {
        try {
            reporter.report(streamSegmentName,
                    aggregates.getTargetRate(), aggregates.getStartTime(),
                    aggregates.getTwoMinuteRate(), aggregates.getFiveMinuteRate(),
                    aggregates.getTenMinuteRate(), aggregates.getTwentyMinuteRate());
        } catch (Exception ex) {
            log.error("Unable to report Segment Aggregates for '{}'.", streamSegmentName, ex);
        }
    }

    @VisibleForTesting
    SegmentAggregates getSegmentAggregates(String streamSegmentName) {
        SegmentWriteContext context = cache.get(streamSegmentName);
        return context == null ? null : context.getSegmentAggregates();
    }

    private static class SegmentWrite {
        long bytes = 0;
        int events = 0;
    }

    @Data
    private static class AppendInfo {
        final String streamSegmentName;
        final long dataLength;
        final int numOfEvents;
        final Duration elapsed;
    }

    private static class SegmentWriteContext implements AutoCloseable {
        final Counter writeBytes;
        final Counter writeEvents;
        @Getter
        @Setter
        private volatile SegmentAggregates segmentAggregates;

        SegmentWriteContext(String segmentName, SegmentAggregates segmentAggregates) {
            this.segmentAggregates = segmentAggregates;
            String[] tags = segmentTags(segmentName);
            this.writeBytes = STATS_LOGGER.createCounter(SEGMENT_WRITE_BYTES, tags);
            this.writeEvents = STATS_LOGGER.createCounter(SEGMENT_WRITE_EVENTS, tags);
        }

        void recordWrite(long bytes, int events) {
            this.writeBytes.add(bytes);
            this.writeEvents.add(events);
        }

        @Override
        public void close() {
            this.writeBytes.close();
            this.writeEvents.close();
        }
    }
}
