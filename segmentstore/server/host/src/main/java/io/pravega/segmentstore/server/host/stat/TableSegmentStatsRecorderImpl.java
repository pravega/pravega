/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Getter;

public class TableSegmentStatsRecorderImpl implements TableSegmentStatsRecorder {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger createSegment = STATS_LOGGER.createStats(MetricsNames.SEGMENT_CREATE_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger deleteSegment = STATS_LOGGER.createStats(MetricsNames.SEGMENT_DELETE_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger updateConditional = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger updateUnconditional = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_UPDATE_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger removeConditional = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger removeUnconditional = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_REMOVE_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger getKeys = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_GET_KEYS_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger iterateKeys = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS_LATENCY);
    @Getter(AccessLevel.PROTECTED)
    private final OpStatsLogger iterateEntries = STATS_LOGGER.createStats(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES_LATENCY);

    @Getter(AccessLevel.PROTECTED)
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    @Override
    public void close() {
        this.createSegment.close();
        this.deleteSegment.close();
        this.updateConditional.close();
        this.updateUnconditional.close();
        this.removeConditional.close();
        this.removeUnconditional.close();
        this.getKeys.close();
        this.iterateKeys.close();
        this.iterateEntries.close();
    }

    @Override
    public void createTableSegment(String tableSegmentName, Duration elapsed) {
        getCreateSegment().reportSuccessEvent(elapsed);
    }

    @Override
    public void deleteTableSegment(String tableSegmentName, Duration elapsed) {
        getDeleteSegment().reportSuccessEvent(elapsed);
        getDynamicLogger().freezeCounters(
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_GET, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, tableSegmentName),
                MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, tableSegmentName));
    }

    @Override
    public void updateEntries(String tableSegmentName, int entryCount, boolean conditional, Duration elapsed) {
        choose(conditional, getUpdateConditional(), getUpdateUnconditional()).reportSuccessEvent(elapsed);
        String countMetric = choose(conditional, MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, MetricsNames.TABLE_SEGMENT_UPDATE);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(countMetric), entryCount);
        getDynamicLogger().incCounterValue(MetricsNames.nameFromSegment(countMetric, tableSegmentName), entryCount);
    }

    @Override
    public void removeKeys(String tableSegmentName, int keyCount, boolean conditional, Duration elapsed) {
        choose(conditional, getRemoveConditional(), getRemoveUnconditional()).reportSuccessEvent(elapsed);
        String countMetric = choose(conditional, MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, MetricsNames.TABLE_SEGMENT_REMOVE);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(countMetric), keyCount);
        getDynamicLogger().incCounterValue(MetricsNames.nameFromSegment(countMetric, tableSegmentName), keyCount);
    }

    @Override
    public void getKeys(String tableSegmentName, int keyCount, Duration elapsed) {
        getGetKeys().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_GET), keyCount);
        getDynamicLogger().incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_GET, tableSegmentName), keyCount);
    }

    @Override
    public void iterateKeys(String tableSegmentName, int resultCount, Duration elapsed) {
        getIterateKeys().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS), resultCount);
        getDynamicLogger().incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, tableSegmentName), resultCount);
    }

    @Override
    public void iterateEntries(String tableSegmentName, int resultCount, Duration elapsed) {
        getIterateEntries().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES), resultCount);
        getDynamicLogger().incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, tableSegmentName), resultCount);
    }

    private <T> T choose(boolean conditional, T whenConditional, T whenUnconditional) {
        return conditional ? whenConditional : whenUnconditional;
    }
}
