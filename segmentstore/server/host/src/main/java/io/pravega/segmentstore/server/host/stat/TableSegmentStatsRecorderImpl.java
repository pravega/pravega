/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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

import static io.pravega.shared.MetricsTags.segmentTags;

/**
 * Implementation for {@link TableSegmentStatsRecorder}.
 */
public class TableSegmentStatsRecorderImpl implements TableSegmentStatsRecorder {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("segmentstore");
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger createSegment = createLogger(MetricsNames.SEGMENT_CREATE_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger deleteSegment = createLogger(MetricsNames.SEGMENT_DELETE_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger updateConditional = createLogger(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger updateUnconditional = createLogger(MetricsNames.TABLE_SEGMENT_UPDATE_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger removeConditional = createLogger(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger removeUnconditional = createLogger(MetricsNames.TABLE_SEGMENT_REMOVE_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger getKeys = createLogger(MetricsNames.TABLE_SEGMENT_GET_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger iterateKeys = createLogger(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final OpStatsLogger iterateEntries = createLogger(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES_LATENCY);
    @Getter(AccessLevel.PACKAGE)
    private final DynamicLogger dynamicLogger = createDynamicLogger();

    //region AutoCloseable Implementation

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

    //endregion

    //region TableSegmentStatsRecorder Implementation

    @Override
    public void createTableSegment(String tableSegmentName, Duration elapsed) {
        getCreateSegment().reportSuccessEvent(elapsed);
    }

    @Override
    public void deleteTableSegment(String tableSegmentName, Duration elapsed) {
        getDeleteSegment().reportSuccessEvent(elapsed);
        String[] segmentTags = segmentTags(tableSegmentName);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_UPDATE, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_REMOVE, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_GET, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, segmentTags);
        getDynamicLogger().freezeCounter(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, segmentTags);
    }

    @Override
    public void updateEntries(String tableSegmentName, int entryCount, boolean conditional, Duration elapsed) {
        choose(conditional, getUpdateConditional(), getUpdateUnconditional()).reportSuccessEvent(elapsed);
        String countMetric = choose(conditional, MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, MetricsNames.TABLE_SEGMENT_UPDATE);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(countMetric), entryCount);
        getDynamicLogger().incCounterValue(countMetric, entryCount, segmentTags(tableSegmentName));
    }

    @Override
    public void removeKeys(String tableSegmentName, int keyCount, boolean conditional, Duration elapsed) {
        choose(conditional, getRemoveConditional(), getRemoveUnconditional()).reportSuccessEvent(elapsed);
        String countMetric = choose(conditional, MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, MetricsNames.TABLE_SEGMENT_REMOVE);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(countMetric), keyCount);
        getDynamicLogger().incCounterValue(countMetric, keyCount, segmentTags(tableSegmentName));
    }

    @Override
    public void getKeys(String tableSegmentName, int keyCount, Duration elapsed) {
        getGetKeys().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_GET), keyCount);
        getDynamicLogger().incCounterValue(MetricsNames.TABLE_SEGMENT_GET, keyCount, segmentTags(tableSegmentName));
    }

    @Override
    public void iterateKeys(String tableSegmentName, int resultCount, Duration elapsed) {
        getIterateKeys().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS), resultCount);
        getDynamicLogger().incCounterValue(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, resultCount, segmentTags(tableSegmentName));
    }

    @Override
    public void iterateEntries(String tableSegmentName, int resultCount, Duration elapsed) {
        getIterateEntries().reportSuccessEvent(elapsed);
        getDynamicLogger().incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES), resultCount);
        getDynamicLogger().incCounterValue(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, resultCount, segmentTags(tableSegmentName));
    }

    //endregion

    protected OpStatsLogger createLogger(String name) {
        return STATS_LOGGER.createStats(name);
    }

    protected DynamicLogger createDynamicLogger() {
        return MetricsProvider.getDynamicLogger();
    }

    private <T> T choose(boolean conditional, T whenConditional, T whenUnconditional) {
        return conditional ? whenConditional : whenUnconditional;
    }
}
