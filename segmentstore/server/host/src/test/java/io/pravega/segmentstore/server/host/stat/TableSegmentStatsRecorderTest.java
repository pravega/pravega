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
import io.pravega.shared.metrics.OpStatsLogger;
import java.time.Duration;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link TableSegmentStatsRecorderImpl} class.
 */
public class TableSegmentStatsRecorderTest {
    private static final String SEGMENT_NAME = "TableSegment";
    private static final Duration ELAPSED = Duration.ofMillis(123456);
    private static final String[] TO_FREEZE = new String[]{
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_GET, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, SEGMENT_NAME),
            MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, SEGMENT_NAME)};

    @Test
    public void testCreateSegment() {
        @Cleanup
        val r = new TestRecorder();

        // Create Segment.
        r.createTableSegment(SEGMENT_NAME, ELAPSED);
        verify(r.getCreateSegment()).reportSuccessEvent(ELAPSED);

        // Delete Segment.
        r.deleteTableSegment(SEGMENT_NAME, ELAPSED);
        verify(r.getDeleteSegment()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).freezeCounters(TO_FREEZE);

        // Unconditional update.
        r.updateEntries(SEGMENT_NAME, 2, false, ELAPSED);
        verify(r.getUpdateUnconditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE), 2);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE, SEGMENT_NAME), 2);

        // Conditional update.
        r.updateEntries(SEGMENT_NAME, 3, true, ELAPSED);
        verify(r.getUpdateConditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL), 3);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, SEGMENT_NAME), 3);

        // Unconditional removal.
        r.removeKeys(SEGMENT_NAME, 4, false, ELAPSED);
        verify(r.getRemoveUnconditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE), 4);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE, SEGMENT_NAME), 4);

        // Conditional removal.
        r.removeKeys(SEGMENT_NAME, 5, true, ELAPSED);
        verify(r.getRemoveConditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL), 5);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, SEGMENT_NAME), 5);

        // Get Keys.
        r.getKeys(SEGMENT_NAME, 6, ELAPSED);
        verify(r.getGetKeys()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_GET), 6);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_GET, SEGMENT_NAME), 6);

        // Iterate Keys.
        r.iterateKeys(SEGMENT_NAME, 7, ELAPSED);
        verify(r.getIterateKeys()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS), 7);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, SEGMENT_NAME), 7);

        // Iterate Entries.
        r.iterateEntries(SEGMENT_NAME, 8, ELAPSED);
        verify(r.getIterateEntries()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES), 8);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, SEGMENT_NAME), 8);
    }

    @RequiredArgsConstructor
    private static class TestRecorder extends TableSegmentStatsRecorderImpl {
        @Override
        protected OpStatsLogger createLogger(String name) {
            return mock(OpStatsLogger.class);
        }

        @Override
        protected DynamicLogger createDynamicLogger() {
            return mock(DynamicLogger.class);
        }
    }
}
