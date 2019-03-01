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
import java.util.ArrayList;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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
        verify(r.createSegment).reportSuccessEvent(ELAPSED);

        // Delete Segment.
        r.deleteTableSegment(SEGMENT_NAME, ELAPSED);
        verify(r.deleteSegment).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).freezeCounters(TO_FREEZE);

        // Unconditional update.
        r.updateEntries(SEGMENT_NAME, 2, false, ELAPSED);
        verify(r.updateUnconditional).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE), 2);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE, SEGMENT_NAME), 2);

        // Conditional update.
        r.updateEntries(SEGMENT_NAME, 3, true, ELAPSED);
        verify(r.updateConditional).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL), 3);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, SEGMENT_NAME), 3);

        // Unconditional removal.
        r.removeKeys(SEGMENT_NAME, 4, false, ELAPSED);
        verify(r.removeUnconditional).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE), 4);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE, SEGMENT_NAME), 4);

        // Conditional removal.
        r.removeKeys(SEGMENT_NAME, 5, true, ELAPSED);
        verify(r.removeConditional).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL), 5);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, SEGMENT_NAME), 5);

        // Get Keys.
        r.getKeys(SEGMENT_NAME, 6, ELAPSED);
        verify(r.getKeys).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_GET), 6);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_GET, SEGMENT_NAME), 6);

        // Iterate Keys.
        r.iterateKeys(SEGMENT_NAME, 7, ELAPSED);
        verify(r.iterateKeys).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS), 7);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, SEGMENT_NAME), 7);

        // Iterate Entries.
        r.iterateEntries(SEGMENT_NAME, 8, ELAPSED);
        verify(r.iterateEntries).reportSuccessEvent(ELAPSED);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES), 8);
        verify(r.dynamicLogger).incCounterValue(MetricsNames.nameFromSegment(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, SEGMENT_NAME), 8);
    }

    @RequiredArgsConstructor
    private static class TestRecorder extends TableSegmentStatsRecorderImpl {
        @Getter(AccessLevel.PROTECTED)
        final OpStatsLogger createSegment = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger deleteSegment = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger updateConditional = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger updateUnconditional = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger removeConditional = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger removeUnconditional = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger getKeys = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger iterateKeys = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final OpStatsLogger iterateEntries = mock(OpStatsLogger.class);
        @Getter(AccessLevel.PROTECTED)
        private final DynamicLogger dynamicLogger = mock(DynamicLogger.class);

        @Override
        @SneakyThrows
        public void close() {
            super.close();

            // Validate that the loggers are properly closed.
            val candidates = new ArrayList<OpStatsLogger>();
            for (val field : this.getClass().getDeclaredFields()) {
                if (field.getType().isAssignableFrom(OpStatsLogger.class)) {
                    candidates.add((OpStatsLogger) field.get(this));
                }
            }

            for (val l : candidates) {
                verify(l).close();
            }
        }
    }
}
