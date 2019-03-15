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

import static io.pravega.shared.MetricsTags.segmentTags;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link TableSegmentStatsRecorderImpl} class.
 */
public class TableSegmentStatsRecorderTest {
    private static final String SEGMENT_NAME = "scope/stream/TableSegment";
    private static final String[] SEGMENT_TAGS = segmentTags(SEGMENT_NAME);
    private static final Duration ELAPSED = Duration.ofMillis(123456);

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
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_UPDATE, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_REMOVE, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_GET, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, SEGMENT_TAGS);
        verify(r.getDynamicLogger()).freezeCounter(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, SEGMENT_TAGS);

        // Unconditional update.
        r.updateEntries(SEGMENT_NAME, 2, false, ELAPSED);
        verify(r.getUpdateUnconditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE), 2);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_UPDATE, 2, segmentTags(SEGMENT_NAME));

        // Conditional update.
        r.updateEntries(SEGMENT_NAME, 3, true, ELAPSED);
        verify(r.getUpdateConditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL), 3);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_UPDATE_CONDITIONAL, 3, segmentTags(SEGMENT_NAME));

        // Unconditional removal.
        r.removeKeys(SEGMENT_NAME, 4, false, ELAPSED);
        verify(r.getRemoveUnconditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE), 4);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_REMOVE, 4, segmentTags(SEGMENT_NAME));

        // Conditional removal.
        r.removeKeys(SEGMENT_NAME, 5, true, ELAPSED);
        verify(r.getRemoveConditional()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL), 5);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_REMOVE_CONDITIONAL, 5, segmentTags(SEGMENT_NAME));

        // Get Keys.
        r.getKeys(SEGMENT_NAME, 6, ELAPSED);
        verify(r.getGetKeys()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_GET), 6);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_GET, 6, segmentTags(SEGMENT_NAME));

        // Iterate Keys.
        r.iterateKeys(SEGMENT_NAME, 7, ELAPSED);
        verify(r.getIterateKeys()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS), 7);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_ITERATE_KEYS, 7, segmentTags(SEGMENT_NAME));

        // Iterate Entries.
        r.iterateEntries(SEGMENT_NAME, 8, ELAPSED);
        verify(r.getIterateEntries()).reportSuccessEvent(ELAPSED);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.globalMetricName(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES), 8);
        verify(r.getDynamicLogger()).incCounterValue(MetricsNames.TABLE_SEGMENT_ITERATE_ENTRIES, 8, segmentTags(SEGMENT_NAME));
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
