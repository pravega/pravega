/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.MetricRegistryUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.shared.MetricsTags.containerTag;
import static org.junit.Assert.*;

/**
 * Unit tests for the SegmentStoreMetrics class.
 */
@Slf4j
public class SegmentStoreMetricsTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                                                .with(MetricsConfig.ENABLE_STATISTICS, true)
                                                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    /**
     * Verify that the Segment Store recovery times are properly reported.
     */
    @Test
    public void testContainerRecoveryDurationMetric() {
        int containerId = 0;
        assertNull(MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)));
        SegmentStoreMetrics.recoveryCompleted(1000, containerId);
        assertEquals(1000, (long) MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)).value());
        SegmentStoreMetrics.recoveryCompleted(500, containerId);
        assertEquals(500, (long) MetricRegistryUtils.getGauge(MetricsNames.CONTAINER_RECOVERY_TIME, containerTag(containerId)).value());
    }
}
