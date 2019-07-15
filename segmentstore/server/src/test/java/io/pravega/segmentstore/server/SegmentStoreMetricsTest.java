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

import io.pravega.common.AbstractTimer;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the SegmentStoreMetrics class.
 */
@Slf4j
public class SegmentStoreMetricsTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("segmentstore");

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
        final int recordedValues = 10;
        final int avgRecoveryTime = 10;
        OpStatsLogger recoveryTimes = statsLogger.createStats(MetricsNames.CONTAINER_RECOVERY_TIME);

        SegmentStoreMetrics.getRECOVERY_TIMES().set(recoveryTimes);
        Assert.assertEquals(0, recoveryTimes.toOpStatsData().getAvgLatencyMillis(), 0.0);
        for (int i = 0; i < recordedValues; i++) {
            SegmentStoreMetrics.recoveryCompleted(avgRecoveryTime);
        }
        Assert.assertEquals(nanoToMs(recoveryTimes.toOpStatsData().getAvgLatencyMillis()), avgRecoveryTime, 0.0);
        Assert.assertEquals(recordedValues, recoveryTimes.toOpStatsData().getNumSuccessfulEvents());
    }

    private double nanoToMs(double iniTime) {
        return iniTime / AbstractTimer.NANOS_TO_MILLIS;
    }
}
