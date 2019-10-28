/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.micrometer.core.instrument.Metrics;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Random;

import static io.pravega.shared.MetricsTags.containerTag;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Slf4j
public class SegmentStoreMetricsTests {
    
    @Test
    public void testContainerMetrics() {

        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build();
        MetricsProvider.initialize(metricsConfig);

        int containerId = new Random().nextInt(Integer.MAX_VALUE);
        SegmentStoreMetrics.Container containerMetrics = new SegmentStoreMetrics.Container(containerId);
        containerMetrics.createSegment();
        containerMetrics.deleteSegment();
        containerMetrics.append();
        containerMetrics.appendWithOffset();
        containerMetrics.updateAttributes();
        containerMetrics.getAttributes();
        containerMetrics.read();
        containerMetrics.getInfo();
        containerMetrics.mergeSegment();
        containerMetrics.seal();
        containerMetrics.truncate();

        assertNotNull(getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(containerId)));
        assertNotNull(getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(containerId)));

        containerMetrics.close();

        assertNull(getMeter(MetricsNames.CONTAINER_APPEND_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_APPEND_OFFSET_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_CREATE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_DELETE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_MERGE_SEGMENT_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_UPDATE_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_GET_ATTRIBUTES_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_READ_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_GET_INFO_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_SEAL_COUNT, containerTag(containerId)));
        assertNull(getMeter(MetricsNames.CONTAINER_TRUNCATE_COUNT, containerTag(containerId)));
    }
    
    private Object getMeter(String meterName, String... tags) {
        return Metrics.globalRegistry.find(meterName).tags(tags).summary();
    }
}
