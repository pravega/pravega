/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.util.UUID;
import org.junit.Test;

import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_APPEND_BLOCK_SIZE;
import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_APPEND_LATENCY;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;

public class ClientMetricKeysTest {

    @Test
    public void testMetricKey() {
        String[] tags = StreamSegmentNameUtils.segmentTags("scope/stream/10.#epoch.123");
        String metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".scope.stream.10.123", metric);

        tags = StreamSegmentNameUtils.segmentTags("scope/stream/10");
        metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".scope.stream.10.0", metric);

        tags = StreamSegmentNameUtils.segmentTags("stream/10");
        metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".default.stream.10.0", metric);
    }

    @Test
    public void testMetricKeyEmptyTags() {
        String metric = CLIENT_APPEND_LATENCY.metric( null);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey(), metric);

        metric = CLIENT_APPEND_LATENCY.metric();
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey(), metric);
    }

    @Test
    public void testMetricKeyWriterId() {
        String writerId = UUID.randomUUID().toString();
        String[] tags = StreamSegmentNameUtils.writerTags(writerId);
        String metric = CLIENT_APPEND_BLOCK_SIZE.metric(tags);
        assertEquals(CLIENT_APPEND_BLOCK_SIZE.getMetricKey() + "." + writerId, metric);

        tags = StreamSegmentNameUtils.segmentTags("scope/stream/10.#epoch.123", writerId);
        metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".scope.stream.10.123." + writerId, metric);

        tags = StreamSegmentNameUtils.segmentTags("scope/stream/10", writerId);
        metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".scope.stream.10.0." + writerId, metric);

        tags = StreamSegmentNameUtils.segmentTags("stream/10", writerId);
        metric = CLIENT_APPEND_LATENCY.metric(tags);
        assertEquals(CLIENT_APPEND_LATENCY.getMetricKey() + ".default.stream.10.0." + writerId, metric);

    }

    @Test
    public void testMetricKeyErrorCase() {
        assertThrows(IllegalArgumentException.class, () -> CLIENT_APPEND_LATENCY.metric( "scope"));
        assertThrows(IllegalArgumentException.class, () -> CLIENT_APPEND_LATENCY.metric( "", ""));
        assertThrows(IllegalArgumentException.class, () -> CLIENT_APPEND_LATENCY.metric( "", "scope1"));
        assertThrows(IllegalArgumentException.class, () -> CLIENT_APPEND_LATENCY.metric( "scope", ""));
    }
}
