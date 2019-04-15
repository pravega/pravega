/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static io.pravega.shared.MetricsTags.containerTag;
import static io.pravega.shared.MetricsTags.hostTag;
import static io.pravega.shared.MetricsTags.segmentTags;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.shared.MetricsTags.transactionTags;
import static org.junit.Assert.assertEquals;

@Slf4j
public class MetricsTagsTest {

    @Test
    public void testContainerTag() {
        String[] tag = containerTag(0);
        assertEquals(MetricsTags.TAG_CONTAINER, tag[0]);
        assertEquals("0", tag[1]);
    }

    @Test
    public void testHostTag() {
        String[] tag = hostTag("localhost");
        assertEquals(MetricsTags.TAG_HOST, tag[0]);
        assertEquals("localhost", tag[1]);
    }

    @Test
    public void testStreamTags() {
        String[] tags = streamTags("scope", "stream");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
    }

    @Test
    public void testTransactionTags() {
        String[] tags = transactionTags("scope", "stream", "123");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(MetricsTags.TAG_TRANSACTION, tags[4]);
        assertEquals("123", tags[5]);
    }

    @Test
    public void testSegmentTags() {
        String[] tags = segmentTags("scope/stream/segment.#epoch.1552095534");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("1552095534", tags[7]);

        //test missing scope and epoch
        tags = segmentTags("stream/segment");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("default", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("stream", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("segment", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
    }

    @Test
    public void testTableSegmentTags() {
        String[] tags = segmentTags("_system/tables/commonTables");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("_system", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("commonTables", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);

        tags = segmentTags("_system/tables/scope/tablesInScope");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("_system", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("scope/tablesInScope", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);

        tags = segmentTags("_system/tables/scope/stream/tablesInStream");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("_system", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("scope/stream/tablesInStream", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
    }
}
