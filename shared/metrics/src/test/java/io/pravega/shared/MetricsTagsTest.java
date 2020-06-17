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

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.net.InetAddress;

import static io.pravega.shared.MetricsTags.DEFAULT_HOSTNAME_KEY;
import static io.pravega.shared.MetricsTags.containerTag;
import static io.pravega.shared.MetricsTags.createHostTag;
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
    public void testCreateHostTag() throws Exception {
        //Scenario 1: system property is defined - property is taken
        String originalProperty = System.getProperty(DEFAULT_HOSTNAME_KEY);
        System.setProperty(DEFAULT_HOSTNAME_KEY, "expectedHostname");
        assertEquals("expectedHostname", createHostTag(DEFAULT_HOSTNAME_KEY)[1]);
        if (!Strings.isNullOrEmpty(originalProperty)) {
            System.setProperty(DEFAULT_HOSTNAME_KEY, originalProperty);
        }

        //Scenario 2: environment var is defined, and system property not defined - env var is taken
        String envVarDefined = null;
        //go through the list to find the env var with non empty/null value
        for (String envVarName: System.getenv().keySet()) {
            if (!Strings.isNullOrEmpty(System.getenv(envVarName))) {
                envVarDefined = envVarName;
                break;
            }
        }
        //test scenario 2 only if there is env var with non empty/null value; otherwise skip scenario 2
        if (envVarDefined != null) {
            originalProperty = System.getProperty(envVarDefined);
            System.clearProperty(envVarDefined);
            assertEquals(System.getenv(envVarDefined), createHostTag(envVarDefined)[1]);
            if (!Strings.isNullOrEmpty(originalProperty)) {
                System.setProperty(envVarDefined, originalProperty);
            }
        }

        //Scenario 3: system property not defined, env var not defined - localhost config is taken
        originalProperty = System.getProperty("NON_EXIST_ENV");
        System.clearProperty("NON_EXIST_ENV");
        assertEquals(InetAddress.getLocalHost().getHostName(), createHostTag("NON_EXIST_ENV")[1]);
        if (!Strings.isNullOrEmpty(originalProperty)) {
            System.setProperty("NON_EXIST_ENV", originalProperty);
        }
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
        String[] tags = segmentTags("_system/_tables/commonTables");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("_system", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("_tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("commonTables", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);

        tags = segmentTags("_system/_tables/scope/tablesInScope");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("_system", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("_tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("scope/tablesInScope", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);

        tags = segmentTags("scope/_tables/scope/stream/tablesInStream");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(MetricsTags.TAG_STREAM, tags[2]);
        assertEquals("_tables", tags[3]);
        assertEquals(MetricsTags.TAG_SEGMENT, tags[4]);
        assertEquals("scope/stream/tablesInStream", tags[5]);
        assertEquals(MetricsTags.TAG_EPOCH, tags[6]);
        assertEquals("0", tags[7]);
    }
}
