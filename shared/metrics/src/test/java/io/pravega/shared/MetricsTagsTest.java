/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.net.InetAddress;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.shared.MetricsTags.*;
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
    public void testThrottlerTags() {
        String[] tags = throttlerTag(1, "Cache");
        assertEquals(MetricsTags.TAG_CONTAINER, tags[0]);
        assertEquals("1", tags[1]);
        assertEquals(MetricsTags.TAG_THROTTLER, tags[2]);
        assertEquals("Cache", tags[3]);
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

    @Test
    public void testExceptionTags() {
        val classNames = ImmutableMap
                .<String, String>builder()
                .put("A", "A")
                .put("B.", "B.")
                .put(".C", "C")
                .put("D.E.F", "F")
                .build();
        for (val logClassName : classNames.entrySet()) {
            // Check without exception.
            String[] tags = exceptionTag(logClassName.getKey(), null);
            checkExceptionTags(tags, logClassName.getValue(), "none");

            // Check with exceptions.
            for (val exceptionClassName : classNames.entrySet()) {
                tags = exceptionTag(logClassName.getKey(), exceptionClassName.getKey());
                checkExceptionTags(tags, logClassName.getValue(), exceptionClassName.getValue());
            }
        }
    }

    @Test
    public void testReaderGroupTags() {
        String[] tags = readerGroupTags("scope", "rg");
        assertEquals(MetricsTags.TAG_SCOPE, tags[0]);
        assertEquals("scope", tags[1]);
        assertEquals(TAG_READER_GROUP, tags[2]);
        assertEquals("rg", tags[3]);
    }

    @Test
    public void testEventProcessorTags() {
        String[] tags = eventProcessorTag(0, "myProcessor");
        assertEquals(TAG_CONTAINER, tags[0]);
        assertEquals("0", tags[1]);
        assertEquals(TAG_EVENT_PROCESSOR, tags[2]);
        assertEquals("myProcessor", tags[3]);
    }

    @Test
    public void testSegmentTagDirect() {
        String[] tags = segmentTagDirect("mySegment");
        assertEquals(TAG_SEGMENT, tags[0]);
        assertEquals("mySegment", tags[1]);
    }

    private void checkExceptionTags(String[] tags, String expectedClassTag, String expectedExceptionTag) {
        Assert.assertEquals(4, tags.length);
        Assert.assertEquals(MetricsTags.TAG_CLASS, tags[0]);
        Assert.assertEquals(expectedClassTag, tags[1]);
        Assert.assertEquals(MetricsTags.TAG_EXCEPTION, tags[2]);
        Assert.assertEquals(expectedExceptionTag, tags[3]);
    }
}
