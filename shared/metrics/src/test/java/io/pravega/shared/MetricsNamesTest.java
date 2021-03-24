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

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Slf4j
public class MetricsNamesTest {

    @Test
    public void testFailMetricName() {

        assertNull(MetricsNames.failMetricName(null));
        assertEquals("", MetricsNames.failMetricName(""));
        assertEquals("tag_fail", MetricsNames.failMetricName("tag"));
        assertEquals("0_fail", MetricsNames.failMetricName("0"));
        assertEquals("tag1_fail", MetricsNames.failMetricName("tag1"));
        assertEquals("tag.tag_fail", MetricsNames.failMetricName("tag.tag"));
        assertEquals("tag_fail.1", MetricsNames.failMetricName("tag.1"));
        assertEquals("tag.tag1_fail", MetricsNames.failMetricName("tag.tag1"));
        assertEquals("tag1.tag2.tag3_fail", MetricsNames.failMetricName("tag1.tag2.tag3"));
        assertEquals("tag1.tag2_fail.3", MetricsNames.failMetricName("tag1.tag2.3"));
    }

    @Test
    public void testMetricKey() {
        MetricsNames.MetricKey keys = MetricsNames.metricKey("append_count.6");
        assertEquals("append_count.6", keys.getCacheKey());
        assertEquals("append_count.6", keys.getRegistryKey());

        keys = MetricsNames.metricKey("insert_latency", "container", "7");
        assertEquals("insert_latency.7", keys.getCacheKey());
        assertEquals("insert_latency", keys.getRegistryKey());

        keys = MetricsNames.metricKey("queue_size", "container", "8", "hostname", "localhost");
        assertEquals("queue_size.8.localhost", keys.getCacheKey());
        assertEquals("queue_size", keys.getRegistryKey());

        keys = MetricsNames.metricKey("create_segment_counter.9");
        assertEquals("create_segment_counter.9", keys.getCacheKey());
        assertEquals("create_segment_counter.9", keys.getRegistryKey());

        keys = MetricsNames.metricKey("create_segment_counter", "container", "9");
        assertEquals("create_segment_counter.9", keys.getCacheKey());
        assertEquals("create_segment_counter", keys.getRegistryKey());

        keys = MetricsNames.metricKey("write_latency", "container", "9");
        assertEquals("write_latency.9", keys.getCacheKey());
        assertEquals("write_latency", keys.getRegistryKey());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testMetricKeyWithTagNameOnly() {
        MetricsNames.metricKey("metric", "tagName");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testMetricKeyWithOddNumberTags() {
        MetricsNames.metricKey("metric", "tag1", "value1", "tag2");
    }

    @Test
    public void testMetricKeyWithSingleNull() {
        MetricsNames.MetricKey keys = MetricsNames.metricKey("metric", (String[]) null);
        assertEquals("metric", keys.getCacheKey());
        assertEquals("metric", keys.getRegistryKey());
    }

    @Test  (expected = IllegalArgumentException.class)
    public void testMetricKeyWithDoubleNull() {
        MetricsNames.metricKey("metric", null, null);
    }
}
