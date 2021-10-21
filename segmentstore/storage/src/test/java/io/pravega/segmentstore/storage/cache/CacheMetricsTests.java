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
package io.pravega.segmentstore.storage.cache;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for the {@link CacheMetrics} class.
 */
@RunWith(SerializedClassRunner.class)
public class CacheMetricsTests {
    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    @Test
    public void testMetrics() {
        @Cleanup
        val c = new CacheMetrics();
        c.append(10);
        c.insert(20);
        c.delete(30);
        c.get(40);

        assertEquals(10, (long) MetricRegistryUtils.getCounter(MetricsNames.CACHE_APPEND_BYTES).count());
        assertEquals(20, (long) MetricRegistryUtils.getCounter(MetricsNames.CACHE_WRITE_BYTES).count());
        assertEquals(30, (long) MetricRegistryUtils.getCounter(MetricsNames.CACHE_DELETE_BYTES).count());
        assertEquals(40, (long) MetricRegistryUtils.getCounter(MetricsNames.CACHE_READ_BYTES).count());

        c.close();
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.CACHE_APPEND_BYTES));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.CACHE_WRITE_BYTES));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.CACHE_DELETE_BYTES));
        assertNull(MetricRegistryUtils.getCounter(MetricsNames.CACHE_READ_BYTES));

    }
}
