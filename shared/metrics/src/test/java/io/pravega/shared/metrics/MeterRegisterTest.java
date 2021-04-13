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
package io.pravega.shared.metrics;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.pravega.test.common.SerializedClassRunner;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(SerializedClassRunner.class)
public class MeterRegisterTest {
    
    private final CompositeMeterRegistry registry = new CompositeMeterRegistry();

    private void initMetrics() {
        SimpleMeterRegistry simple = new SimpleMeterRegistry();
        registry.clear();
        registry.add(simple);
    }
    
    @After
    public void tearDown() {
        registry.close();
    }
    
    @Test
    public void testMeterRegister() {
        initMetrics();
        Timer success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(2, success.count());
        Timer latencyValues = registry.find("name").timer();
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(2, latencyValues.count());
    }
    
    @Test
    public void testMeterReRegister() {
        initMetrics();
        Timer success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        success.close();
        
        Timer latencyValues = registry.find("name").timer();
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(2, latencyValues.count());
        
        initMetrics();
        success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);

        latencyValues = registry.find("name").timer();
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(3, latencyValues.count());
    }
    
}
