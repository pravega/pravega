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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import org.junit.Assert;
import org.junit.Test;
import java.time.Duration;

/**
 * Unit tests for {@link SlowDelaySuppliers}
 */
public class SlowDelaySuppliersTests {

    /**
     * Test for the Sinusoidal type of delay generated by the working of LTS
     */
    @Test
    public void testSinusoidal() {
       SlowDelaySuppliers.SinusoidalDelaySupplier delaySupplier = (SlowDelaySuppliers.SinusoidalDelaySupplier) SlowDelaySuppliers.getDurationSupplier(StorageExtraConfig.builder()
               .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
               .with(StorageExtraConfig.STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDistributionType.SINUSOIDAL_DISTRIBUTION_TYPE)
               .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 100)
               .with(StorageExtraConfig.STORAGE_SLOW_MODE_CYCLE_TIME, 90)
               .build());

        Assert.assertEquals(100, delaySupplier.calculateValue(0, 0, 90), 0);
        Assert.assertEquals(100, delaySupplier.calculateValue(90, 0, 90), 0);
        Assert.assertEquals(171, delaySupplier.calculateValue(45, 0, 90), 0);
        Assert.assertEquals(100, delaySupplier.calculateValue(0, 0, 90), 0);
        Assert.assertEquals(150, delaySupplier.calculateValue(30, 0, 90), 0);
    }

    /**
     * Test for the fixed delay generated in the working of LTS
     */
    @Test
    public void testFixed() {
        SlowDelaySuppliers.FixedDelaySupplier delaySupplier = (SlowDelaySuppliers.FixedDelaySupplier) SlowDelaySuppliers.getDurationSupplier(StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDistributionType.FIXED_DISTRIBUTION_TYPE)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 123)
                .build());

        Assert.assertEquals(Duration.ofMillis(123), delaySupplier.get());
    }

    /**
     * Test for the normal type of delay generated by the working of LTS
     */
    @Test
    public void testNormal() {
        SlowDelaySuppliers.GaussianDelaySupplier delaySupplier = (SlowDelaySuppliers.GaussianDelaySupplier) SlowDelaySuppliers.getDurationSupplier(StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDistributionType.NORMAL_DISTRIBUTION_TYPE)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 100)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_STD_DEV, 100)
                .build());

        Assert.assertEquals(170, delaySupplier.calculateValue(() -> 0.7));
        Assert.assertEquals(140, delaySupplier.calculateValue(() -> 0.4));
        Assert.assertEquals(220, delaySupplier.calculateValue(() -> 1.2));
        Assert.assertEquals(490, delaySupplier.calculateValue(() -> 3.9));
    }
}
