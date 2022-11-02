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
package io.pravega.segmentstore.storage.noop;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.storage.mocks.StorageDelayDistributionType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StorageExtraConfigTest {

    @Test
    public void testDefault() {
        StorageExtraConfig defaultConfig = StorageExtraConfig.builder().build();
        assertEquals(20, defaultConfig.getStorageWriteNoOpLatencyMillis());
        assertEquals(false, defaultConfig.isStorageNoOpMode());
        assertEquals(false, defaultConfig.isSlowModeEnabled());
        assertEquals(true, defaultConfig.isSlowModeInjectChunkStorageOnly());
    }

    @Test
    public void testLatency() {
        ConfigBuilder<StorageExtraConfig> builder = StorageExtraConfig.builder();
        builder.with(Property.named("storageWriteNoOpLatencyMillis"), 50);
        assertEquals(50, builder.build().getStorageWriteNoOpLatencyMillis());
    }

    @Test
    public void testNoOpSwitch() {
        ConfigBuilder<StorageExtraConfig> builder = StorageExtraConfig.builder();
        builder.with(Property.named("storageNoOpMode"), true);
        assertEquals(true, builder.build().isStorageNoOpMode());
    }

    @Test
    public void testSlowModeEnabled() {
        ConfigBuilder<StorageExtraConfig> builder = StorageExtraConfig.builder();
        builder.with(Property.named("slow.enable"), true);
        builder.with(Property.named("slow.inject.chunk.storage"), false);
        assertEquals(true, builder.build().isSlowModeEnabled());
        assertEquals(false, builder.build().isSlowModeInjectChunkStorageOnly());
    }

    @Test
    public void testSlowModeProperties() {
        ConfigBuilder<StorageExtraConfig> builder = StorageExtraConfig.builder();
        builder.with(Property.named("slow.latency.mean.ms"), 42);
        builder.with(Property.named("slow.latency.std.ms"), 43);
        builder.with(Property.named("slow.latency.cycle.ms"), 44);
        builder.with(Property.named("slow.type"), StorageDelayDistributionType.FIXED_DISTRIBUTION_TYPE);
        assertEquals(42, builder.build().getSlowModeLatencyMeanMillis());
        assertEquals(43, builder.build().getSlowModeLatencyStdDevMillis());
        assertEquals(44, builder.build().getSlowModeLatencyCycleTimeMillis());
        assertEquals(StorageDelayDistributionType.FIXED_DISTRIBUTION_TYPE, builder.build().getDistributionType());
    }
}
