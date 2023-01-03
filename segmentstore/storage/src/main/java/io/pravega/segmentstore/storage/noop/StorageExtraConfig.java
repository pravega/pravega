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
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.storage.mocks.StorageDelayDistributionType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Extra Configuration for Storage Component.
 */
@Slf4j
public class StorageExtraConfig {

    public static final Property<Boolean> STORAGE_NO_OP_MODE = Property.named("noOp.mode.enable", false, "storageNoOpMode");
    public static final Property<Integer> STORAGE_WRITE_NO_OP_LATENCY = Property.named("noOp.write.latency.milliseconds", 20, "storageWriteNoOpLatencyMillis");

    public static final Property<Boolean> STORAGE_SLOW_MODE = Property.named("slow.enable", false);
    public static final Property<Boolean> STORAGE_SLOW_MODE_INJECT_CHUNK_STORAGE_ONLY = Property.named("slow.inject.chunk.storage", true);
    public static final Property<Integer> STORAGE_SLOW_MODE_LATENCY_MEAN = Property.named("slow.latency.mean.ms", 500);
    public static final Property<Integer> STORAGE_SLOW_MODE_LATENCY_STD_DEV = Property.named("slow.latency.std.ms", 500);
    public static final Property<StorageDelayDistributionType> STORAGE_SLOW_MODE_DISTRIBUTION_TYPE = Property.named("slow.type", StorageDelayDistributionType.NORMAL_DISTRIBUTION_TYPE);
    public static final Property<Integer> STORAGE_SLOW_MODE_CYCLE_TIME = Property.named("slow.latency.cycle.ms", 300000);
    private static final String COMPONENT_CODE = "storageextra";

    /**
     * Latency in milliseconds applied for storage write in no-op mode
     */
    @Getter
    private final int storageWriteNoOpLatencyMillis;

    /**
     * Flag of No Operation Mode of the underlying tier-2 storage.
     */
    @Getter
    private final boolean storageNoOpMode;

    /**
     * Flag to enable slow mode.
     */
    @Getter
    private final boolean slowModeEnabled;

    /**
     * Flag to enable only the slow chunk storage mode.
     */
    @Getter
    private final boolean slowModeInjectChunkStorageOnly;

    /**
     * Latency in milliseconds applied for slow storage mode.
     */
    @Getter
    private final int slowModeLatencyMeanMillis;

    @Getter
    private final int slowModeLatencyStdDevMillis;

    @Getter
    private final StorageDelayDistributionType distributionType;

    @Getter
    private final int slowModeLatencyCycleTimeMillis;

    /**
     * Creates a new instance of StorageExtraConfig.
     *
     * @param properties The TypedProperties object to read properties from.
     * @throws ConfigurationException
     */
    private StorageExtraConfig(TypedProperties properties) throws ConfigurationException {
        this.storageNoOpMode = properties.getBoolean(STORAGE_NO_OP_MODE);
        this.storageWriteNoOpLatencyMillis = properties.getNonNegativeInt(STORAGE_WRITE_NO_OP_LATENCY);
        this.slowModeEnabled = properties.getBoolean(STORAGE_SLOW_MODE);
        this.slowModeInjectChunkStorageOnly = properties.getBoolean(STORAGE_SLOW_MODE_INJECT_CHUNK_STORAGE_ONLY);
        this.slowModeLatencyMeanMillis = properties.getNonNegativeInt(STORAGE_SLOW_MODE_LATENCY_MEAN);
        this.slowModeLatencyStdDevMillis = properties.getNonNegativeInt(STORAGE_SLOW_MODE_LATENCY_STD_DEV);
        this.distributionType = properties.getEnum(STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDelayDistributionType.class);
        this.slowModeLatencyCycleTimeMillis = properties.getNonNegativeInt(STORAGE_SLOW_MODE_CYCLE_TIME);
    }

    public static ConfigBuilder<StorageExtraConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, StorageExtraConfig::new);
    }

}
