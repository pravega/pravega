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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Extra Configuration for Storage Component.
 */
@Slf4j
public class StorageExtraConfig {

    public static final Property<Boolean> STORAGE_NO_OP_MODE = Property.named("noOp.mode.enable", false, "storageNoOpMode");
    public static final Property<Integer> STORAGE_WRITE_NO_OP_LATENCY = Property.named("noOp.write.latency.milliseconds", 20, "storageWriteNoOpLatencyMillis");
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
     * Creates a new instance of StorageExtraConfig.
     *
     * @param properties The TypedProperties object to read properties from.
     * @throws ConfigurationException
     */
    private StorageExtraConfig(TypedProperties properties) throws ConfigurationException {
        this.storageNoOpMode = properties.getBoolean(STORAGE_NO_OP_MODE);
        this.storageWriteNoOpLatencyMillis = properties.getInt(STORAGE_WRITE_NO_OP_LATENCY);
    }

    public static ConfigBuilder<StorageExtraConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, StorageExtraConfig::new);
    }

}
