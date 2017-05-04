/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.local;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

public class SingleNodeConfig {
    //region config names
    public final static Property<Integer> ZK_PORT = Property.named("zkPort", 4000);
    public final static Property<Integer> SEGMENTSTORE_PORT = Property.named("segmentstorePort", 6000);
    public final static Property<Integer> CONTROLLER_PORT = Property.named("controllerPort", 9090);
    private static final String COMPONENT_CODE = "singlenode";
    //end region

    //region members

    /**
     * The Zookeeper port for singlenode
     */
    @Getter
    private final int zkPort;

    /**
     * The SegmentStore port for singlenode
     */
    @Getter
    private final int segmentStorePort;

    /**
     * The controller port for singlenode
     */
    @Getter
    private final int controllerPort;

    //end region

    //region constructor
    private SingleNodeConfig(TypedProperties properties) {
        this.zkPort = properties.getInt(ZK_PORT);
        this.segmentStorePort = properties.getInt(SEGMENTSTORE_PORT);
        this.controllerPort = properties.getInt(CONTROLLER_PORT);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<SingleNodeConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, SingleNodeConfig::new);
    }
    //end region
}
