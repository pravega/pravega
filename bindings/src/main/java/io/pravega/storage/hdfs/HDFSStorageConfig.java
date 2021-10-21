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
package io.pravega.storage.hdfs;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the HDFS Storage component.
 */
@Slf4j
public class HDFSStorageConfig {
    //region Config Names

    public static final Property<String> URL = Property.named("connect.uri", "localhost:9000", "hdfsUrl");
    public static final Property<String> ROOT = Property.named("root", "", "hdfsRoot");
    public static final Property<Integer> REPLICATION = Property.named("replication.factor", 3, "replication");
    public static final Property<Integer> BLOCK_SIZE = Property.named("block.size", 1024 * 1024, "blockSize");
    public static final Property<Boolean> REPLACE_DATANODES_ON_FAILURE = Property.named("replaceDataNodesOnFailure.enable", true, "replaceDataNodesOnFailure");
    private static final String COMPONENT_CODE = "hdfs";

    //endregion

    //region Members

    /**
     * HDFS host URL. This is generally in host:port format
     */
    @Getter
    private final String hdfsHostURL;

    /**
     * Root of the Pravega owned HDFS path. All the directories/files under this path will be exclusively
     * owned by Pravega.
     */
    @Getter
    private final String hdfsRoot;

    /**
     * Decides the replication factor of the data stored on HDFS.
     * This can be used to control availability of HDFS data.
     */
    @Getter
    private final short replication;

    /**
     * The default block size that will be used to exchange data with HDFS.
     */
    @Getter
    private final long blockSize;

    /**
     * Whether to replace DataNodes on failure or not. This should be set to TRUE for deployments where sufficient data
     * nodes are available(More than Max(3, replication)), otherwise set to FALSE.
     */
    @Getter
    private final boolean replaceDataNodesOnFailure;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private HDFSStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.hdfsHostURL = properties.get(URL);
        this.hdfsRoot = properties.get(ROOT);
        this.replication = (short) properties.getInt(REPLICATION);
        this.blockSize = properties.getInt(BLOCK_SIZE);
        this.replaceDataNodesOnFailure = properties.getBoolean(REPLACE_DATANODES_ON_FAILURE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<HDFSStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, HDFSStorageConfig::new);
    }

    //endregion
}
