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
package io.pravega.service.storage.impl.rocksdb;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Configuration for RocksDB-backed cache.
 */
public class RocksDBConfig {
    //region Config Names

    public static final Property<String> DATABASE_DIR = Property.named("dbDir", "/tmp/pravega/cache");
    private static final String COMPONENT_CODE = "rocksdb";

    //endregion

    //region Members

    /**
     * The path to the RocksDB database (in the local filesystem).
     */
    @Getter
    private final String databaseDir;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RocksDBConfig class.
     *
     * @param properties    The TypedProperties object to read Properties from.
     */
    private RocksDBConfig(TypedProperties properties) throws ConfigurationException {
        this.databaseDir = properties.get(DATABASE_DIR);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<RocksDBConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, RocksDBConfig::new);
    }

    //endregion
}
