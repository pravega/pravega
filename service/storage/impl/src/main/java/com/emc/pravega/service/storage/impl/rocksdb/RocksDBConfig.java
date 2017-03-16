/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.rocksdb;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Configuration for RocksDB-backed cache.
 */
public class RocksDBConfig {
    //region Config Names

    public static final Property<String> DATABASE_DIR = new Property<>("dbDir", "/tmp/pravega/cache");
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
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<RocksDBConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, RocksDBConfig::new);
    }

    //endregion
}
