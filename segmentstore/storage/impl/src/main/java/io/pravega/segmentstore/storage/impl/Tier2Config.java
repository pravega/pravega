/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the Tier2.
 */
@Slf4j
public class Tier2Config {
    //region Config Names

    public static final Property<Boolean> ENABLE_HDFS = Property.named("enableHdfs", Boolean.TRUE);
    public static final Property<Boolean> ENABLE_NFS = Property.named("enableNfs", Boolean.FALSE);
    private static final String COMPONENT_CODE = "tier2";

    //endregion

    //region Members
    /**
     * Enable HDFS. This is default storage.
     */
    @Getter
    private final boolean enableHdfs;

    /**
     * Enable NFS. This is default storage.
     */
    @Getter
    private final boolean enableNfs;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the HDFSStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private Tier2Config(TypedProperties properties) throws ConfigurationException {
        this.enableHdfs = properties.getBoolean(ENABLE_HDFS);
        this.enableNfs = properties.getBoolean(ENABLE_NFS);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<Tier2Config> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, Tier2Config::new);
    }

    //endregion
}
