/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for DistributedLog Client.
 */
public class DistributedLogConfig {
    //region Config Names

    public static final Property<String> HOSTNAME = new Property<>("hostname", "zk1");
    public static final Property<Integer> PORT = new Property<>("port", 2181);
    public static final Property<String> NAMESPACE = new Property<>("namespace", "pravega/segmentstore/containers");
    private static final String COMPONENT_CODE = "dlog";

    //endregion

    //region Members

    /**
     * The host name (no port) where DistributedLog is listening.
     */
    @Getter
    private final String distributedLogHost;

    /**
     * The port where DistributedLog is listening.
     */
    @Getter
    private final int distributedLogPort;

    /**
     * The DistributedLog Namespace to use.
     */
    @Getter
    private final String distributedLogNamespace;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private DistributedLogConfig(TypedProperties properties) throws ConfigurationException {
        this.distributedLogHost = properties.get(HOSTNAME);
        this.distributedLogPort = properties.getInt32(PORT);
        this.distributedLogNamespace = properties.get(NAMESPACE);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<DistributedLogConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, DistributedLogConfig::new);
    }

    //endregion
}
