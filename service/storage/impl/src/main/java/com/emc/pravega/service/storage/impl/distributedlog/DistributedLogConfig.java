/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.util.Properties;

/**
 * General configuration for DistributedLog Client.
 */
public class DistributedLogConfig extends ComponentConfig {
    //region Members

    public static final String PROPERTY_HOSTNAME = "hostname";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_NAMESPACE = "namespace";
    private static final String COMPONENT_CODE = "dlog";

    private static final String DEFAULT_HOSTNAME = "zk1";
    private static final int DEFAULT_PORT = 2181;
    private static final String DEFAULT_NAMESPACE = "pravega/segmentstore/containers";

    private String distributedLogHost;
    private int distributedLogPort;
    private String distributedLogNamespace;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public DistributedLogConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static Builder<DistributedLogConfig> builder() {
        return ComponentConfig.builder(DistributedLogConfig.class, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the host name (no port) where DistributedLog is listening.
     */
    public String getDistributedLogHost() {
        return this.distributedLogHost;
    }

    /**
     * Gets a value indicating the port where DistributedLog is listening.
     */
    public int getDistributedLogPort() {
        return this.distributedLogPort;
    }

    /**
     * Gets a value indicating the DistributedLog Namespace to use.
     */
    public String getDistributedLogNamespace() {
        return this.distributedLogNamespace;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.distributedLogHost = getProperty(PROPERTY_HOSTNAME, DEFAULT_HOSTNAME);
        this.distributedLogPort = getInt32Property(PROPERTY_PORT, DEFAULT_PORT);
        this.distributedLogNamespace = getProperty(PROPERTY_NAMESPACE, DEFAULT_NAMESPACE);
    }

    //endregion
}
