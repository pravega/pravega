/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Retry;
import java.util.Properties;
import lombok.Getter;

/**
 * General configuration for DistributedLog Client.
 */
public class DistributedLogConfig extends ComponentConfig {
    //region Members

    public static final String COMPONENT_CODE = "dlog";
    public static final String PROPERTY_HOSTNAME = "hostname";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_NAMESPACE = "namespace";
    public static final String PROPERTY_RETRY_POLICY = "retryPolicy";

    private static final String DEFAULT_HOSTNAME = "zk1";
    private static final int DEFAULT_PORT = 2181;
    private static final String DEFAULT_NAMESPACE = "pravega/segmentstore/containers";
    private static final Retry.RetryWithBackoff DEFAULT_RETRY_POLICY = Retry.withExpBackoff(100, 4, 5, 30000);

    /**
     * The host name (no port) where DistributedLog is listening.
     */
    @Getter
    private String distributedLogHost;

    /**
     * The port where DistributedLog is listening.
     */
    @Getter
    private int distributedLogPort;

    /**
     * The DistributedLog Namespace to use.
     */
    @Getter
    private String distributedLogNamespace;

    /**
     * The Retry Policy base to use for all DistributedLog parameters.
     */
    @Getter
    private Retry.RetryWithBackoff retryPolicy;

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

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.distributedLogHost = getProperty(PROPERTY_HOSTNAME, DEFAULT_HOSTNAME);
        this.distributedLogPort = getInt32Property(PROPERTY_PORT, DEFAULT_PORT);
        this.distributedLogNamespace = getProperty(PROPERTY_NAMESPACE, DEFAULT_NAMESPACE);
        this.retryPolicy = getRetryWithBackoffProperty(PROPERTY_RETRY_POLICY, DEFAULT_RETRY_POLICY);
    }

    //endregion
}
