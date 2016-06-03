package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.*;

import java.util.Properties;

/**
 * General configuration for DistributedLog Client.
 */
public class DistributedLogConfig extends ComponentConfig {
    //region Members

    private static final String ComponentCode = "dlog";
    private String distributedLogHost;
    private int distributedLogPort;
    private String distributedLogNamespace;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DistributedLogConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public DistributedLogConfig(Properties properties) throws MissingPropertyException, InvalidPropertyValueException {
        super(properties, ComponentCode);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the host name (no port) where DistributedLog is listening.
     * @return
     */
    public String getDistributedLogHost() {
        return this.distributedLogHost;
    }

    /**
     * Gets a value indicating the port where DistributedLog is listening.
     * @return
     */
    public int getDistributedLogPort() {
        return this.distributedLogPort;
    }

    /**
     * Gets a value indicating the DistributedLog Namespace to use.
     * @return
     */
    public String getDistributedLogNamespace() {
        return this.distributedLogNamespace;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws MissingPropertyException, InvalidPropertyValueException {
        this.distributedLogHost = getProperty("hostname");
        this.distributedLogPort = getInt32Property("port");
        this.distributedLogNamespace = getProperty("namespace");
    }

    //endregion
}
