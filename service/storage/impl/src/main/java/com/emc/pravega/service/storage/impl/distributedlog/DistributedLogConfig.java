/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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

    public static final String COMPONENT_CODE = "dlog";
    public static final String PROPERTY_HOSTNAME = "hostname";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_NAMESPACE = "namespace";

    private static final String DEFAULT_HOSTNAME = "zk1";
    private static final int DEFAULT_PORT = 2181;
    private static final String DEFAULT_NAMESPACE = "messaging/distributedlog/mynamespace";

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
