/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.storage.impl.distributedlog;

import com.emc.nautilus.common.util.ComponentConfig;
import com.emc.nautilus.common.util.ConfigurationException;
import com.emc.nautilus.common.util.MissingPropertyException;

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
    public DistributedLogConfig(Properties properties) {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the host name (no port) where DistributedLog is listening.
     *
     * @return
     */
    public String getDistributedLogHost() {
        return this.distributedLogHost;
    }

    /**
     * Gets a value indicating the port where DistributedLog is listening.
     *
     * @return
     */
    public int getDistributedLogPort() {
        return this.distributedLogPort;
    }

    /**
     * Gets a value indicating the DistributedLog Namespace to use.
     *
     * @return
     */
    public String getDistributedLogNamespace() {
        return this.distributedLogNamespace;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.distributedLogHost = getProperty(PROPERTY_HOSTNAME);
        this.distributedLogPort = getInt32Property(PROPERTY_PORT);
        this.distributedLogNamespace = getProperty(PROPERTY_NAMESPACE);
    }

    //endregion
}
