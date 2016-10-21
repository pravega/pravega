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

package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.MissingPropertyException;

import java.util.Properties;

/**
 * General Service Configuration.
 */
public class ServiceConfig extends ComponentConfig {
    //region Members

    public static final String COMPONENT_CODE = "logservice";
    public static final String PROPERTY_CONTAINER_COUNT = "containerCount";
    public static final String PROPERTY_THREAD_POOL_SIZE = "threadPoolSize";
    public static final String PROPERTY_LISTENING_PORT = "listeningPort";
    private int containerCount;
    private int threadPoolSize;
    private int listeningPort;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public ServiceConfig(Properties properties) {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the number of containers in the system.
     */
    public int getContainerCount() {
        return this.containerCount;
    }

    /**
     * Gets a value indicating the number of threads in the common thread pool.
     */
    public int getThreadPoolSize() {
        return this.threadPoolSize;
    }

    /**
     * Gets a value indicating the TCP Port number to listed to.
     */
    public int getListeningPort() {
        return this.listeningPort;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.containerCount = getInt32Property(PROPERTY_CONTAINER_COUNT);
        this.threadPoolSize = getInt32Property(PROPERTY_THREAD_POOL_SIZE);
        this.listeningPort = getInt32Property(PROPERTY_LISTENING_PORT);
    }

    //endregion
}
