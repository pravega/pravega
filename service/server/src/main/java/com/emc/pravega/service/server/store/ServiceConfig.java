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

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * General Service Configuration.
 */
public class ServiceConfig extends ComponentConfig {
    //region Members

    public static final String COMPONENT_CODE = "pravegaservice";
    public static final String PROPERTY_CONTAINER_COUNT = "containerCount";
    public static final String PROPERTY_THREAD_POOL_SIZE = "threadPoolSize";
    public static final String PROPERTY_LISTENING_PORT = "listeningPort";
    public static final String PROPERTY_LISTENING_IP_ADDRESS = "listeningIPAddress";
    public static final String PROPERTY_ZK_URL = "zkURL";
    public static final String PROPERTY_ZK_RETRY_SLEEP_MS = "zkRetrySleepMs";
    public static final String PROPERTY_ZK_RETRY_COUNT = "zkRetryCount";
    public static final String PROPERTY_CLUSTER_NAME = "clusterName";

    private static final int DEFAULT_LISTENING_PORT = 12345;
    private static final int DEFAULT_THREAD_POOL_SIZE = 50;
    private static final String DEFAULT_ZK_URL = "zk1:2181";
    private static final int DEFAULT_ZK_RETRY_SLEEP_MS = 100;
    private static final int DEFAULT_ZK_RETRY_COUNT = 5;
    private static final String DEFAULT_CLUSTER_NAME = "pravega-cluster";

    private int containerCount;
    private int threadPoolSize;
    private int listeningPort;
    private String listeningIPAddress;
    private String zkURL;
    private int zkPort;
    private int zkRetrySleepMs;
    private int zkRetryCount;
    private String clusterName;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given
     *                                  properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string.
     */
    public ServiceConfig(Properties properties) throws ConfigurationException {
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
     * Gets a value indicating the TCP Port number to listen to.
     */
    public int getListeningPort() {
        return this.listeningPort;
    }

    /**
     * Gets a value indicating the IP address to listen to.
     */
    public String getListeningIPAddress() {
        return this.listeningIPAddress;
    }

    /**
     * Gets a value indicating the Zookeeper URL.
     */
    public String getZkURL() {
        return zkURL;
    }

    /**
     * Gets a value indicating the Zookeeper port.
     */
    public int getZkPort() {
        return zkPort;
    }

    /**
     * Gets a value indicating the sleep duration before retrying for Zookeeper connection.
     */
    public int getZkRetrySleepMs() {
        return zkRetrySleepMs;
    }

    /**
     * Gets a value indicating the retry count for a failed Zookeeper connection.
     */
    public int getZkRetryCount() {
        return zkRetryCount;
    }

    /**
     * Gets a value indicating the cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.containerCount = getInt32Property(PROPERTY_CONTAINER_COUNT);
        this.threadPoolSize = getInt32Property(PROPERTY_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
        this.listeningPort = getInt32Property(PROPERTY_LISTENING_PORT, DEFAULT_LISTENING_PORT);
        this.listeningIPAddress = getProperty(PROPERTY_LISTENING_IP_ADDRESS, null);
        if (this.listeningIPAddress == null) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            this.listeningIPAddress = getHostAddress();
        }
        this.zkURL = getProperty(PROPERTY_ZK_URL, DEFAULT_ZK_URL);
        this.zkRetrySleepMs = getInt32Property(PROPERTY_ZK_RETRY_SLEEP_MS, DEFAULT_ZK_RETRY_SLEEP_MS);
        this.zkRetryCount = getInt32Property(PROPERTY_ZK_RETRY_COUNT, DEFAULT_ZK_RETRY_COUNT);
        this.clusterName = getProperty(PROPERTY_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    }

    private static String getHostAddress() {
        //TODO: Find a better way to compute the host address. https://github.com/emccode/pravega/issues/162
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unable to get the Host Address", e);
        }
    }

    //endregion
}
