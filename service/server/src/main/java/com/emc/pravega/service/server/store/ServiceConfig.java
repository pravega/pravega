/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * General Service Configuration.
 */
public class ServiceConfig {
    //region Config Names

    public static final String PROPERTY_CONTAINER_COUNT = "containerCount";
    public static final String PROPERTY_THREAD_POOL_SIZE = "threadPoolSize";
    public static final String PROPERTY_LISTENING_PORT = "listeningPort";
    public static final String PROPERTY_LISTENING_IP_ADDRESS = "listeningIPAddress";
    public static final String PROPERTY_ZK_URL = "zkURL";
    public static final String PROPERTY_ZK_RETRY_SLEEP_MS = "zkRetrySleepMs";
    public static final String PROPERTY_ZK_RETRY_COUNT = "zkRetryCount";
    public static final String PROPERTY_CLUSTER_NAME = "clusterName";
    public static final String PROPERTY_CONTROLLER_URI = "controllerUri";
    public static final String PROPERTY_REQUEST_STREAM = "internalRequestStream";
    public static final String PROPERTY_INTERNAL_SCOPE = "internalScope";
    private static final String COMPONENT_CODE = "pravegaservice";

    private static final int DEFAULT_LISTENING_PORT = 12345;
    private static final int DEFAULT_THREAD_POOL_SIZE = 50;

    private static final String DEFAULT_ZK_URL = "localhost:2181";
    private static final int DEFAULT_ZK_RETRY_SLEEP_MS = 5000;

    private static final int DEFAULT_ZK_RETRY_COUNT = 5;
    private static final String DEFAULT_CLUSTER_NAME = "pravega-cluster";
    private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
    private static final String DEFAULT_INTERNAL_SCOPE = "pravega";
    private static final String DEFAULT_REQUEST_STREAM = "requeststream";

    //endregion

    //region Members

    /**
     * The number of containers in the system.
     */
    @Getter
    private final int containerCount;

    /**
     * The number of threads in the common thread pool.
     */
    @Getter
    private final int threadPoolSize;

    /**
     * The TCP Port number to listen to.
     */
    @Getter
    private final int listeningPort;

    /**
     * The IP address to listen to.
     */
    @Getter
    private final String listeningIPAddress;

    /**
     * The Zookeeper URL.
     */
    @Getter
    private final String zkURL;

    /**
     * The sleep duration before retrying for Zookeeper connection.
     */
    @Getter
    private final int zkRetrySleepMs;

    /**
     * The retry count for a failed Zookeeper connection.
     */
    @Getter
    private final int zkRetryCount;

    /**
     * The cluster name.
     */
    @Getter
    private final String clusterName;
    @Getter
    private final String controllerUri;
    @Getter
    private final String internalScope;
    @Getter
    private final String internalRequestStream;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ServiceConfig(TypedProperties properties) throws ConfigurationException {
        this.containerCount = properties.getInt32(PROPERTY_CONTAINER_COUNT);
        this.threadPoolSize = properties.getInt32(PROPERTY_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
        this.listeningPort = properties.getInt32(PROPERTY_LISTENING_PORT, DEFAULT_LISTENING_PORT);
        String ipAddress = properties.get(PROPERTY_LISTENING_IP_ADDRESS, null);
        if (ipAddress == null) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            ipAddress = getHostAddress();
        }

        this.listeningIPAddress = ipAddress;
        this.zkURL = properties.get(PROPERTY_ZK_URL, DEFAULT_ZK_URL);
        this.zkRetrySleepMs = properties.getInt32(PROPERTY_ZK_RETRY_SLEEP_MS, DEFAULT_ZK_RETRY_SLEEP_MS);
        this.zkRetryCount = properties.getInt32(PROPERTY_ZK_RETRY_COUNT, DEFAULT_ZK_RETRY_COUNT);
        this.clusterName = properties.get(PROPERTY_CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
        this.controllerUri = properties.get(PROPERTY_CONTROLLER_URI, DEFAULT_CONTROLLER_URI);
        this.internalScope = properties.get(PROPERTY_INTERNAL_SCOPE, DEFAULT_INTERNAL_SCOPE);
        this.internalRequestStream = properties.get(PROPERTY_REQUEST_STREAM, DEFAULT_REQUEST_STREAM);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ServiceConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ServiceConfig::new);
    }

    //endregion

    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }
}
