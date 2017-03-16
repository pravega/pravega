/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.store;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
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

    public static final Property<Integer> CONTAINER_COUNT = new Property<>("containerCount");
    public static final Property<Integer> THREAD_POOL_SIZE = new Property<>("threadPoolSize", 50);
    public static final Property<Integer> LISTENING_PORT = new Property<>("listeningPort", 12345);
    public static final Property<String> LISTENING_IP_ADDRESS = new Property<>("listeningIPAddress", "");
    public static final Property<String> ZK_URL = new Property<>("zkURL", "localhost:2181");
    public static final Property<Integer> ZK_RETRY_SLEEP_MS = new Property<>("zkRetrySleepMs", 5000);
    public static final Property<Integer> ZK_RETRY_COUNT = new Property<>("zkRetryCount", 5);
    public static final Property<String> CLUSTER_NAME = new Property<>("clusterName", "pravega-cluster");
    public static final Property<String> CONTROLLER_URI = new Property<>("controllerUri", "tcp://localhost:9090");
    public static final Property<String> REQUEST_STREAM  = new Property<>("internalRequestStream", "pravega");
    public static final Property<String> INTERNAL_SCOPE = new Property<>("internalScope", "requeststream");
    private static final String COMPONENT_CODE = "pravegaservice";

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
        this.containerCount = properties.getInt(CONTAINER_COUNT);
        this.threadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        this.listeningPort = properties.getInt(LISTENING_PORT);
        String ipAddress = properties.get(LISTENING_IP_ADDRESS);
        if (ipAddress == null || ipAddress.equals(LISTENING_IP_ADDRESS.getDefaultValue())) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            ipAddress = getHostAddress();
        }

        this.listeningIPAddress = ipAddress;
        this.zkURL = properties.get(ZK_URL);
        this.zkRetrySleepMs = properties.getInt(ZK_RETRY_SLEEP_MS);
        this.zkRetryCount = properties.getInt(ZK_RETRY_COUNT);
        this.clusterName = properties.get(CLUSTER_NAME);
        this.controllerUri = properties.get(CONTROLLER_URI);
        this.internalScope = properties.get(INTERNAL_SCOPE);
        this.internalRequestStream = properties.get(REQUEST_STREAM);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
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
