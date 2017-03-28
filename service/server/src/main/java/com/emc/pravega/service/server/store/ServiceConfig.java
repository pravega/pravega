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

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * General Service Configuration.
 */
public class ServiceConfig {
    //region Config Names

    public static final Property<Integer> CONTAINER_COUNT = Property.named("containerCount");
    public static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPoolSize", 50);
    public static final Property<Integer> LISTENING_PORT = Property.named("listeningPort", 12345);
    public static final Property<Integer> PUBLISHED_PORT = Property.named("publishedPort", -1);
    public static final Property<String> LISTENING_IP_ADDRESS = Property.named("listeningIPAddress", "");
    public static final Property<String> PUBLISHED_IP_ADDRESS = Property.named("publishedIPAddress", "");
    public static final Property<String> ZK_URL = Property.named("zkURL", "localhost:2181");
    public static final Property<Integer> ZK_RETRY_SLEEP_MS = Property.named("zkRetrySleepMs", 5000);
    public static final Property<Integer> ZK_RETRY_COUNT = Property.named("zkRetryCount", 5);
    public static final Property<String> CLUSTER_NAME = Property.named("clusterName", "pravega-cluster");
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
     * Pravega SSS allows a configuration in which it connects to an IP address:port pair on the node and a different
     * IP address:port pair is advertised to the clients through controller.
     * In this configuration: publishedIPAddress and publishedPort configs are defined and this pair is registered to
     * the controller. In case these configs need not be different, they are not defined and they default to
     * listeningIPAddress and listeningPort.
     */

    /**
     * The port registered with controller
     */
    @Getter
    private final int publishedPort;
    /**
     * The IP address registered with controller.
     */
    @Getter
    private final String publishedIPAddress;

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

        int publishedPort = properties.getInt(PUBLISHED_PORT);
        if (publishedPort != -1) {
             this.publishedPort = publishedPort;
        } else {
            this.publishedPort = this.listeningPort;
        }
        String ipAddress = properties.get(LISTENING_IP_ADDRESS);
        if (ipAddress == null || ipAddress.equals(LISTENING_IP_ADDRESS.getDefaultValue())) {
            // Can't put this in the 'defaultValue' above because that would cause getHostAddress to be evaluated every time.
            ipAddress = getHostAddress();
        }

        this.listeningIPAddress = ipAddress;
        String publishedIPAddress = properties.get(PUBLISHED_IP_ADDRESS);
        if (Strings.isNullOrEmpty(publishedIPAddress)) {
            this.publishedIPAddress = this.listeningIPAddress;
        } else {
            this.publishedIPAddress = publishedIPAddress;
        }
        this.zkURL = properties.get(ZK_URL);
        this.zkRetrySleepMs = properties.getInt(ZK_RETRY_SLEEP_MS);
        this.zkRetryCount = properties.getInt(ZK_RETRY_COUNT);
        this.clusterName = properties.get(CLUSTER_NAME);
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
