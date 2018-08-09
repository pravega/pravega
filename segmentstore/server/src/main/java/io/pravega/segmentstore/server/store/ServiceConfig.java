/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import com.google.common.base.Strings;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.segmentstore.server.CachePolicy;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.Duration;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * General Service Configuration.
 */
public class ServiceConfig {
    //region Config Names

    public static final Property<Integer> CONTAINER_COUNT = Property.named("containerCount");
    public static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPoolSize", 30);
    public static final Property<Integer> STORAGE_THREAD_POOL_SIZE = Property.named("storageThreadPoolSize", 20);
    public static final Property<Integer> LISTENING_PORT = Property.named("listeningPort", 12345);
    public static final Property<Integer> PUBLISHED_PORT = Property.named("publishedPort");
    public static final Property<String> LISTENING_IP_ADDRESS = Property.named("listeningIPAddress", "");
    public static final Property<String> PUBLISHED_IP_ADDRESS = Property.named("publishedIPAddress", "");
    public static final Property<String> ZK_URL = Property.named("zkURL", "localhost:2181");
    public static final Property<Integer> ZK_RETRY_SLEEP_MS = Property.named("zkRetrySleepMs", 5000);
    public static final Property<Integer> ZK_RETRY_COUNT = Property.named("zkRetryCount", 5);
    public static final Property<Integer> ZK_SESSION_TIMEOUT_MS = Property.named("zkSessionTimeoutMs", 10000);
    public static final Property<Boolean> SECURE_ZK = Property.named("secureZK", false);
    public static final Property<String> ZK_TRUSTSTORE_LOCATION = Property.named("zkTrustStore", "");
    public static final Property<String> ZK_TRUST_STORE_PASSWORD_PATH = Property.named("zkTrustStorePasswordPath", "");
    public static final Property<String> CLUSTER_NAME = Property.named("clusterName", "pravega-cluster");
    public static final Property<DataLogType> DATALOG_IMPLEMENTATION = Property.named("dataLogImplementation", DataLogType.INMEMORY);
    public static final Property<StorageType> STORAGE_IMPLEMENTATION = Property.named("storageImplementation", StorageType.INMEMORY);
    public static final Property<Boolean> READONLY_SEGMENT_STORE = Property.named("readOnlySegmentStore", false);
    public static final Property<Boolean> ENABLE_TLS = Property.named("enableTls", false);
    public static final Property<String> CERT_FILE = Property.named("certFile", "");
    public static final Property<String> KEY_FILE = Property.named("keyFile", "");
    public static final Property<Long> CACHE_POLICY_MAX_SIZE = Property.named("cacheMaxSize", 16L * 1024 * 1024 * 1024);
    public static final Property<Integer> CACHE_POLICY_MAX_TIME = Property.named("cacheMaxTimeSeconds", 30 * 60);
    public static final Property<Integer> CACHE_POLICY_GENERATION_TIME = Property.named("cacheGenerationTimeSeconds", 5);

    public static final String COMPONENT_CODE = "pravegaservice";

    //endregion

    //region Storage Types

    public enum DataLogType {
        /**
         * DataLog is implemented by a BookKeeper Cluster.
         */
        BOOKKEEPER,

        /**
         * InMemory DataLog. Contents will be lost when the process exits.
         */
        INMEMORY
    }

    public enum StorageType {
        /**
         * Storage is implemented by a cluster exposing an ExtendedS3 API.
         */
        EXTENDEDS3,

        /**
         * Storage is implemented by a POSIX File System. This may be a local file system or an NFS mount.
         */
        FILESYSTEM,

        /**
         * Storage is implemented by a cluster exposing a HDFS API.
         */
        HDFS,

        /**
         * InMemory Storage. Contents will be lost when the process exits.
         */
        INMEMORY
    }

    //endregion

    //region Members

    /**
     * The number of containers in the system.
     */
    @Getter
    private final int containerCount;

    /**
     * The number of threads in the core Segment Store Thread Pool.
     */
    @Getter
    private final int coreThreadPoolSize;

    /**
     * The number of threads in the Thread Pool used for accessing Storage.
     */
    @Getter
    private final int storageThreadPoolSize;

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
     * Pravega segment store allows a configuration in which it connects to an IP address:port pair on the node and a
     * different IP address:port pair is advertised to the clients through controller.
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
     * The session timeout for Zookeeper.
     */
    @Getter
    private final int zkSessionTimeoutMs;

    /**
     * The retry count for a failed Zookeeper connection.
     */
    @Getter
    private final int zkRetryCount;

    /**
     * The flag to denote whether connection to ZK is secure.
     */
    @Getter
    private final boolean secureZK;

    /**
     * Location of trust store file to make a secure connection to ZK.
     */
    @Getter
    private final String zkTrustStore;

    /**
     * Location of password file to access ZK trust store.
     */
    @Getter
    private final String zkTrustStorePasswordPath;

    /**
     * The cluster name.
     */
    @Getter
    private final String clusterName;

    /**
     * The Type of DataLog Implementation to use.
     */
    @Getter
    private final DataLogType dataLogTypeImplementation;

    /**
     * The Type of Storage Implementation to use.
     */
    @Getter
    private final StorageType storageImplementation;

    /**
     * Whether this SegmentStore instance is Read-Only (i.e., it can only process reads from Storage and nothing else).
     * Note that if this is set to 'true', then many other settings will not apply. The most important other one to set
     * is 'Storage Implementation'.
     */
    @Getter
    private final boolean readOnlySegmentStore;

    /**
     * Enables TLS support for the serer.
     */
    @Getter
    private final boolean enableTls;

    /**
     * Represents the certificate file for the TLS server.
     */
    @Getter
    private final String certFile;

    /**
     * Represents the private key file for the TLS server.
     */
    @Getter
    private final String keyFile;

    /**
     * The CachePolicy, as defined in this configuration.
     */
    @Getter
    private final CachePolicy cachePolicy;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ServiceConfig(TypedProperties properties) throws ConfigurationException {
        this.containerCount = properties.getInt(CONTAINER_COUNT);
        this.coreThreadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        this.storageThreadPoolSize = properties.getInt(STORAGE_THREAD_POOL_SIZE);
        this.listeningPort = properties.getInt(LISTENING_PORT);

        int publishedPort;
        try {
            publishedPort = properties.getInt(PUBLISHED_PORT);
        } catch (ConfigurationException e) {
            publishedPort = this.listeningPort;
        }
        this.publishedPort = publishedPort;

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
        this.zkSessionTimeoutMs = properties.getInt(ZK_SESSION_TIMEOUT_MS);
        this.clusterName = properties.get(CLUSTER_NAME);
        this.dataLogTypeImplementation = properties.getEnum(DATALOG_IMPLEMENTATION, DataLogType.class);
        this.storageImplementation = properties.getEnum(STORAGE_IMPLEMENTATION, StorageType.class);
        this.readOnlySegmentStore = properties.getBoolean(READONLY_SEGMENT_STORE);
        this.secureZK = properties.getBoolean(SECURE_ZK);
        this.zkTrustStore = properties.get(ZK_TRUSTSTORE_LOCATION);
        this.zkTrustStorePasswordPath = properties.get(ZK_TRUST_STORE_PASSWORD_PATH);
        this.enableTls = properties.getBoolean(ENABLE_TLS);
        this.keyFile = properties.get(KEY_FILE);
        this.certFile = properties.get(CERT_FILE);
        long cachePolicyMaxSize = properties.getLong(CACHE_POLICY_MAX_SIZE);
        int cachePolicyMaxTime = properties.getInt(CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = properties.getInt(CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, Duration.ofSeconds(cachePolicyMaxTime), Duration.ofSeconds(cachePolicyGenerationTime));
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
