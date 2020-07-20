/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.segmentstore.storage.StorageManagerLayoutType;
import io.pravega.segmentstore.storage.StorageManagerType;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * General Service Configuration.
 */
public class ServiceConfig {
    //region Config Names

    public static final Property<Integer> CONTAINER_COUNT = Property.named("container.count", null, "containerCount");
    public static final Property<Integer> PARALLEL_CONTAINER_STARTS = Property.named("container.parallelStarts", 2);
    public static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPool.core.size", 30, "threadPoolSize");
    public static final Property<Integer> STORAGE_THREAD_POOL_SIZE = Property.named("threadPool.storage.size", 200, "storageThreadPoolSize");
    public static final Property<Integer> LOW_PRIORITY_THREAD_POOL_SIZE = Property.named("threadPool.lowPriorityTasks.size", 10, "lowPriorityThreadPoolSize");
    public static final Property<Integer> LISTENING_PORT = Property.named("service.listener.port", 12345, "listeningPort");
    public static final Property<Integer> PUBLISHED_PORT = Property.named("service.published.port", null, "publishedPort");
    public static final Property<String> LISTENING_IP_ADDRESS = Property.named("service.listener.host.nameOrIp", "", "listeningIPAddress");
    public static final Property<String> PUBLISHED_IP_ADDRESS = Property.named("service.published.host.nameOrIp", "", "publishedIPAddress");
    public static final Property<String> ZK_URL = Property.named("zk.connect.uri", "localhost:2181", "zkURL");
    public static final Property<Integer> ZK_RETRY_SLEEP_MS = Property.named("zk.connect.retries.interval.milliseconds", 5000, "zkRetrySleepMs");
    public static final Property<Integer> ZK_RETRY_COUNT = Property.named("zk.connect.retries.count.max", 5, "zkRetryCount");
    public static final Property<Integer> ZK_SESSION_TIMEOUT_MS = Property.named("zk.connect.sessionTimeout.milliseconds", 10000, "zkSessionTimeoutMs");
    public static final Property<Boolean> SECURE_ZK = Property.named("zk.connect.security.enable", false, "secureZK");
    public static final Property<String> ZK_TRUSTSTORE_LOCATION = Property.named("zk.connect.security.tls.trustStore.location", "", "zkTrustStore");
    public static final Property<String> ZK_TRUST_STORE_PASSWORD_PATH = Property.named("zk.connect.security.tls.trustStore.pwd.location", "", "zkTrustStorePasswordPath");

    // Not changing this configuration property (to "cluster.name"), as it is set by Pravega operator, and changing this
    // will require simultaneous changes there. So, we'll change this at a later time, employing strategy like this:
    // 1. Modify the operator to set this old, as well as the new property.
    // 2. Modify this property to use the new key, with legacy key name set as the old key.
    // 3. Remove old property from the operator.
    public static final Property<String> CLUSTER_NAME = Property.named("clusterName", "pravega-cluster");
    public static final Property<DataLogType> DATALOG_IMPLEMENTATION = Property.named("dataLog.impl.name", DataLogType.INMEMORY, "dataLogImplementation");
    public static final Property<StorageType> STORAGE_IMPLEMENTATION = Property.named("storage.impl.name", StorageType.HDFS, "storageImplementation");
    public static final Property<StorageManagerLayoutType> STORAGE_LAYOUT = Property.named("storageLayout", StorageManagerLayoutType.LEGACY);
    public static final Property<StorageManagerType> STORAGE_MANAGER = Property.named("storageManager", StorageManagerType.NONE);
    public static final Property<Boolean> READONLY_SEGMENT_STORE = Property.named("readOnly.enable", false, "readOnlySegmentStore");
    public static final Property<Long> CACHE_POLICY_MAX_SIZE = Property.named("cache.size.max", 4L * 1024 * 1024 * 1024, "cacheMaxSize");
    public static final Property<Integer> CACHE_POLICY_TARGET_UTILIZATION = Property.named("cache.utilization.percent.target", (int) (100 * CachePolicy.DEFAULT_TARGET_UTILIZATION), "cacheTargetUtilizationPercent");
    public static final Property<Integer> CACHE_POLICY_MAX_UTILIZATION = Property.named("cache.utilization.percent.max", (int) (100 * CachePolicy.DEFAULT_MAX_UTILIZATION), "cacheMaxUtilizationPercent");
    public static final Property<Integer> CACHE_POLICY_MAX_TIME = Property.named("cache.time.seconds.max", 30 * 60, "cacheMaxTimeSeconds");
    public static final Property<Integer> CACHE_POLICY_GENERATION_TIME = Property.named("cache.generation.duration.seconds", 1, "cacheGenerationTimeSeconds");
    public static final Property<Boolean> REPLY_WITH_STACK_TRACE_ON_ERROR = Property.named("request.replyWithStackTraceOnError.enable", false, "replyWithStackTraceOnError");
    public static final Property<String> INSTANCE_ID = Property.named("instance.id", "");

    // TLS-related config for the service
    public static final Property<Boolean> ENABLE_TLS = Property.named("security.tls.enable", false, "enableTls");
    public static final Property<String> CERT_FILE = Property.named("security.tls.server.certificate.location", "", "certFile");
    public static final Property<String> KEY_FILE = Property.named("security.tls.server.privateKey.location", "", "keyFile");
    public static final Property<Boolean> ENABLE_TLS_RELOAD = Property.named("security.tls.certificate.autoReload.enable", false, "enableTlsReload");

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
     * The number of threads in the thread pool that runs low priority tasks.
     */
    @Getter
    private final int lowPriorityThreadPoolSize;

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

    /*
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
     * Number of segment containers that a Segment Store will start (and recover) in parallel.
     */
    @Getter
    private final int parallelContainerStarts;

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
     * The Type of Storage Layout to use.
     */
    @Getter
    private final StorageManagerLayoutType storageLayout;

    /**
     * The Type of Storage manager to use.
     */
    @Getter
    private final StorageManagerType storageManager;

    /**
     * Whether this SegmentStore instance is Read-Only (i.e., it can only process reads from Storage and nothing else).
     * Note that if this is set to 'true', then many other settings will not apply. The most important other one to set
     * is 'Storage Implementation'.
     */
    @Getter
    private final boolean readOnlySegmentStore;

    /**
     * Enables TLS support for the server.
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
     * Enables automatic reloading of SSL/TLS when the TLS certificate is modified.
     */
    @Getter
    private final boolean enableTlsReload;

    /**
     * The CachePolicy, as defined in this configuration.
     */
    @Getter
    private final CachePolicy cachePolicy;

    /**
     * Defines whether server-side stack traces should be send to clients as part of an error response.
     */
    @Getter
    private final boolean replyWithStackTraceOnError;

    /**
     * Gets a value that uniquely identifies the Service. This is useful if multiple Service Instances share the same
     * log files or during testing, when multiple Service Instances run in the same process. This value will be prefixed
     * to the names of all Threads used by this Service Instance, which should make log parsing easier.
     */
    @Getter
    private final String instanceId;

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
        this.lowPriorityThreadPoolSize = properties.getInt(LOW_PRIORITY_THREAD_POOL_SIZE);
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
        this.parallelContainerStarts = properties.getInt(PARALLEL_CONTAINER_STARTS);
        this.zkURL = properties.get(ZK_URL);
        this.zkRetrySleepMs = properties.getInt(ZK_RETRY_SLEEP_MS);
        this.zkRetryCount = properties.getInt(ZK_RETRY_COUNT);
        this.zkSessionTimeoutMs = properties.getInt(ZK_SESSION_TIMEOUT_MS);
        this.clusterName = properties.get(CLUSTER_NAME);
        this.dataLogTypeImplementation = properties.getEnum(DATALOG_IMPLEMENTATION, DataLogType.class);
        this.storageImplementation = properties.getEnum(STORAGE_IMPLEMENTATION, StorageType.class);
        this.storageLayout = properties.getEnum(STORAGE_LAYOUT, StorageManagerLayoutType.class);
        this.storageManager = properties.getEnum(STORAGE_MANAGER, StorageManagerType.class);
        this.readOnlySegmentStore = properties.getBoolean(READONLY_SEGMENT_STORE);
        this.secureZK = properties.getBoolean(SECURE_ZK);
        this.zkTrustStore = properties.get(ZK_TRUSTSTORE_LOCATION);
        this.zkTrustStorePasswordPath = properties.get(ZK_TRUST_STORE_PASSWORD_PATH);
        this.enableTls = properties.getBoolean(ENABLE_TLS);
        this.keyFile = properties.get(KEY_FILE);
        this.certFile = properties.get(CERT_FILE);
        this.enableTlsReload = properties.getBoolean(ENABLE_TLS_RELOAD);
        long cachePolicyMaxSize = properties.getLong(CACHE_POLICY_MAX_SIZE);
        double cachePolicyTargetUtilization = properties.getInt(CACHE_POLICY_TARGET_UTILIZATION) / 100.0;
        double cachePolicyMaxUtilization = properties.getInt(CACHE_POLICY_MAX_UTILIZATION) / 100.0;
        int cachePolicyMaxTime = properties.getInt(CACHE_POLICY_MAX_TIME);
        int cachePolicyGenerationTime = properties.getInt(CACHE_POLICY_GENERATION_TIME);
        this.cachePolicy = new CachePolicy(cachePolicyMaxSize, cachePolicyTargetUtilization, cachePolicyMaxUtilization,
                Duration.ofSeconds(cachePolicyMaxTime), Duration.ofSeconds(cachePolicyGenerationTime));
        this.replyWithStackTraceOnError = properties.getBoolean(REPLY_WITH_STACK_TRACE_ON_ERROR);
        this.instanceId = properties.get(INSTANCE_ID);
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

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("containerCount: %d, ", containerCount))
                .append(String.format("coreThreadPoolSize: %d, ", coreThreadPoolSize))
                .append(String.format("storageThreadPoolSize: %d, ", storageThreadPoolSize))
                .append(String.format("listeningPort: %d, ", listeningPort))
                .append(String.format("listeningIPAddress: %s, ", listeningIPAddress))
                .append(String.format("publishedPort: %d, ", publishedPort))
                .append(String.format("publishedIPAddress: %s, ", publishedIPAddress))
                .append(String.format("parallelContainerStarts: %d, ", parallelContainerStarts))
                .append(String.format("zkURL: %s, ", zkURL))
                .append(String.format("zkRetrySleepMs: %d, ", zkRetrySleepMs))
                .append(String.format("zkSessionTimeoutMs: %d, ", zkSessionTimeoutMs))
                .append(String.format("zkRetryCount: %d, ", zkRetryCount))
                .append(String.format("secureZK: %b, ", secureZK))
                .append(String.format("zkTrustStore is %s, ",
                        Strings.isNullOrEmpty(zkTrustStore) ? "unspecified" : "specified"))
                .append(String.format("zkTrustStorePasswordPath is %s, ",
                        Strings.isNullOrEmpty(zkTrustStorePasswordPath) ? "unspecified" : "specified"))
                .append(String.format("clusterName: %s, ", clusterName))
                .append(String.format("dataLogTypeImplementation: %s, ", dataLogTypeImplementation.name()))
                .append(String.format("storageImplementation: %s, ", storageImplementation.name()))
                .append(String.format("readOnlySegmentStore: %b, ", readOnlySegmentStore))
                .append(String.format("enableTls: %b, ", enableTls))
                .append(String.format("certFile is %s, ",
                        Strings.isNullOrEmpty(certFile) ? "unspecified" : "specified"))
                .append(String.format("keyFile is %s, ",
                        Strings.isNullOrEmpty(keyFile) ? "unspecified" : "specified"))
                .append(String.format("enableTlsReload: %b, ", enableTlsReload))
                .append(String.format("cachePolicy is %s, ", (cachePolicy != null) ? cachePolicy.toString() : "null"))
                .append(String.format("replyWithStackTraceOnError: %b, ", replyWithStackTraceOnError))
                .append(String.format("instanceId: %s", instanceId))
                .append(")")
                .toString();
    }

    @SneakyThrows(UnknownHostException.class)
    private static String getHostAddress() {
        return Inet4Address.getLocalHost().getHostAddress();
    }
}
