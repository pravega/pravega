/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.util;

import com.google.common.base.Strings;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.shared.metrics.MetricsConfig;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Utility class to supply Controller Configuration.
 *
 * The configuration values are retrieved using the following order, with every one overriding previously loaded values:
 * 1. Environment Variables ({@link System#getenv()}).
 * 2. System Properties ({@link System#getProperties()}.
 * 3. The configuration file. By default a 'controller.config.properties' file is sought in the classpath; this can be
 * overridden by setting the 'conf.file' system property to point to another one.
 * 4. All currently loaded values will be resolved against themselves.
 * 5. Anything which is not supplied via the methods above will be defaulted to the values defined in this class.
 *
 * Configuration values can be resolved against themselves by referencing them using a special syntax. Any value
 * of the form '${CFG}' will lookup the already loaded config value with key 'CFG' and, if defined and non-empty, it will
 * use that config value as the final value (if not defined or empty, it will not be included in the final result and the
 * default value (step 5) will be used). Chained resolution is not supported (i.e., CFG1=${CFG2};CFG2=${CFG3} will not
 * set CFG1 to the value of CFG3).
 */
@Slf4j
public final class Config {

    //#region Properties

    //RPC Server configuration
    public static final int RPC_SERVER_PORT;
    public static final int ASYNC_TASK_POOL_SIZE;
    public static final String RPC_PUBLISHED_SERVER_HOST;
    public static final int RPC_PUBLISHED_SERVER_PORT;

    //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST;
    public static final int SERVICE_PORT;

    //Store configuration.
    //HostStore configuration.
    public static final int HOST_STORE_CONTAINER_COUNT;

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED;
    public static final String CLUSTER_NAME;
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL;
    public static final boolean AUTHORIZATION_ENABLED;
    public static final String USER_PASSWORD_FILE;
    public static final boolean TLS_ENABLED;
    public static final String TLS_KEY_FILE;
    public static final String TLS_CERT_FILE;
    public static final String TLS_TRUST_STORE;
    public static final String TOKEN_SIGNING_KEY;
    public static final boolean REPLY_WITH_STACK_TRACE_ON_ERROR;
    public static final boolean REQUEST_TRACING_ENABLED;

    //Zookeeper configuration.
    public static final String ZK_URL;
    public static final int ZK_RETRY_SLEEP_MS;
    public static final int ZK_MAX_RETRIES;
    public static final int ZK_SESSION_TIMEOUT_MS;
    public static final boolean SECURE_ZK;

    //REST server configuration
    public static final String REST_SERVER_IP;
    public static final int REST_SERVER_PORT;

    //Transaction configuration
    public static final long MIN_LEASE_VALUE;
    public static final long MAX_LEASE_VALUE;

    // Completed Transaction TTL
    public static final int COMPLETED_TRANSACTION_TTL_IN_HOURS;

    // Retention Configuration
    public static final int MINIMUM_RETENTION_FREQUENCY_IN_MINUTES;
    public static final int BUCKET_COUNT;
    public static final int RETENTION_THREAD_POOL_SIZE;

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME;

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP;

    public static final MetricsConfig METRICS_CONFIG;
    public static final GRPCServerConfig GRPC_SERVER_CONFIG;

    private static final String METRIC_PATH = "config.controller.metric";

    //endregion

    //region Property Definitions

    private static final Property<Integer> PROPERTY_CONTAINER_COUNT = Property.named("containerCount", 4);
    private static final Property<Boolean> PROPERTY_HOST_MONITORING_ENABLED = Property.named("hostMonitorEnabled", true);
    private static final Property<Integer> PROPERTY_MIN_REBALANCE_INTERVAL_SECONDS = Property.named("minRebalanceIntervalSeconds", 10);
    private static final Property<Boolean> PROPERTY_REPLY_WITH_STACK_TRACE_ON_ERROR = Property.named("replyWithStackTraceOnError", false);
    private static final Property<Boolean> PROPERTY_REQUEST_TRACING_ENABLED = Property.named("requestTracingEnabled", true);
    private static final Property<Integer> PROPERTY_SERVICE_PORT = Property.named("service.port", 9090);
    private static final Property<Integer> PROPERTY_TASK_POOL_SIZE = Property.named("service.asyncTaskPoolSize", 80);
    private static final Property<String> PROPERTY_SERVICE_HOST_IP = Property.named("service.hostIp", "localhost");
    private static final Property<Integer> PROPERTY_SERVICE_HOST_PORT = Property.named("service.hostPort", 12345);
    private static final Property<String> PROPERTY_RPC_HOST = Property.named("service.publishedRPCHost", "localhost");
    private static final Property<Integer> PROPERTY_RPC_PORT = Property.named("service.publishedRPCPort", 9090);
    private static final Property<String> PROPERTY_CLUSTER_NAME = Property.named("service.cluster", "pravega-cluster");
    private static final Property<String> PROPERTY_REST_IP = Property.named("service.restIp", "0.0.0.0");
    private static final Property<Integer> PROPERTY_REST_PORT = Property.named("service.restPort", 9091);
    private static final Property<Boolean> PROPERTY_AUTH_ENABLED = Property.named("auth.enabled", false);
    private static final Property<String> PROPERTY_AUTH_PASSWORD_FILE = Property.named("auth.userPasswordFile", "");
    private static final Property<Boolean> PROPERTY_TLS_ENABLED = Property.named("auth.tlsEnabled", false);
    private static final Property<String> PROPERTY_TLS_CERT_FILE = Property.named("auth.tlsCertFile", "");
    private static final Property<String> PROPERTY_TLS_TRUST_STORE = Property.named("auth.tlsTrustStore", "");
    private static final Property<String> PROPERTY_TLS_KEY_FILE = Property.named("auth.tlsKeyFile", "");
    private static final Property<String> PROPERTY_TOKEN_SIGNING_KEY = Property.named("auth.tokenSigningKey", "");
    private static final Property<String> PROPERTY_ZK_URL = Property.named("zk.url", "localhost:2121");
    private static final Property<Integer> PROPERTY_ZK_RETRY_MILLIS = Property.named("zk.retryIntervalMillis", 5000);
    private static final Property<Integer> PROPERTY_ZK_MAX_RETRY_COUNT = Property.named("maxRetries", 5);
    private static final Property<Integer> PROPERTY_ZK_SESSION_TIMEOUT_MILLIS = Property.named("sessionTimeoutMillis", 10000);
    private static final Property<Boolean> PROPERTY_ZK_SECURE_CONNECTION = Property.named("secureConnection", false);
    private static final Property<Integer> PROPERTY_RETENTION_FREQUENCY_MINUTES = Property.named("retention.frequencyMinutes", 30);
    private static final Property<Integer> PROPERTY_RETENTION_BUCKET_COUNT = Property.named("retention.bucketCount", 1);
    private static final Property<Integer> PROPERTY_RETENTION_THREAD_COUNT = Property.named("retention.threadCount", 1);
    private static final Property<Integer> PROPERTY_TXN_MIN_LEASE = Property.named("transaction.minLeaseValue", 10000);
    private static final Property<Integer> PROPERTY_TXN_MAX_LEASE = Property.named("transaction.maxLeaseValue", 120000);
    private static final Property<Integer> PROPERTY_TXN_TTL_HOURS = Property.named("transaction.ttlHours", 24);
    private static final Property<String> PROPERTY_SCALE_STREAM_NAME = Property.named("scale.streamName", "_requestStream");
    private static final Property<String> PROPERTY_SCALE_READER_GROUP = Property.named("scale.ReaderGroup", "scaleGroup");
    private static final String COMPONENT_CODE = "controller";

    //endregion

    //region Initialization

    static {
        val properties = loadConfiguration();
        val p = new TypedProperties(properties, COMPONENT_CODE);
        RPC_SERVER_PORT = p.getInt(PROPERTY_SERVICE_PORT);
        ASYNC_TASK_POOL_SIZE = p.getInt(PROPERTY_TASK_POOL_SIZE);
        RPC_PUBLISHED_SERVER_HOST = p.get(PROPERTY_RPC_HOST);
        RPC_PUBLISHED_SERVER_PORT = p.getInt(PROPERTY_RPC_PORT);
        SERVICE_HOST = p.get(PROPERTY_SERVICE_HOST_IP);
        SERVICE_PORT = p.getInt(PROPERTY_SERVICE_HOST_PORT);
        HOST_STORE_CONTAINER_COUNT = p.getInt(PROPERTY_CONTAINER_COUNT);
        HOST_MONITOR_ENABLED = p.getBoolean(PROPERTY_HOST_MONITORING_ENABLED);
        CLUSTER_NAME = p.get(PROPERTY_CLUSTER_NAME);
        CLUSTER_MIN_REBALANCE_INTERVAL = p.getInt(PROPERTY_MIN_REBALANCE_INTERVAL_SECONDS);
        AUTHORIZATION_ENABLED = p.getBoolean(PROPERTY_AUTH_ENABLED);
        USER_PASSWORD_FILE = p.get(PROPERTY_AUTH_PASSWORD_FILE);
        TLS_ENABLED = p.getBoolean(PROPERTY_TLS_ENABLED);
        TLS_KEY_FILE = p.get(PROPERTY_TLS_KEY_FILE);
        TLS_CERT_FILE = p.get(PROPERTY_TLS_CERT_FILE);
        TLS_TRUST_STORE = p.get(PROPERTY_TLS_TRUST_STORE);
        TOKEN_SIGNING_KEY = p.get(PROPERTY_TOKEN_SIGNING_KEY);
        REPLY_WITH_STACK_TRACE_ON_ERROR = p.getBoolean(PROPERTY_REPLY_WITH_STACK_TRACE_ON_ERROR);
        REQUEST_TRACING_ENABLED = p.getBoolean(PROPERTY_REQUEST_TRACING_ENABLED);
        ZK_URL = p.get(PROPERTY_ZK_URL);
        ZK_RETRY_SLEEP_MS = p.getInt(PROPERTY_ZK_RETRY_MILLIS);
        ZK_MAX_RETRIES = p.getInt(PROPERTY_ZK_MAX_RETRY_COUNT);
        ZK_SESSION_TIMEOUT_MS = p.getInt(PROPERTY_ZK_SESSION_TIMEOUT_MILLIS);
        SECURE_ZK = p.getBoolean(PROPERTY_ZK_SECURE_CONNECTION);
        REST_SERVER_IP = p.get(PROPERTY_REST_IP);
        REST_SERVER_PORT = p.getInt(PROPERTY_REST_PORT);
        MIN_LEASE_VALUE = p.getInt(PROPERTY_TXN_MIN_LEASE);
        MAX_LEASE_VALUE = p.getInt(PROPERTY_TXN_MAX_LEASE);
        COMPLETED_TRANSACTION_TTL_IN_HOURS = p.getInt(PROPERTY_TXN_TTL_HOURS);
        MINIMUM_RETENTION_FREQUENCY_IN_MINUTES = p.getInt(PROPERTY_RETENTION_FREQUENCY_MINUTES);
        BUCKET_COUNT = p.getInt(PROPERTY_RETENTION_BUCKET_COUNT);
        RETENTION_THREAD_POOL_SIZE = p.getInt(PROPERTY_RETENTION_THREAD_COUNT);
        SCALE_STREAM_NAME = p.get(PROPERTY_SCALE_STREAM_NAME);
        SCALE_READER_GROUP = p.get(PROPERTY_SCALE_READER_GROUP);
        GRPC_SERVER_CONFIG = createGrpcServerConfig();
        METRICS_CONFIG = createMetricsConfig(properties);
    }

    private static Properties loadConfiguration() {
        // Fetch configuration in a specific order (from lowest priority to highest), at each step resolving references
        // against already loaded config values..
        Properties properties = new Properties();
        properties.putAll(System.getenv());
        properties.putAll(System.getProperties());
        properties.putAll(loadFromFile());
        properties = resolveReferences(properties);

        log.info("Controller configuration:");
        properties.forEach((k, v) -> log.info("{} = {}", k, v));
        return properties;
    }

    @SneakyThrows(IOException.class)
    private static Properties loadFromFile() {
        Properties result = new Properties();

        String filePath = System.getProperty("conf.file", "controller.config.properties");
        File file = null;

        if (!Strings.isNullOrEmpty(filePath)) {
            file = new File(filePath);
            if (!file.exists()) {
                file = null;
            }
        }

        if (file == null) {
            ClassLoader classLoader = Config.class.getClassLoader();
            URL url = classLoader.getResource("controller.config.properties");
            if (url != null) {
                file = new File(url.getFile());
                if (!file.exists()) {
                    file = null;
                }
            }
        }

        if (file != null) {
            try (FileReader reader = new FileReader(file)) {
                result.load(reader);
            }
        }

        return result;
    }

    private static Properties resolveReferences(Properties properties) {
        // Any value that looks like ${REF} will need to be replaced by the value of REF in source.
        final String pattern = "^\\$\\{(.+)\\}$";
        val resolved = new Properties();
        for (val e : properties.entrySet()) {
            // Fetch reference value.
            String existingValue = e.getValue().toString();
            String newValue = existingValue; // Default to existing value (in case it's not a reference).
            if (existingValue.matches(pattern)) {
                // Only include the referred value if it resolves to anything; otherwise exclude this altogether.
                String lookupKey = pattern.replaceAll(pattern, "$1");
                newValue = (String) properties.getOrDefault(lookupKey, null);
            }

            if (newValue != null) {
                resolved.put(e.getKey().toString(), newValue);
            }
        }

        return resolved;
    }

    private static GRPCServerConfig createGrpcServerConfig() {
        String publishHost = Config.RPC_PUBLISHED_SERVER_HOST;
        if (publishHost != null && publishHost.equals("{null}")) {
            // This config is optional. "{null}" is used for testing purposes - we override all defaults to null.
            publishHost = null;
            log.info("publishedRPCHost is not configured, will use default value");
        }

        return GRPCServerConfigImpl.builder()
                .port(Config.RPC_SERVER_PORT)
                .publishedRPCHost(publishHost)
                .publishedRPCPort(Config.RPC_PUBLISHED_SERVER_PORT)
                .authorizationEnabled(Config.AUTHORIZATION_ENABLED)
                .userPasswordFile(Config.USER_PASSWORD_FILE)
                .tlsEnabled(Config.TLS_ENABLED)
                .tlsCertFile(Config.TLS_CERT_FILE)
                .tlsTrustStore(Config.TLS_TRUST_STORE)
                .tlsKeyFile(Config.TLS_KEY_FILE)
                .tokenSigningKey(Config.TOKEN_SIGNING_KEY)
                .replyWithStackTraceOnError(Config.REPLY_WITH_STACK_TRACE_ON_ERROR)
                .requestTracingEnabled(Config.REQUEST_TRACING_ENABLED)
                .build();
    }

    private static MetricsConfig createMetricsConfig(Properties p) {
        val builder = MetricsConfig.builder();
        for (val e : p.entrySet()) {
            String key = (String) e.getKey();
            if (key.startsWith(METRIC_PATH)) {
                builder.with(Property.named(key.replaceFirst(METRIC_PATH, "")), e.getValue());
            }
        }

        return builder.build();
    }

    //endregion
}
