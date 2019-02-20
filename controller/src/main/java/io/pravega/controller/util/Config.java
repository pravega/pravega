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

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import io.pravega.common.util.Property;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.shared.metrics.MetricsConfig;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * This is a utility used to read configuration. It can be configured to read custom configuration
 * files by setting the following system properties conf.file= < FILE PATH > or conf.resource=< Resource Name>. By default
 * it reads application.conf if no system property is set. Reference: {@link ConfigFactory#defaultApplication()}
 */
@Slf4j
public final class Config {
    /*
     * TODO
     * 1. Make all these properties instance members and generate getters
     * 2. Make a Config Builder that allows resolution of all the properties. TODO: where to put default values? In config only?
     * 3. Provide a static field DEFAULT with the result of the builder.
     * 4. Make sure configuration value sourcing is backwards compatible.
     * 5. Search through the code for random settings
     */
    private static final Property<Integer> PROPERTY_CONTAINER_COUNT = Property.named("containerCount");
    private static final Property<Boolean> PROPERTY_HOST_MONITORING_ENABLED = Property.named("hostMonitorEnabled");
    private static final Property<Integer> PROPERTY_MIN_REBALANCE_INTERVAL_SECONDS = Property.named("minRebalanceIntervalSeconds");
    private static final Property<Boolean> PROPERTY_REPLY_WITH_STACK_TRACE_ON_ERROR = Property.named("replyWithStackTraceOnError");
    private static final Property<Boolean> PROPERTY_REQUEST_TRACING_ENABLED = Property.named("requestTracingEnabled");

    private static final Property<Integer> PROPERTY_SERVICE_PORT = Property.named("service.port");
    private static final Property<Integer> PROPERTY_TASK_POOL_SIZE = Property.named("service.asyncTaskPoolSize");
    private static final Property<String> PROPERTY_SERVICE_HOST_IP_ = Property.named("service.hostIp");
    private static final Property<Integer> PROPERTY_SERVICE_HOST_PORT = Property.named("service.hostPort");
    private static final Property<String> PROPERTY_RPC_HOST = Property.named("service.publishedRPCHost");
    private static final Property<Integer> PROPERTY_RPC_PORT = Property.named("service.publishedRPCPort");
    private static final Property<Integer> PROPERTY_CLUSTER_NAME = Property.named("service.cluster");
    private static final Property<String> PROPERTY_REST_IP = Property.named("service.restIp");
    private static final Property<Integer> PROPERTY_REST_PORT = Property.named("service.restPort");

    private static final Property<Boolean> PROPERTY_AUTH_ENABLED = Property.named("auth.enabled");
    private static final Property<String> PROPERTY_AUTH_PASSWORD_FILE = Property.named("auth.userPasswordFile");
    private static final Property<Boolean> PROPERTY_TLS_ENABLED = Property.named("auth.tlsEnabled");
    private static final Property<String> PROPERTY_TLS_CERT_FILE = Property.named("auth.tlsCertFile");
    private static final Property<String> PROPERTY_TLS_TRUST_STORE = Property.named("auth.tlsTrustStore");
    private static final Property<String> PROPERTY_TLS_KEY_FILE = Property.named("auth.tlsKeyFile");
    private static final Property<String> PROPERTY_TOKEN_SIGNING_KEY = Property.named("auth.tokenSigningKey");

    private static final Property<String> PROPERTY_ZK_URL = Property.named("zk.url");
    private static final Property<Integer> PROPERTY_ZK_RETRY_MILLIS = Property.named("zk.retryIntervalMillis");
    private static final Property<Integer> PROPERTY_ZK_MAX_RETRY_COUNT = Property.named("maxRetries");
    private static final Property<Integer> PROPERTY_ZK_SESSION_TIMEOUT_MILLIS = Property.named("sessionTimeoutMillis");
    private static final Property<Boolean> PROPERTY_ZK_SECURE_CONNECTION = Property.named("secureConnection");

    private static final Property<Integer> PROPERTY_RETENTION_FREQUENCY_MINUTES = Property.named("retention.frequencyMinutes");
    private static final Property<Integer> PROPERTY_RETENTION_BUCKET_COUNT = Property.named("retention.bucketCount");
    private static final Property<Integer> PROPERTY_RETENTION_THREAD_COUNT = Property.named("retention.threadCount");

    private static final Property<Integer> PROPERTY_TXN_MIN_LEASE = Property.named("transaction.minLeaseValue");
    private static final Property<Integer> PROPERTY_TXN_MAX_LEASE = Property.named("transaction.maxLeaseValue");
    private static final Property<Integer> PROPERTY_TXN_TTL_HOURS = Property.named("transaction.ttlHours");

    private static final Property<String> PROPERTY_SCALE_STREAM_NAME = Property.named("scale.streamName");
    private static final Property<String> PROPERTY_SCALE_READER_GROUP = Property.named("scale.ReaderGroup");
    private static final String COMPONENT_CODE = "controller";


    //RPC Server configuration
    public static final int RPC_SERVER_PORT = LegacyConfig.RPC_SERVER_PORT;
    public static final int ASYNC_TASK_POOL_SIZE = LegacyConfig.ASYNC_TASK_POOL_SIZE;
    public static final int RPC_PUBLISHED_SERVER_PORT = LegacyConfig.RPC_PUBLISHED_SERVER_PORT;

    //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST = LegacyConfig.SERVICE_HOST;
    public static final int SERVICE_PORT = LegacyConfig.SERVICE_PORT;

    //Store configuration.
    //HostStore configuration.
    public static final int HOST_STORE_CONTAINER_COUNT = LegacyConfig.HOST_STORE_CONTAINER_COUNT;

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED = LegacyConfig.HOST_MONITOR_ENABLED;
    public static final String CLUSTER_NAME = LegacyConfig.CLUSTER_NAME;
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL = LegacyConfig.CLUSTER_MIN_REBALANCE_INTERVAL;
    private static final boolean AUTHORIZATION_ENABLED = LegacyConfig.AUTHORIZATION_ENABLED;
    private static final String USER_PASSWORD_FILE = LegacyConfig.USER_PASSWORD_FILE;
    private static final boolean TLS_ENABLED = LegacyConfig.TLS_ENABLED;
    private static final String TLS_KEY_FILE = LegacyConfig.TLS_KEY_FILE;
    private static final String TLS_CERT_FILE = LegacyConfig.TLS_CERT_FILE;
    private static final String TLS_TRUST_STORE = LegacyConfig.TLS_TRUST_STORE;
    private static final String TOKEN_SIGNING_KEY = LegacyConfig.TOKEN_SIGNING_KEY;
    private static final boolean REPLY_WITH_STACK_TRACE_ON_ERROR = LegacyConfig.REPLY_WITH_STACK_TRACE_ON_ERROR;
    private static final boolean REQUEST_TRACING_ENABLED = LegacyConfig.REQUEST_TRACING_ENABLED;

    //Zookeeper configuration.
    public static final String ZK_URL = LegacyConfig.ZK_URL;
    public static final int ZK_RETRY_SLEEP_MS = LegacyConfig.ZK_RETRY_SLEEP_MS;
    public static final int ZK_MAX_RETRIES = LegacyConfig.ZK_MAX_RETRIES;
    public static final int ZK_SESSION_TIMEOUT_MS = LegacyConfig.ZK_SESSION_TIMEOUT_MS;
    public static final boolean SECURE_ZK = LegacyConfig.SECURE_ZK;
    static {
        Set<Map.Entry<String, ConfigValue>> entries = LegacyConfig.CONFIG.entrySet();
        log.info("Controller legacy configuration:");
        entries.forEach(entry -> log.info("{} = {}", entry.getKey(), entry.getValue()));
    }

    //REST server configuration
    public static final String REST_SERVER_IP = LegacyConfig.REST_SERVER_IP;
    public static final int REST_SERVER_PORT = LegacyConfig.REST_SERVER_PORT;

    //Transaction configuration
    public static final long MIN_LEASE_VALUE = LegacyConfig.MIN_LEASE_VALUE;
    public static final long MAX_LEASE_VALUE = LegacyConfig.MAX_LEASE_VALUE;

    // Completed Transaction TTL
    public static final int COMPLETED_TRANSACTION_TTL_IN_HOURS = LegacyConfig.COMPLETED_TRANSACTION_TTL_IN_HOURS;

    // Retention Configuration
    public static final int MINIMUM_RETENTION_FREQUENCY_IN_MINUTES = LegacyConfig.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES;
    public static final int BUCKET_COUNT = LegacyConfig.BUCKET_COUNT;
    public static final int RETENTION_THREAD_POOL_SIZE = LegacyConfig.RETENTION_THREAD_POOL_SIZE;

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME = LegacyConfig.SCALE_STREAM_NAME;

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP = LegacyConfig.SCALE_READER_GROUP;

    private static final String METRIC_PATH = "config.controller.metric";

    public static MetricsConfig getMetricsConfig() {
        val builder = MetricsConfig.builder();
        for (Map.Entry<String, ConfigValue> e : LegacyConfig.CONFIG.entrySet()) {
            if (e.getKey().startsWith(METRIC_PATH)) {
                builder.with(Property.named(e.getKey().replaceFirst(METRIC_PATH, "")), e.getValue().unwrapped());
            }
        }

        return builder.build();
    }

    public static GRPCServerConfig getGRPCServerConfig() {
        String publishHost = null;
        try {
            publishHost = LegacyConfig.RPC_PUBLISHED_SERVER_HOST;
        } catch (ConfigException.NotResolved e) {
            // This config is optional so we can ignore this exception.
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

    private static class LegacyConfig {
        final static com.typesafe.config.Config CONFIG = ConfigFactory.defaultApplication()
                                                                              .withFallback(ConfigFactory.defaultOverrides().resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true)))
                                                                              .withFallback(ConfigFactory.systemEnvironment())
                                                                              .withFallback(ConfigFactory.defaultReference())
                                                                              .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));

        //RPC Server configuration
        static final int RPC_SERVER_PORT = CONFIG.getInt("config.controller.server.port");
        static final int ASYNC_TASK_POOL_SIZE = CONFIG.getInt("config.controller.server.asyncTaskPoolSize");
        static final String RPC_PUBLISHED_SERVER_HOST = CONFIG.getString("config.controller.server.publishedRPCHost");
        static final int RPC_PUBLISHED_SERVER_PORT = CONFIG.getInt("config.controller.server.publishedRPCPort");

        //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
        static final String SERVICE_HOST = CONFIG.getString("config.controller.server.serviceHostIp");
        static final int SERVICE_PORT = CONFIG.getInt("config.controller.server.serviceHostPort");

        //Store configuration.
        //HostStore configuration.
        static final int HOST_STORE_CONTAINER_COUNT = CONFIG.getInt("config.controller.server.store.host.containerCount");

        //Cluster configuration.
        static final boolean HOST_MONITOR_ENABLED = CONFIG.getBoolean("config.controller.server.hostMonitorEnabled");
        static final String CLUSTER_NAME = CONFIG.getString("config.controller.server.cluster");
        static final int CLUSTER_MIN_REBALANCE_INTERVAL = CONFIG.getInt("config.controller.server.minRebalanceInterval");
        static final boolean AUTHORIZATION_ENABLED = CONFIG.getBoolean("config.controller.server.authorizationEnabled");
        static final String USER_PASSWORD_FILE = CONFIG.getString("config.controller.server.userPasswordFile");
        static final boolean TLS_ENABLED = CONFIG.getBoolean("config.controller.server.tlsEnabled");
        static final String TLS_KEY_FILE = CONFIG.getString("config.controller.server.tlsKeyFile");
        static final String TLS_CERT_FILE = CONFIG.getString("config.controller.server.tlsCertFile");
        static final String TLS_TRUST_STORE = CONFIG.getString("config.controller.server.tlsTrustStore");
        static final String TOKEN_SIGNING_KEY = CONFIG.getString("config.controller.server.tokenSigningKey");
        static final boolean REPLY_WITH_STACK_TRACE_ON_ERROR = CONFIG.getBoolean("config.controller.server.replyWithStackTraceOnError");
        static final boolean REQUEST_TRACING_ENABLED = CONFIG.getBoolean("config.controller.server.requestTracingEnabled");

        //Zookeeper configuration.
        static final String ZK_URL = CONFIG.getString("config.controller.server.zk.url");
        static final int ZK_RETRY_SLEEP_MS = CONFIG.getInt("config.controller.server.zk.retryIntervalMS");
        static final int ZK_MAX_RETRIES = CONFIG.getInt("config.controller.server.zk.maxRetries");
        static final int ZK_SESSION_TIMEOUT_MS = CONFIG.getInt("config.controller.server.zk.sessionTimeoutMS");
        static final boolean SECURE_ZK = CONFIG.getBoolean("config.controller.server.zk.secureConnectionToZooKeeper");
        static {
            Set<Map.Entry<String, ConfigValue>> entries = CONFIG.entrySet();
            log.info("Controller legacy configuration:");
            entries.forEach(entry -> log.info("{} = {}", entry.getKey(), entry.getValue()));
        }

        //REST server configuration
        static final String REST_SERVER_IP = CONFIG.getString("config.controller.server.rest.serverIp");
        static final int REST_SERVER_PORT = CONFIG.getInt("config.controller.server.rest.serverPort");

        //Transaction configuration
        static final long MIN_LEASE_VALUE = CONFIG.getLong("config.controller.server.transaction.minLeaseValue");
        static final long MAX_LEASE_VALUE = CONFIG.getLong("config.controller.server.transaction.maxLeaseValue");

        // Completed Transaction TTL
        static final int COMPLETED_TRANSACTION_TTL_IN_HOURS = CONFIG.getInt("config.controller.server.transaction.completed.ttlInHours");

        // Retention Configuration
        static final int MINIMUM_RETENTION_FREQUENCY_IN_MINUTES = CONFIG.getInt("config.controller.server.retention.frequencyInMinutes");
        static final int BUCKET_COUNT = CONFIG.getInt("config.controller.server.retention.bucketCount");
        static final int RETENTION_THREAD_POOL_SIZE = CONFIG.getInt("config.controller.server.retention.threadCount");

        // Request Stream Configuration
        static final String SCALE_STREAM_NAME = CONFIG.getString("config.controller.server.internal.scale.streamName");

        // Request Stream readerGroup
        static final String SCALE_READER_GROUP = CONFIG.getString("config.controller.server.internal.scale.readerGroup.name");
    }
}
