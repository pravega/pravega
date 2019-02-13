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
    private final static com.typesafe.config.Config CONFIG = ConfigFactory.defaultApplication()
                                                                          .withFallback(ConfigFactory.defaultOverrides().resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true)))
                                                                          .withFallback(ConfigFactory.systemEnvironment())
                                                                          .withFallback(ConfigFactory.defaultReference())
                                                                          .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));

    //RPC Server configuration
    public static final int RPC_SERVER_PORT = CONFIG.getInt("config.controller.server.port");
    public static final int ASYNC_TASK_POOL_SIZE = CONFIG.getInt("config.controller.server.asyncTaskPoolSize");
    public static final int RPC_PUBLISHED_SERVER_PORT = CONFIG.getInt("config.controller.server.publishedRPCPort");

    //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST = CONFIG.getString("config.controller.server.serviceHostIp");
    public static final int SERVICE_PORT = CONFIG.getInt("config.controller.server.serviceHostPort");

    //Store configuration.
    //HostStore configuration.
    public static final int HOST_STORE_CONTAINER_COUNT = CONFIG.getInt("config.controller.server.store.host.containerCount");

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED = CONFIG.getBoolean("config.controller.server.hostMonitorEnabled");
    public static final String CLUSTER_NAME = CONFIG.getString("config.controller.server.cluster");
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL = CONFIG.getInt("config.controller.server.minRebalanceInterval");
    private static final boolean AUTHORIZATION_ENABLED = CONFIG.getBoolean("config.controller.server.authorizationEnabled");
    private static final String USER_PASSWORD_FILE = CONFIG.getString("config.controller.server.userPasswordFile");
    private static final boolean TLS_ENABLED = CONFIG.getBoolean("config.controller.server.tlsEnabled");
    private static final String TLS_KEY_FILE = CONFIG.getString("config.controller.server.tlsKeyFile");
    private static final String TLS_CERT_FILE = CONFIG.getString("config.controller.server.tlsCertFile");
    private static final String TLS_TRUST_STORE = CONFIG.getString("config.controller.server.tlsTrustStore");
    private static final String TOKEN_SIGNING_KEY = CONFIG.getString("config.controller.server.tokenSigningKey");
    private static final boolean REPLY_WITH_STACK_TRACE_ON_ERROR = CONFIG.getBoolean("config.controller.server.replyWithStackTraceOnError");
    private static final boolean REQUEST_TRACING_ENABLED = CONFIG.getBoolean("config.controller.server.requestTracingEnabled");

    //Zookeeper configuration.
    public static final String ZK_URL = CONFIG.getString("config.controller.server.zk.url");
    public static final int ZK_RETRY_SLEEP_MS = CONFIG.getInt("config.controller.server.zk.retryIntervalMS");
    public static final int ZK_MAX_RETRIES = CONFIG.getInt("config.controller.server.zk.maxRetries");
    public static final int ZK_SESSION_TIMEOUT_MS = CONFIG.getInt("config.controller.server.zk.sessionTimeoutMS");
    public static final boolean SECURE_ZK = CONFIG.getBoolean("config.controller.server.zk.secureConnectionToZooKeeper");
    static {
        Set<Map.Entry<String, ConfigValue>> entries = CONFIG.entrySet();
        log.info("Controller configuration:");
        entries.forEach(entry -> log.info("{} = {}", entry.getKey(), entry.getValue()));
    }

    //REST server configuration
    public static final String REST_SERVER_IP = CONFIG.getString("config.controller.server.rest.serverIp");
    public static final int REST_SERVER_PORT = CONFIG.getInt("config.controller.server.rest.serverPort");

    //Transaction configuration
    public static final long MIN_LEASE_VALUE = CONFIG.getLong("config.controller.server.transaction.minLeaseValue");
    public static final long MAX_LEASE_VALUE = CONFIG.getLong("config.controller.server.transaction.maxLeaseValue");

    // Completed Transaction TTL
    public static final int COMPLETED_TRANSACTION_TTL_IN_HOURS = CONFIG.getInt("config.controller.server.transaction.completed.ttlInHours");
    public static final boolean DISABLE_COMPLETED_TXN_BACKWARD_COMPATIBILITY =
            CONFIG.getBoolean("config.controller.server.transaction.completed.disableBackwardCompatiblity");

    // Retention Configuration
    public static final int MINIMUM_RETENTION_FREQUENCY_IN_MINUTES = CONFIG.getInt("config.controller.server.retention.frequencyInMinutes");
    public static final int BUCKET_COUNT = CONFIG.getInt("config.controller.server.retention.bucketCount");
    public static final int RETENTION_THREAD_POOL_SIZE = CONFIG.getInt("config.controller.server.retention.threadCount");

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME = CONFIG.getString("config.controller.server.internal.scale.streamName");

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP = CONFIG.getString("config.controller.server.internal.scale.readerGroup.name");

    // Metrics
    private static final String METRIC_PATH = "config.controller.metric";

    public static GRPCServerConfig getGRPCServerConfig() {
        String publishHost = null;
        try {
            publishHost = CONFIG.getString("config.controller.server.publishedRPCHost");
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

    public static MetricsConfig getMetricsConfig() {
        val builder = MetricsConfig.builder();
        for (Map.Entry<String, ConfigValue> e : CONFIG.entrySet()) {
            if (e.getKey().startsWith(METRIC_PATH)) {
                builder.with(Property.named(e.getKey().replaceFirst(METRIC_PATH, "")), e.getValue().unwrapped());
            }
        }

        return builder.build();
    }
}
