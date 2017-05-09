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

import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.common.util.Property;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
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

    //Zookeeper configuration.
    public static final String ZK_URL = CONFIG.getString("config.controller.server.zk.url");
    public static final int ZK_RETRY_SLEEP_MS = CONFIG.getInt("config.controller.server.zk.retryIntervalMS");
    public static final int ZK_MAX_RETRIES = CONFIG.getInt("config.controller.server.zk.maxRetries");

    static {
        Set<Map.Entry<String, ConfigValue>> entries = CONFIG.entrySet();
        entries.forEach(entry -> log.debug("{} = {}", entry.getKey(), entry.getValue()));
    }

    //REST server configuration
    public static final String REST_SERVER_IP = CONFIG.getString("config.controller.server.rest.serverIp");
    public static final int REST_SERVER_PORT = CONFIG.getInt("config.controller.server.rest.serverPort");

    //Transaction configuration
    public static final long MAX_LEASE_VALUE = CONFIG.getLong("config.controller.server.transaction.maxLeaseValue");
    public static final long MAX_SCALE_GRACE_PERIOD = CONFIG.getLong("config.controller.server.transaction.maxScaleGracePeriod");

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
