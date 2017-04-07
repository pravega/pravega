/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.util;

import com.emc.pravega.metrics.MetricsConfig;
import com.emc.pravega.common.util.Property;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.Set;

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
            .resolve();

    //RPC Server configuration
    public static final int RPC_SERVER_PORT = CONFIG.getInt("controller.server.port");
    public static final int ASYNC_TASK_POOL_SIZE = CONFIG.getInt("controller.server.asyncTaskPoolSize");

    //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST = CONFIG.getString("controller.server.serviceHostIp");
    public static final int SERVICE_PORT = CONFIG.getInt("controller.server.serviceHostPort");

    //Store configuration.
    //Stream store configuration.
    public static final String STREAM_STORE_TYPE = CONFIG.getString("controller.server.store.stream.type");

    //HostStore configuration.
    public static final String HOST_STORE_TYPE = CONFIG.getString("controller.server.store.host.type");
    public static final int HOST_STORE_CONTAINER_COUNT = CONFIG.getInt("controller.server.store.host.containerCount");

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED = CONFIG.getBoolean("controller.server.hostMonitorEnabled");
    public static final String CLUSTER_NAME = CONFIG.getString("controller.server.cluster");
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL = CONFIG.getInt("controller.server.minRebalanceInterval");

    //Zookeeper configuration.
    public static final String ZK_URL = CONFIG.getString("zk.url");
    public static final int ZK_RETRY_SLEEP_MS = CONFIG.getInt("zk.retryIntervalMS");
    public static final int ZK_MAX_RETRIES = CONFIG.getInt("zk.maxRetries");

    //TaskStore configuration.
    public static final String STORE_TYPE = CONFIG.getString("controller.server.store.type");

    static {
        Set<Map.Entry<String, ConfigValue>> entries = CONFIG.entrySet();
        entries.forEach(entry -> log.debug("{} = {}", entry.getKey(), entry.getValue()));
    }

    //REST server configuration
    public static final String REST_SERVER_IP = CONFIG.getString("controller.server.rest.serverIp");
    public static final int REST_SERVER_PORT = CONFIG.getInt("controller.server.rest.serverPort");

    //Transaction configuration
    public static final long MAX_LEASE_VALUE = CONFIG.getLong("controller.server.transaction.maxLeaseValue");
    public static final long MAX_SCALE_GRACE_PERIOD = CONFIG.getLong("controller.server.transaction.maxScaleGracePeriod");

    public static final String INTERNAL_SCOPE = CONFIG.getString("controller.server.internal.scope");

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME = CONFIG.getString("controller.server.internal.scale.streamName");

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP = CONFIG.getString("controller.server.internal.scale.readerGroup.name");
    public static final String SCALE_READER_ID = CONFIG.getString("controller.server.internal.scale.readerGroup.readerId");

    // Metrics
    private static final String METRIC_PATH = "metrics";

    public static MetricsConfig getMetricsConfig() {
        val builder = MetricsConfig.builder();
        CONFIG.atPath(METRIC_PATH).entrySet()
                .forEach(e -> builder.with(Property.named(e.getKey()), e.getValue().unwrapped()));

        return builder.build();
    }
}
