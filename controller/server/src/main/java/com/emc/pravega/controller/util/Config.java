/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.util;

import com.emc.pravega.common.metrics.MetricsConfig;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
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
    public static final int SERVER_PORT = CONFIG.getInt("config.controller.server.port");
    public static final int SERVER_SELECTOR_THREAD_COUNT = CONFIG.getInt("config.controller.server.selectorThreadCount");
    public static final int SERVER_WORKER_THREAD_COUNT = CONFIG.getInt("config.controller.server.workerThreadCount");
    public static final int SERVER_MAX_READ_BUFFER_BYTES = CONFIG.getInt("config.controller.server.maxReadBufferBytes");
    public static final int ASYNC_TASK_POOL_SIZE = CONFIG.getInt("config.controller.server.asyncTaskPoolSize");

    //Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST = CONFIG.getString("config.controller.server.serviceHostIp");
    public static final int SERVICE_PORT = CONFIG.getInt("config.controller.server.serviceHostPort");

    //Store configuration.
    //Stream store configuration.
    public static final String STREAM_STORE_TYPE = CONFIG.getString("config.controller.server.store.stream.type");

    //HostStore configuration.
    public static final String HOST_STORE_TYPE = CONFIG.getString("config.controller.server.store.host.type");
    public static final int HOST_STORE_CONTAINER_COUNT = CONFIG.getInt("config.controller.server.store.host.containerCount");

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED = CONFIG.getBoolean("config.controller.server.hostMonitorEnabled");
    public static final String CLUSTER_NAME = CONFIG.getString("config.controller.server.cluster");
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL = CONFIG.getInt("config.controller.server.minRebalanceInterval");

    //Zookeeper configuration.
    static String zKURL = CONFIG.getString("config.controller.server.zk.url");
    public static final int ZK_RETRY_SLEEP_MS = CONFIG.getInt("config.controller.server.zk.retryIntervalMS");
    public static final int ZK_MAX_RETRIES = CONFIG.getInt("config.controller.server.zk.maxRetries");

    //TaskStore configuration.
    public static final String STORE_TYPE = CONFIG.getString("config.controller.server.store.type");

    static {
        Set<Map.Entry<String, ConfigValue>> entries = CONFIG.entrySet();
        entries.forEach(entry -> log.debug("{} = {}", entry.getKey(), entry.getValue()));
    }

    //REST server configuration
    public static final String REST_SERVER_IP = CONFIG.getString("config.controller.server.rest.serverIp");
    public static final int REST_SERVER_PORT = CONFIG.getInt("config.controller.server.rest.serverPort");

    // Transaction related configuration
    public static final long TXN_TIMEOUT_IN_SECONDS = Duration.ofSeconds(CONFIG.getLong("config.controller.server.transactions.timeoutInSeconds")).toMillis();

    public static final String INTERNAL_SCOPE = CONFIG.getString("config.controller.server.internal.scope");

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME = CONFIG.getString("config.controller.server.internal.scale.streamName");

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP = CONFIG.getString("config.controller.server.internal.scale.readerGroup.name");
    public static final String SCALE_READER_ID = CONFIG.getString("config.controller.server.internal.scale.readerGroup.readerId");

    public static void setZKURL(String replaceZKURL) {
        zKURL = replaceZKURL;
    }

    public static MetricsConfig getMetricsConfig() {
        MetricsConfig metricsConfig = new MetricsConfig(new Properties());
        metricsConfig.refresh();
        return metricsConfig;
    }
}
