/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
import io.pravega.shared.rest.RESTServerConfig;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Optional;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {

        StatsProvider statsProvider = null;
        try {
            //0. Initialize metrics provider
            MetricsProvider.initialize(Config.METRICS_CONFIG);
            statsProvider = MetricsProvider.getMetricsProvider();
            statsProvider.start();

            ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
                    .connectionString(Config.ZK_URL)
                    .secureConnectionToZooKeeper(Config.SECURE_ZK)
                    .trustStorePath(Config.ZK_TRUSTSTORE_FILE_PATH)
                    .trustStorePasswordPath(Config.ZK_TRUSTSTORE_PASSWORD_FILE_PATH)
                    .namespace("pravega/" + Config.CLUSTER_NAME)
                    .initialSleepInterval(Config.ZK_RETRY_SLEEP_MS)
                    .maxRetries(Config.ZK_MAX_RETRIES)
                    .sessionTimeoutMs(Config.ZK_SESSION_TIMEOUT_MS)
                    .build();

            StoreClientConfig storeClientConfig;
            if (Config.USE_PRAVEGA_TABLES) {
                storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);
            } else {
                storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);
            }

            HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                    .hostMonitorEnabled(Config.HOST_MONITOR_ENABLED)
                    .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                    .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                    .hostContainerMap(HostMonitorConfigImpl.getHostContainerMap(Config.SERVICE_HOST,
                            Config.SERVICE_PORT, Config.HOST_STORE_CONTAINER_COUNT))
                    .build();

            TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                    .maxLeaseValue(Config.MAX_LEASE_VALUE)
                    .build();

            ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfigImpl.withDefault();

            GRPCServerConfig grpcServerConfig = Config.GRPC_SERVER_CONFIG;

            RESTServerConfig restServerConfig = RESTServerConfigImpl.builder()
                    .host(Config.REST_SERVER_IP)
                    .port(Config.REST_SERVER_PORT)
                    .tlsEnabled(Config.TLS_ENABLED)
                    .tlsProtocolVersion(Config.TLS_PROTOCOL_VERSION.toArray(new String[Config.TLS_PROTOCOL_VERSION.size()]))
                    .keyFilePath(Config.REST_KEYSTORE_FILE_PATH)
                    .keyFilePasswordPath(Config.REST_KEYSTORE_PASSWORD_FILE_PATH)
                    .build();

            ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                    .threadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                    .storeClientConfig(storeClientConfig)
                    .hostMonitorConfig(hostMonitorConfig)
                    .controllerClusterListenerEnabled(true)
                    .timeoutServiceConfig(timeoutServiceConfig)
                    .eventProcessorConfig(Optional.of(eventProcessorConfig))
                    .grpcServerConfig(Optional.of(grpcServerConfig))
                    .restServerConfig(Optional.of(restServerConfig))
                    .tlsEnabledForSegmentStore(Config.TLS_ENABLED_FOR_SEGMENT_STORE)
                    .minBucketRedistributionIntervalInSeconds(Config.MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS)
                    .build();

            setUncaughtExceptionHandler(Main::logUncaughtException);

            ControllerServiceMain controllerServiceMain = new ControllerServiceMain(serviceConfig);
            controllerServiceMain.startAsync();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                onShutdown(controllerServiceMain);
            }));
            
            controllerServiceMain.awaitTerminated();

            log.info("Controller service exited");
            System.exit(0);
        } catch (Throwable e) {
            log.error("Controller service failed", e);
            System.exit(-1);
        } finally {
            if (statsProvider != null) {
                statsProvider.close();
            }
        }
    }

    @VisibleForTesting
    static void setUncaughtExceptionHandler(BiConsumer<Thread, Throwable> exceptionConsumer) {
        Thread.setDefaultUncaughtExceptionHandler(exceptionConsumer::accept);
    }

    static void logUncaughtException(Thread t, Throwable e) {
        log.error("Thread {} with stackTrace {} failed with uncaught exception", t.getName(), t.getStackTrace(), e);
    }

    @VisibleForTesting
    static void onShutdown(ControllerServiceMain controllerServiceMain) {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        boolean previousVerbose = memoryMXBean.isVerbose();
        memoryMXBean.setVerbose(true);
        try {
            log.info("Shutdown hook memory usage dump: Heap memory usage: {}, non heap memory usage {}", memoryMXBean.getHeapMemoryUsage(),
                    memoryMXBean.getNonHeapMemoryUsage());
        } finally {
            memoryMXBean.setVerbose(previousVerbose);
        }

        log.info("Controller service shutting down");
        try {
            controllerServiceMain.stopAsync();
            controllerServiceMain.awaitTerminated();
        } finally {
            if (Config.DUMP_STACK_ON_SHUTDOWN) {
                Thread.getAllStackTraces().forEach((key, value) ->
                        log.info("Shutdown Hook Thread dump: Thread {} stackTrace: {} ", key.getName(), value));
            }
        }
    }
}
