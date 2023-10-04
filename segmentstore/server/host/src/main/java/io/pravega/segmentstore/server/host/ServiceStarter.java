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
package io.pravega.segmentstore.server.host;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sun.management.HotSpotDiagnosticMXBean;
import io.netty.util.internal.PlatformDependent;
import io.pravega.common.Exceptions;
import io.pravega.common.security.JKSHelper;
import io.pravega.common.security.ZKTLSUtils;
import io.pravega.common.cluster.Host;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.CacheManager.CacheManagerHealthContributor;
import io.pravega.segmentstore.server.host.delegationtoken.TokenVerifierImpl;
import io.pravega.segmentstore.server.host.handler.AdminConnectionListener;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.health.ZKHealthContributor;
import io.pravega.shared.health.bindings.resources.HealthImpl;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.server.host.health.SegmentContainerRegistryHealthContributor;
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.rest.RESTServer;
import io.pravega.shared.rest.security.AuthHandlerManager;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Starts the Pravega Service.
 */
@Slf4j
public final class ServiceStarter {
    //region Members
    @VisibleForTesting
    @Getter
    private HealthServiceManager healthServiceManager;

    @VisibleForTesting
    @Getter
    private final ServiceBuilder serviceBuilder;

    private final ServiceBuilderConfig builderConfig;
    private final ServiceConfig serviceConfig;
    private StatsProvider statsProvider;
    private PravegaConnectionListener listener;
    private AdminConnectionListener adminListener;
    private AutoScaleMonitor autoScaleMonitor;
    private CuratorFramework zkClient;
    private RESTServer restServer;
    private boolean closed;
    private IndexAppendProcessor indexAppendProcessor;

    //endregion

    //region Constructor

    public ServiceStarter(ServiceBuilderConfig config) {
        this.builderConfig = config;
        this.serviceConfig = this.builderConfig.getConfig(ServiceConfig::builder);
        this.serviceBuilder = createServiceBuilder();
    }

    private ServiceBuilder createServiceBuilder() {
        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(this.builderConfig);
        attachDataLogFactory(builder);
        attachStorage(builder);
        attachZKSegmentManager(builder);
        return builder;
    }

    //endregion

    //region Service Operation

    public void start() throws Exception {
        Exceptions.checkNotClosed(this.closed, this);

        healthServiceManager = new HealthServiceManager(serviceConfig.getHealthCheckInterval());
        healthServiceManager.start();
        log.info("Initializing HealthService ...");

        MetricsConfig metricsConfig = builderConfig.getConfig(MetricsConfig::builder);
        if (metricsConfig.isEnableStatistics()) {
            log.info("Initializing metrics provider ...");
            MetricsProvider.initialize(metricsConfig);
            statsProvider = MetricsProvider.getMetricsProvider();
            statsProvider.start();
        }

        log.info("Initializing ZooKeeper Client ...");
        this.zkClient = createZKClient();

        log.info("Initializing Service Builder ...");
        this.serviceBuilder.initialize();

        log.info("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        log.info("Creating TableStoreService ...");
        TableStore tableStoreService = this.serviceBuilder.createTableStoreService();

        log.info("Creating Segment Stats recorder ...");
        autoScaleMonitor = new AutoScaleMonitor(service, builderConfig.getConfig(AutoScalerConfig::builder));

        AutoScalerConfig autoScalerConfig = builderConfig.getConfig(AutoScalerConfig::builder);
        TokenVerifierImpl tokenVerifier = null;
        if (autoScalerConfig.isAuthEnabled()) {
            tokenVerifier = new TokenVerifierImpl(autoScalerConfig.getTokenSigningKey());
        }

        // Log the configuration
        log.info(serviceConfig.toString());
        log.info(autoScalerConfig.toString());
        this.indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), service);

        this.listener = new PravegaConnectionListener(this.serviceConfig.isEnableTls(), this.serviceConfig.isEnableTlsReload(),
                                                      this.serviceConfig.getListeningIPAddress(),
                                                      this.serviceConfig.getListeningPort(), service, tableStoreService,
                                                      autoScaleMonitor.getStatsRecorder(), autoScaleMonitor.getTableSegmentStatsRecorder(),
                                                      tokenVerifier, this.serviceConfig.getCertFile(), this.serviceConfig.getKeyFile(),
                                                      this.serviceConfig.isReplyWithStackTraceOnError(), serviceBuilder.getLowPriorityExecutor(),
                                                      this.serviceConfig.getTlsProtocolVersion(), healthServiceManager, indexAppendProcessor);

        this.listener.startListening();
        log.info("PravegaConnectionListener started successfully.");

        if (serviceConfig.isEnableAdminGateway()) {
            this.adminListener = new AdminConnectionListener(this.serviceConfig.isEnableTls(), this.serviceConfig.isEnableTlsReload(),
                    this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getAdminGatewayPort(), service, tableStoreService,
                    tokenVerifier, this.serviceConfig.getCertFile(), this.serviceConfig.getKeyFile(), this.serviceConfig.getTlsProtocolVersion(),
                    healthServiceManager, indexAppendProcessor);
            this.adminListener.startListening();
            log.info("AdminConnectionListener started successfully.");
        }
        log.info("StreamSegmentService started.");

        healthServiceManager.register(new ZKHealthContributor(zkClient));
        healthServiceManager.register(new CacheManagerHealthContributor(serviceBuilder.getCacheManager()));
        healthServiceManager.register(new SegmentContainerRegistryHealthContributor(serviceBuilder.getSegmentContainerRegistry()));

        if (this.serviceConfig.isRestServerEnabled()) {
            log.info("Initializing RESTServer ...");
            List<Object> resources = new ArrayList<>();
            resources.add(new HealthImpl(new AuthHandlerManager(serviceConfig.getRestServerConfig()), healthServiceManager.getEndpoint()));

            MetricsProvider.getMetricsProvider().prometheusResource().ifPresent(resources::add);

            restServer = new RESTServer(serviceConfig.getRestServerConfig(), Set.copyOf(resources));
            restServer.startAsync();
            restServer.awaitRunning();
        }
    }

    public void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            log.info("StreamSegmentService shut down.");

            if (this.healthServiceManager != null) {
                this.healthServiceManager.close();
                log.info("HealthServiceManager closed.");
            }

            if (this.restServer != null) {
                this.restServer.stopAsync();
                this.restServer.awaitTerminated();
                log.info("RESTServer closed.");
            }
            
            if (this.indexAppendProcessor != null) {
                this.indexAppendProcessor.close();
                log.info("IndexAppendProcessor closed.");
            }

            if (this.listener != null) {
                this.listener.close();
                log.info("PravegaConnectionListener closed.");
            }

            if (this.adminListener != null) {
                this.adminListener.close();
                log.info("AdminConnectionListener closed.");
            }

            if (this.statsProvider != null) {
                statsProvider.close();
                statsProvider = null;
                log.info("Metrics statsProvider is now closed.");
            }

            if (this.zkClient != null) {
                this.zkClient.close();
                this.zkClient = null;
                log.info("ZooKeeper Client shut down.");
            }

            if (this.autoScaleMonitor != null) {
                autoScaleMonitor.close();
                autoScaleMonitor = null;
                log.info("AutoScaleMonitor shut down.");
            }

            if (this.serviceConfig.isSecureZK()) {
                ZKTLSUtils.unsetSecureZKClientProperties();
            }
            this.closed = true;
        }
    }

    private void attachDataLogFactory(ServiceBuilder builder) {
        builder.withDataLogFactory(setup -> {
            switch (this.serviceConfig.getDataLogTypeImplementation()) {
                case BOOKKEEPER:
                    return new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), this.zkClient, setup.getCoreExecutor());
                case INMEMORY:
                    return new InMemoryDurableDataLogFactory(setup.getCoreExecutor());
                default:
                    throw new IllegalStateException("Unsupported storage implementation: " + this.serviceConfig.getDataLogTypeImplementation());
            }
        });
    }

    private void attachStorage(ServiceBuilder builder) {
        builder.withStorageFactory(setup -> {
            StorageLoader loader = new StorageLoader();
            return loader.load(setup,
                    this.serviceConfig.getStorageImplementation(),
                    this.serviceConfig.getStorageLayout(),
                    setup.getStorageExecutor());
        });
    }

    @VisibleForTesting
    static void validateConfig(ServiceBuilderConfig config) {
        long xmx = Runtime.getRuntime().maxMemory();
        long nettyDirectMem = PlatformDependent.maxDirectMemory(); //Dio.netty.maxDirectMemory
        long maxDirectMemorySize = Long.parseLong(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                                                                   .getVMOption("MaxDirectMemorySize").getValue());
        maxDirectMemorySize = (maxDirectMemorySize == 0) ? xmx : maxDirectMemorySize;
        long cacheSize = config.getConfig(ServiceConfig::builder).getCachePolicy().getMaxSize();
        log.info("MaxDirectMemorySize is {}, Cache size is {} and Netty DM is {}", maxDirectMemorySize, cacheSize, nettyDirectMem);
        //run checks
        validateConfig(cacheSize, xmx, maxDirectMemorySize, ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getTotalPhysicalMemorySize());
    }

    @VisibleForTesting
    static void validateConfig(long cacheSize, long xmx, long maxDirectMem, long totalMem) {
        Preconditions.checkState(totalMem > (maxDirectMem + xmx), String.format("MaxDirectMemorySize(%s B) along " +
                "with JVM Xmx value(%s B) is greater than the available system memory!", maxDirectMem, xmx));
        Preconditions.checkState(maxDirectMem > cacheSize, String.format("Cache size (%s B) configured is more " +
                "than the JVM MaxDirectMemory(%s B) value", cacheSize, maxDirectMem));
    }

    private void attachZKSegmentManager(ServiceBuilder builder) {
        builder.withContainerManager(setup ->
                new ZKSegmentContainerManager(setup.getContainerRegistry(),
                        this.zkClient,
                        new Host(this.serviceConfig.getPublishedIPAddress(),
                                this.serviceConfig.getPublishedPort(), null),
                        this.serviceConfig.getParallelContainerStarts(),
                        setup.getCoreExecutor()));
    }

    @VisibleForTesting
    public CuratorFramework createZKClient() {
        if (this.serviceConfig.isSecureZK()) {
            ZKTLSUtils.setSecureZKClientProperties(this.serviceConfig.getZkTrustStore(),
                    JKSHelper.loadPasswordFrom(this.serviceConfig.getZkTrustStorePasswordPath()));
        }
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(this.serviceConfig.getZkURL())
                .namespace("pravega/" + this.serviceConfig.getClusterName())
                .zookeeperFactory(new ZKClientFactory())
                .retryPolicy(new ExponentialBackoffRetry(this.serviceConfig.getZkRetrySleepMs(), this.serviceConfig.getZkRetryCount()))
                .sessionTimeoutMs(this.serviceConfig.getZkSessionTimeoutMs())
                .build();
        zkClient.start();
        return zkClient;
    }

    /**
     * This custom factory is used to ensure that Zookeeper clients in Curator are always created using the Zookeeper
     * hostname, so it can be resolved to a new IP in the case of a Zookeeper instance restart.
     */
    @ThreadSafe
    static class ZKClientFactory implements ZookeeperFactory {
        @GuardedBy("this")
        private ZooKeeper client;
        @GuardedBy("this")
        private String connectString;
        @GuardedBy("this")
        private int sessionTimeout;
        @GuardedBy("this")
        private boolean canBeReadOnly;

        @Override
        public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly) throws Exception {
            Exceptions.checkNotNullOrEmpty(connectString, "connectString");
            Preconditions.checkArgument(sessionTimeout > 0, "sessionTimeout should be a positive integer");
            synchronized (this) {
                if (client == null) {
                    this.connectString = connectString;
                    this.sessionTimeout = sessionTimeout;
                    this.canBeReadOnly = canBeReadOnly;
                }
                log.info("Creating new Zookeeper client with arguments: {}, {}, {}.", this.connectString, this.sessionTimeout,
                        this.canBeReadOnly);
                this.client = new ZooKeeper(this.connectString, this.sessionTimeout, watcher, this.canBeReadOnly);
                return this.client;
            }
        }
    }

    //endregion

    //region main()

    public static void main(String[] args) throws Exception {
        AtomicReference<ServiceStarter> serviceStarter = new AtomicReference<>();
        try {
            System.err.println(System.getProperty(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, "config.properties"));
            // Load up the ServiceBuilderConfig, using this priority order (lowest to highest):
            // 1. Configuration file (either default or specified via SystemProperties)
            // 2. System Properties overrides (these will be passed in via the command line or inherited from the JVM)
            ServiceBuilderConfig config = ServiceBuilderConfig
                    .builder()
                    .include(System.getProperty(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, "config.properties"))
                    .include(System.getProperties())
                    .build();

            // For debugging purposes, it may be useful to know the non-default values for configurations being used.
            // This will unfortunately include all System Properties as well, but knowing those can be useful too sometimes.
            log.info("Segment store configuration:");
            config.forEach((key, value) -> log.info("{} = {}", key, value));
            validateConfig(config);
            serviceStarter.set(new ServiceStarter(config));
        } catch (Throwable e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }

        try {
            serviceStarter.get().start();
        } catch (Throwable e) {
            log.error("Could not start the Service, Aborting.", e);
            System.exit(1);
        }

        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Caught interrupt signal...");
                serviceStarter.get().shutdown();
            }));

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            log.info("Caught interrupt signal...");
        } finally {
            serviceStarter.get().shutdown();
            System.exit(0);
        }
    }

    //endregion
}
