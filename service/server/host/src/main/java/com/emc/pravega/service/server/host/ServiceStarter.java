/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.shared.metrics.MetricsConfig;
import com.emc.pravega.shared.metrics.MetricsProvider;
import com.emc.pravega.shared.metrics.StatsProvider;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.host.stat.AutoScalerConfig;
import com.emc.pravega.service.server.host.stat.SegmentStatsFactory;
import com.emc.pravega.service.server.host.stat.SegmentStatsRecorder;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogDataLogFactory;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Starts the Pravega Service.
 */
@Slf4j
public final class ServiceStarter {
    //region Members

    private final ServiceBuilderConfig builderConfig;
    private final ServiceConfig serviceConfig;
    private final ServiceBuilder serviceBuilder;
    private StatsProvider statsProvider;
    private PravegaConnectionListener listener;
    private SegmentStatsFactory segmentStatsFactory;
    private boolean closed;

    //endregion

    //region Constructor

    public ServiceStarter(ServiceBuilderConfig config, Options options) {
        this.builderConfig = config;
        this.serviceConfig = this.builderConfig.getConfig(ServiceConfig::builder);
        this.serviceBuilder = createServiceBuilder(options);
    }

    private ServiceBuilder createServiceBuilder(Options options) {
        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(this.builderConfig);
        if (options.distributedLog) {
            attachDistributedLog(builder);
        }

        if (options.rocksDb) {
            attachRocksDB(builder);
        }

        if (options.hdfs) {
            attachHDFS(builder);
        }

        if (options.zkSegmentManager) {
            attachZKSegmentManager(builder);
        }

        return builder;
    }

    //endregion

    //region Service Operation

    public void start() {
        Exceptions.checkNotClosed(this.closed, this);

        log.info("Initializing metrics provider ...");
        MetricsProvider.initialize(builderConfig.getConfig(MetricsConfig::builder));
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();

        log.info("Initializing Service Builder ...");
        this.serviceBuilder.initialize().join();

        log.info("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        log.info("Creating Segment Stats recorder ...");
        segmentStatsFactory = new SegmentStatsFactory();
        SegmentStatsRecorder statsRecorder = segmentStatsFactory
                .createSegmentStatsRecorder(service, builderConfig.getConfig(AutoScalerConfig::builder));

        this.listener = new PravegaConnectionListener(false, this.serviceConfig.getListeningIPAddress(),
                this.serviceConfig.getListeningPort(), service, statsRecorder);
        this.listener.startListening();
        log.info("PravegaConnectionListener started successfully.");
        log.info("StreamSegmentService started.");
    }

    public void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            log.info("StreamSegmentService shut down.");

            if (this.listener != null) {
                this.listener.close();
                log.info("PravegaConnectionListener closed.");
            }

            if (this.statsProvider != null) {
                statsProvider.close();
                statsProvider = null;
                log.info("Metrics statsProvider is now closed.");
            }

            if (this.segmentStatsFactory != null) {
                segmentStatsFactory.close();
            }

            this.closed = true;
        }
    }

    private void attachDistributedLog(ServiceBuilder builder) {
        builder.withDataLogFactory(setup -> {
            try {
                DistributedLogConfig dlConfig = setup.getConfig(DistributedLogConfig::builder);
                String clientId = String.format("%s-%s", this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getListeningPort());
                DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory(clientId, dlConfig, setup.getExecutor());
                factory.initialize();
                return factory;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private void attachRocksDB(ServiceBuilder builder) {
        builder.withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::builder)));
    }

    private void attachHDFS(ServiceBuilder builder) {
        builder.withStorageFactory(setup -> {
            try {
                HDFSStorageConfig hdfsConfig = setup.getConfig(HDFSStorageConfig::builder);
                return new HDFSStorageFactory(hdfsConfig, setup.getExecutor());
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private void attachZKSegmentManager(ServiceBuilder builder) {
        builder.withContainerManager(setup -> {
            CuratorFramework zkClient = createZKClient();
            return new ZKSegmentContainerManager(setup.getContainerRegistry(),
                    zkClient,
                    new Host(this.serviceConfig.getPublishedIPAddress(), this.serviceConfig.getPublishedPort(), null),
                    setup.getExecutor());
        });
    }

    private CuratorFramework createZKClient() {
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(this.serviceConfig.getZkURL())
                .namespace("pravega/" + this.serviceConfig.getClusterName())
                .retryPolicy(new ExponentialBackoffRetry(this.serviceConfig.getZkRetrySleepMs(), this.serviceConfig.getZkRetryCount()))
                .build();
        zkClient.start();
        return zkClient;
    }

    //endregion

    //region main()

    public static void main(String[] args) {
        AtomicReference<ServiceStarter> serviceStarter = new AtomicReference<>();
        try {
            // Load up the ServiceBuilderConfig, using this priority order:
            // 1. Configuration file
            // 2. System Properties overrides (these will be passed in via the command line or inherited from the JVM)
            ServiceBuilderConfig config = ServiceBuilderConfig
                    .builder()
                    .include("config.properties")
                    .include(System.getProperties())
                    .build();
            serviceStarter.set(new ServiceStarter(config, Options.builder().
                    distributedLog(true).hdfs(true).rocksDb(true).zkSegmentManager(true).build()));
        } catch (Throwable e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }

        try {
            serviceStarter.get().start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        log.info("Caught interrupt signal...");
                        serviceStarter.get().shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            log.info("Caught interrupt signal...");
        } finally {
            serviceStarter.get().shutdown();
        }
    }

    //endregion

    //region Options
    @Builder
    public static class Options {
        final boolean distributedLog;
        final boolean hdfs;
        final boolean rocksDb;
        final boolean zkSegmentManager;
    }

    //endregion
}
