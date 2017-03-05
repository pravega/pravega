/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.StatsProvider;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
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
import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

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

    public ServiceStarter(ServiceBuilderConfig config) {
        this.builderConfig = config;
        this.serviceConfig = this.builderConfig.getConfig(ServiceConfig::new);
        Options opt = new Options();
        opt.distributedLog = true;
        opt.hdfs = true;
        opt.rocksDb = true;
        opt.zkSegmentManager = true;
        this.serviceBuilder = createServiceBuilder(opt);
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
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.start();

        log.info("Initializing Service Builder ...");
        this.serviceBuilder.initialize().join();

        log.info("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        segmentStatsFactory = new SegmentStatsFactory();
        SegmentStatsRecorder statsRecorder = segmentStatsFactory
                .createSegmentStatsRecorder(service,
                this.serviceConfig.getInternalScope(),
                this.serviceConfig.getInternalRequestStream(),
                URI.create(this.serviceConfig.getControllerUri()));

        this.listener = new PravegaConnectionListener(false, this.serviceConfig.getListeningPort(), service, statsRecorder);
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
                DistributedLogConfig dlConfig = setup.getConfig(DistributedLogConfig::new);
                String clientId = String.format("%s-%s", this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getListeningPort());
                DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory(clientId, dlConfig);
                factory.initialize();
                return factory;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private void attachRocksDB(ServiceBuilder builder) {
        builder.withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::new)));
    }

    private void attachHDFS(ServiceBuilder builder) {
        builder.withStorageFactory(setup -> {
            try {
                HDFSStorageConfig hdfsConfig = setup.getConfig(HDFSStorageConfig::new);
                HDFSStorageFactory factory = new HDFSStorageFactory(hdfsConfig);
                factory.initialize();
                return factory;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private void attachZKSegmentManager(ServiceBuilder builder) {
        builder.withContainerManager(setup -> {
            CuratorFramework zkClient = createZKClient();
            return new ZKSegmentContainerManager(setup.getContainerRegistry(),
                    setup.getSegmentToContainerMapper(),
                    zkClient,
                    new Host(this.serviceConfig.getListeningIPAddress(), this.serviceConfig.getListeningPort()));
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
            serviceStarter.set(new ServiceStarter(ServiceBuilderConfig.getConfigFromFile()));
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

    private static class Options {
        boolean distributedLog;
        boolean hdfs;
        boolean rocksDb;
        boolean zkSegmentManager;
    }

    //endregion
}
