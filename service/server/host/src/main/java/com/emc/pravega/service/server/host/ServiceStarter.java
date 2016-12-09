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

package com.emc.pravega.service.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogDataLogFactory;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletionException;

/**
 * Starts the Pravega Service.
 */
@Slf4j
public final class ServiceStarter {
    private final ServiceBuilderConfig serviceConfig;
    private final ServiceBuilder serviceBuilder;
    private PravegaConnectionListener listener;
    private boolean closed;

    private ServiceStarter(ServiceBuilderConfig config) {
        this.serviceConfig = config;
        Options opt = new Options();
        opt.distributedLog = false;
        opt.hdfs = false;
        opt.rocksDb = true;
        opt.zkSegmentManager = false;
        this.serviceBuilder = createServiceBuilder(this.serviceConfig, opt);
    }

    private ServiceBuilder createServiceBuilder(ServiceBuilderConfig config, Options options) {
        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(config);
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

    private void start() {
        Exceptions.checkNotClosed(this.closed, this);

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);

        log.info("Initializing Container Manager ...");
        this.serviceBuilder.initialize().join();

        log.info("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        this.listener = new PravegaConnectionListener(false, this.serviceConfig.getConfig(ServiceConfig::new)
                .getListeningPort(), service);
        this.listener.startListening();
        log.info("LogServiceConnectionListener started successfully.");
        log.info("Started.");
    }

    private void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            log.info("StreamSegmentService is now closed.");

            if (this.listener != null) {
                this.listener.close();
                log.info("LogServiceConnectionListener is now closed.");
            }

            log.info("Shutdown.");
            this.closed = true;
        }
    }

    public static void main(String[] args) {
        ServiceStarter serviceStarter = null;
        try {
            serviceStarter = new ServiceStarter(ServiceBuilderConfig.getConfigFromFile());
        } catch (IOException e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }

        try {
            serviceStarter.start();
            ServiceStarter finalServiceStarter = serviceStarter;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        log.info("Caught interrupt signal...");
                        finalServiceStarter.shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            log.info("Caught interrupt signal...");
        } finally {
            serviceStarter.shutdown();
        }
    }

    /**
     * Attaches a DistributedlogDataLogFactory to the given ServiceBuilder.
     */
    static void attachDistributedLog(ServiceBuilder builder) {
        builder.withDataLogFactory(setup -> {
            try {
                DistributedLogConfig dlConfig = setup.getConfig(DistributedLogConfig::new);
                DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory("interactive-console",
                        dlConfig);
                factory.initialize();
                return factory;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    static void attachRocksDB(ServiceBuilder builder) {
        builder.withCacheFactory(setup -> new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::new)));
    }

    private static class Options {
        boolean distributedLog;
        boolean hdfs;
        boolean rocksDb;
        boolean zkSegmentManager;
    }

    static ServiceBuilder attachHDFS(ServiceBuilder builder) {
        return builder.withStorageFactory(setup -> {
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

    /**
     * Attaches a Zookeeper based segment manager
     */
    static ServiceBuilder attachZKSegmentManager(ServiceBuilder builder) {
        return builder.withContainerManager(setup -> {
            ServiceConfig config = setup.getConfig(ServiceConfig::new);
            CuratorFramework zkClient = createZKClient(config);
            joinCluster(config, zkClient);
            return new ZKSegmentContainerManager(setup.getContainerRegistry(),
                    setup.getSegmentToContainerMapper(),
                    zkClient,
                    new Host(config.getListeningIPAddress(), config.getListeningPort()),
                    config.getClusterName());
        });
    }

    private static CuratorFramework createZKClient(ServiceConfig config) {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(config.getZkHostName() + ":" + config.getZkPort(),
                new ExponentialBackoffRetry(config.getZkRetrySleepMs(), config.getZkRetryCount()));
        zkClient.start();
        return zkClient;
    }

    private static void joinCluster(ServiceConfig config, CuratorFramework zkClient) {
        Cluster cluster = new ClusterZKImpl(zkClient, config.getClusterName());
        cluster.registerHost(new Host(config.getListeningIPAddress(), config.getListeningPort()));
    }
}
