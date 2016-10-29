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
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * Starts the Pravega Service.
 */
public final class ServiceStarter {
    private static final Duration INITIALIZE_TIMEOUT = Duration.ofSeconds(30);
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

        return builder;
    }

    private void start() {
        Exceptions.checkNotClosed(this.closed, this);

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);

        System.out.println("Initializing Container Manager ...");
        this.serviceBuilder.initialize(INITIALIZE_TIMEOUT).join();

        System.out.println("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        this.listener = new PravegaConnectionListener(false, this.serviceConfig.getConfig(ServiceConfig::new).getListeningPort(), service);
        this.listener.startListening();
        System.out.println("LogServiceConnectionListener started successfully.");
    }

    private void shutdown() {
        if (!this.closed) {
            this.serviceBuilder.close();
            System.out.println("StreamSegmentService is now closed.");

            this.listener.close();
            System.out.println("LogServiceConnectionListener is now closed.");
            this.closed = true;
        }
    }

    public static void main(String[] args) {
        ServiceStarter serviceStarter = new ServiceStarter(ServiceBuilderConfig.getDefaultConfig());
        try {
            serviceStarter.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        System.out.println("Caught interrupt signal...");
                        serviceStarter.shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            System.out.println("Caught interrupt signal");
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
                DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory("interactive-console", dlConfig);
                factory.initialize();
                return factory;
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    static void attachRocksDB(ServiceBuilder builder) {
        builder.withCacheFactory(setup -> {
            RocksDBCacheFactory factory = new RocksDBCacheFactory(setup.getConfig(RocksDBConfig::new));
            factory.initialize(true); // Always clear/reset the cache at startup.
            return factory;
        });
    }

    private static class Options {
        boolean distributedLog;
        boolean hdfs;
        boolean rocksDb;
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
}
