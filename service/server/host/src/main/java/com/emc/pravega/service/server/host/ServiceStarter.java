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
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.ZKSegmentContainerManager;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogDataLogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * Starts the Pravega Service.
 */
public final class ServiceStarter {
    private static final Duration INITIALIZE_TIMEOUT = Duration.ofSeconds(30);
    private static final int ZK_RETRY_SLEEP_MS = 100;
    private static final int ZK_MAX_RETRY = 5;
    private final ServiceBuilderConfig serviceConfig;
    private final ServiceBuilder serviceBuilder;
    private PravegaConnectionListener listener;
    private boolean closed;

    private ServiceStarter(ServiceBuilderConfig config) {
        this.serviceConfig = config;
        this.serviceBuilder = createServiceBuilder(this.serviceConfig, true);
    }

    private ServiceBuilder createServiceBuilder(ServiceBuilderConfig config, boolean inMemory) {
        if (inMemory) {
            return ServiceBuilder.newInMemoryBuilder(config);
        } else {
            // Real (Distributed Log) Data Log with ZK based segment manager.
            return attachDistributedLog(attachedZKSegmentManager(ServiceBuilder.newInMemoryBuilder(config)));
        }
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
    static ServiceBuilder attachDistributedLog(ServiceBuilder builder) {
        return builder.withDataLogFactory(setup -> {
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

    /**
     * Attaches a Zookeeper based segment manager
     */
    static ServiceBuilder attachedZKSegmentManager(ServiceBuilder builder) {
        return builder.withContainerManager(setup -> {
            try {
                DistributedLogConfig dlConfig = setup.getConfig(DistributedLogConfig::new);
                ServiceConfig config = setup.getConfig(ServiceConfig::new);
                return (SegmentContainerManager) new ZKSegmentContainerManager(setup.getContainerRegistry(), setup.getSegmentToContainerMapper(),
                        getZKClient(dlConfig.getDistributedLogHost(), dlConfig.getDistributedLogPort()), new Host(config.getListeningIPAddress(), config.getListeningPort()));
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private static CuratorFramework getZKClient(String zkHost, int zkPort) {
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkHost + ":" + zkPort, new ExponentialBackoffRetry(
                ZK_RETRY_SLEEP_MS, ZK_MAX_RETRY));
        zkClient.start();
        return zkClient;
    }
}
