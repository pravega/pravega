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
import com.emc.pravega.cluster.Cluster;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerType;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.LogServiceConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Starts the Pravega Service.
 */
public final class ServiceStarter {
    private static final Duration INITIALIZE_TIMEOUT = Duration.ofSeconds(30);
    private final ServiceBuilderConfig serviceConfig;
    private final ServiceBuilder serviceBuilder;
    private LogServiceConnectionListener listener;
    private boolean closed;
    private Cluster cluster;

    private ServiceStarter(ServiceBuilderConfig config) {
        this.serviceConfig = config;
        this.serviceBuilder = new DistributedLogServiceBuilder(this.serviceConfig);
        //this.serviceBuilder = new InMemoryServiceBuilder(this.serviceConfig);
    }

    private void start() {
        Exceptions.checkNotClosed(this.closed, this);

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);

        System.out.println("Initializing Container Manager ...");
        this.serviceBuilder.getContainerManager().initialize(INITIALIZE_TIMEOUT).join();

        System.out.println("Creating StreamSegmentService ...");
        StreamSegmentStore service = this.serviceBuilder.createStreamSegmentService();

        this.listener = new LogServiceConnectionListener(false, this.serviceConfig.getServiceConfig().getListeningPort(), service);
        this.listener.startListening();

        this.cluster = this.serviceBuilder.getCluster();

        try {
            cluster.initializeCluster(ConfigSyncManagerType.DUMMY, this.serviceConfig.getServiceConfig().getZKConnectString(),
                    this.serviceConfig.getServiceConfig().getZKClusterTimeout());
            cluster.registerPravegaNode(this.serviceConfig.getServiceConfig().getListeningIP(), this.serviceConfig.getServiceConfig().getListeningPort(), "");
        } catch (Exception e) {
            System.err.println("Zookeeper setup failed. Continuing with standalone existence.");
        }

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
}
