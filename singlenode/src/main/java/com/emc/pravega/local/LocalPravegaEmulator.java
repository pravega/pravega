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

package com.emc.pravega.local;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.emc.pravega.service.server.host.ServiceStarter;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.LocalDLMEmulator;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.emc.pravega.controller.util.Config.*;

@Slf4j
public class LocalPravegaEmulator {


    private static LocalHDFSEmulator localHdfs;
    AtomicReference<ServiceStarter> nodeServiceStarter = new AtomicReference<>();

    private final int controllerPort;
    private final int hostPort;
    private ScheduledExecutorService controllerExecutor;

    public LocalPravegaEmulator(int controllerPort, int hostPort) {
        this.controllerPort = controllerPort;
        this.hostPort = hostPort;
    }

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println("Usage: LocalPravegaEmulator <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            final int zkPort = Integer.parseInt(args[0]);
            final int controllerPort = Integer.parseInt(args[1]);
            final int hostPort = Integer.parseInt(args[2]);

            final File zkDir = IOUtils.createTempDir("distrlog", "zookeeper");
            final LocalDLMEmulator localDlm = LocalDLMEmulator.newBuilder()
                    .zkPort(zkPort)
                    .numBookies(1)
                    .build();

             localHdfs = LocalHDFSEmulator.newBuilder()
                                                .baseDirName("temp")
                                                .build();

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.newBuilder()
                    .controllerPort(controllerPort)
                    .hostPort(hostPort)
                    .build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.teardown();
                        localDlm.teardown();
                        localHdfs.teardown();
                        FileUtils.deleteDirectory(zkDir);
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });

            localHdfs.start();
            localDlm.start();
            localPravega.start();

            System.out.println(String.format(
                    "Pravega Sandbox is running locally now. You could access it at %s:%d",
                    "127.0.0.1",
                    controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
            System.exit(1);
        }
    }

    /**
     * Stop controller and host.
     * */
    private void teardown() {
        localHdfs.teardown();
        controllerExecutor.shutdown();
        nodeServiceStarter.get().shutdown();
    }

    /**
     * Start controller and host.
     * */
    private void start() {
        startController();
        startPravegaHost();
    }

    private void startPravegaHost() {
        try {
            Properties p = new Properties();
            ServiceBuilderConfig props = ServiceBuilderConfig.getConfigFromFile();
            props.set(p,
                    HDFSStorageConfig.COMPONENT_CODE,
                    HDFSStorageConfig.PROPERTY_HDFS_URL,
                    String.format("hdfs://localhost:%d/", localHdfs.getNameNodePort()));

            // Change Number of containers and Thread Pool Size for each test.
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT, "2");
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_THREAD_POOL_SIZE, "20");

            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, "100");
            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, "100");
            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE, DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, "104857600");

            ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_TIME, Integer.toString(60 * 1000));
            ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_SIZE, Long.toString(128 * 1024 * 1024));

            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_LISTENING_PORT,
                    new Integer(hostPort).toString());

            props = new ServiceBuilderConfig(p);

            nodeServiceStarter.set(new ServiceStarter(props));
        } catch (Throwable e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }

        try {
            nodeServiceStarter.get().start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        log.info("Caught interrupt signal...");
                        nodeServiceStarter.get().shutdown();
                    } catch (Exception e) {
                        // do nothing
                    }
                }
            });
    } catch (Exception e) {
        }
    }

    private void startController() {
        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        //1. LOAD configuration.
        //Initialize the executor service.
        controllerExecutor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.Zookeeper);

        log.info("Creating the stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.Zookeeper, controllerExecutor );

        log.info("Creating zk based task store");
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor );

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(
                HostStoreFactory.StoreType.Zookeeper);

        //Host monitor is not required for a single node local setup.
        if (Config.HOST_MONITOR_ENABLED) {
            //Start the Segment Container Monitor.
            log.info("Starting the segment container monitor");
            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore,
                    ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient(), Config.CLUSTER_NAME,
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.startAsync();
        }

        //2. Start the RPC server.
        log.info("Starting RPC server");
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                controllerExecutor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, controllerExecutor, hostId);
        RPCServer.start(new ControllerServiceAsyncImpl(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks));

        //3. Hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure.
        // todo: hook up TaskSweeper.sweepOrphanedTasks with Failover support feature
        // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
        // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
        // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
        // are processed and deleted, that failed HostId is removed from FH folder.
        // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
        // controllers and starts sweeping tasks orphaned by those hostIds.
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks,
                streamTransactionMetadataTasks);
    }


    private static Builder newBuilder() {
        return new Builder();
    }


    private static class Builder {
        private int controllerPort;
        private int hostPort;

        public Builder controllerPort(int controllerPort) {
            this.controllerPort = controllerPort;
            return this;
        }

        public Builder hostPort(int hostPort) {
            this.hostPort = hostPort;
            return this;
        }

        public LocalPravegaEmulator build() {
            return new LocalPravegaEmulator(controllerPort, hostPort);
        }
    }
}
