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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.controller.server.Main;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.service.server.host.ServiceStarter;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private static final int NUM_BOOKIES = 5;
    private static final String CONTAINER_COUNT = "2";
    private static final String THREADPOOL_SIZE = "20";
    private static LocalHDFSEmulator localHdfs;
    private static int zkPort;
    private static LocalDLMEmulator localDlm;
    private final AtomicReference<ServiceStarter> nodeServiceStarter = new AtomicReference<>();

    private final int controllerPort;
    private final int hostPort;
    private ScheduledExecutorService controllerExecutor;

    private LocalPravegaEmulator(int controllerPort, int hostPort) {
        this.controllerPort = controllerPort;
        this.hostPort = hostPort;
    }

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println("Usage: LocalPravegaEmulator <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            zkPort = Integer.parseInt(args[0]);
            final int controllerPort = Integer.parseInt(args[1]);
            final int hostPort = Integer.parseInt(args[2]);

            final File zkDir = IOUtils.createTempDir("distrlog", "zookeeper");
            localDlm = LocalDLMEmulator.newBuilder().zkPort(zkPort).numBookies(NUM_BOOKIES).build();

            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.getLoggerList().get(0).setLevel(Level.OFF);

            localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.newBuilder().controllerPort(
                    controllerPort).hostPort(hostPort).build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        localDlm.teardown();
                        localHdfs.close();
                        FileUtils.deleteDirectory(zkDir);
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        log.warn("Caught an exception shutting down", e);
                    }
                }
            });

            localHdfs.start();
            localDlm.start();
            configureDLBinding();
            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
            System.exit(1);
        }
    }

    private static void configureDLBinding() {
        DistributedLogAdmin admin = new DistributedLogAdmin();
        String[] params = {"bind", "-dlzr", "localhost:" + zkPort, "-dlzw", "localhost:" + 7000, "-s", "localhost:" +
                zkPort, "-bkzr", "localhost:" + 7000, "-l", "/ledgers", "-i", "false", "-r", "true", "-c",
                "distributedlog://localhost:" + zkPort + "/messaging/distributedlog/mynamespace"};
        try {
            admin.run(params);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Stop controller and host.
     */
    @Override
    public void close() {
        localHdfs.close();
        controllerExecutor.shutdown();
        nodeServiceStarter.get().shutdown();
    }

    /**
     * Start controller and host.
     */
    private void start() {
        startController();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        startPravegaHost();
    }

    private void startPravegaHost() {
        try {
            Properties p = new Properties();
            ServiceBuilderConfig props = ServiceBuilderConfig.getConfigFromFile();
            props.set(p, HDFSStorageConfig.COMPONENT_CODE, HDFSStorageConfig.PROPERTY_HDFS_URL,
                    String.format("hdfs://localhost:%d/", localHdfs.getNameNodePort()));

            // Change Number of containers and Thread Pool Size for each test.
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTAINER_COUNT,
                    CONTAINER_COUNT);
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_THREAD_POOL_SIZE,
                    THREADPOOL_SIZE);

            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE,
                    DurableLogConfig.PROPERTY_CHECKPOINT_COMMIT_COUNT, "100");
            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE,
                    DurableLogConfig.PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, "100");
            ServiceBuilderConfig.set(p, DurableLogConfig.COMPONENT_CODE,
                    DurableLogConfig.PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, "104857600");

            ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_TIME,
                    Integer.toString(60 * 1000));
            ServiceBuilderConfig.set(p, ReadIndexConfig.COMPONENT_CODE, ReadIndexConfig.PROPERTY_CACHE_POLICY_MAX_SIZE,
                    Long.toString(128 * 1024 * 1024));

            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_ZK_HOSTNAME, "localhost");
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_ZK_PORT,
                    Integer.toString(zkPort));
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_LISTENING_PORT,
                    Integer.toString(hostPort));

            ServiceBuilderConfig.set(p, DistributedLogConfig.COMPONENT_CODE, DistributedLogConfig.PROPERTY_HOSTNAME,
                    "localhost");
            ServiceBuilderConfig.set(p, DistributedLogConfig.COMPONENT_CODE, DistributedLogConfig.PROPERTY_PORT,
                    Integer.toString(zkPort));

            props = new ServiceBuilderConfig(p);

            nodeServiceStarter.set(new ServiceStarter(props));
        } catch (Exception e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            System.exit(1);
        }
        nodeServiceStarter.get().start();
    }

    private void startController() {
        Config.setZKURL("tcp://localhost:" + zkPort);
        Main.main(null);
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
