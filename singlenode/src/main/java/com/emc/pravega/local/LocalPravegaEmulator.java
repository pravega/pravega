/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.local;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.emc.pravega.service.server.host.ServiceStarter;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private static final int NUM_BOOKIES = 5;
    private static final String CONTAINER_COUNT = "2";
    private static final String THREADPOOL_SIZE = "20";

    private final AtomicReference<ServiceStarter> nodeServiceStarter = new AtomicReference<>();

    private final InProcPravegaCluster inProcPravegaCluster;

    @Builder
    private LocalPravegaEmulator(int zkPort, int controllerPort, int hostPort) {
        inProcPravegaCluster = InProcPravegaCluster
                .builder()
                .isInProcZK(true)
                .zkPort(zkPort)
                .isInProcHDFS(true)
                .isInProcDL(true)
                .initialBookiePort(5000)
                .isInprocController(true)
                .controllerCount(1)
                .controllerPorts(new int[] {controllerPort})
                .isInprocHost(true)
                .hostPorts(new int[] {hostPort})
                .build();
     /*   this.zkPort = zkPort;
        this.controllerPort = controllerPort;
        this.hostPort = hostPort;
        this.localHdfs = localHdfs;
        this.controllerExecutor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d")
                        .build());*/
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


            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder().controllerPort(
                    controllerPort).hostPort(hostPort).zkPort(zkPort).build();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                        log.warn("Exception running local pravega emulator: " + e.getMessage());
                    }
                }
            });

            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void configureDLBinding(int zkPort) {
        DistributedLogAdmin admin = new DistributedLogAdmin();
        String[] params = {"bind", "-dlzr", "localhost:" + zkPort, "-dlzw", "localhost:" + 7000, "-s", "localhost:" +
                zkPort, "-bkzr", "localhost:" + 7000, "-l", "/ledgers", "-i", "false", "-r", "true", "-c",
                "distributedlog://localhost:" + zkPort + "/pravega/segmentstore/containers"};
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
       inProcPravegaCluster.close();
    }

    /**
     * Start controller and host.
     */
    private void start() throws Exception {
        inProcPravegaCluster.start();
    }



}
