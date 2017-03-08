/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServer;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.controller.util.Config;
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
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;

@Slf4j
public class LocalPravegaEmulator implements AutoCloseable {

    private static final int NUM_BOOKIES = 5;
    private static final String CONTAINER_COUNT = "2";
    private static final String THREADPOOL_SIZE = "20";

    private final AtomicReference<ServiceStarter> nodeServiceStarter = new AtomicReference<>();

    private final int zkPort;
    private final int controllerPort;
    private final int hostPort;
    private final LocalHDFSEmulator localHdfs;

    private final ScheduledExecutorService controllerExecutor;

    @Builder
    private LocalPravegaEmulator(int zkPort, int controllerPort, int hostPort, LocalHDFSEmulator localHdfs) {
        this.zkPort = zkPort;
        this.controllerPort = controllerPort;
        this.hostPort = hostPort;
        this.localHdfs = localHdfs;
        this.controllerExecutor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d")
                        .build());
    }

    public static void main(String[] args) {
        try {
            if (args.length < 4) {
                log.warn("Usage: LocalPravegaEmulator <run_only_bookkeeper> <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            boolean runOnlyBookkeeper = Boolean.parseBoolean(args[0]);
            int zkPort = Integer.parseInt(args[1]);

            if (runOnlyBookkeeper) {
                final LocalDLMEmulator localDlm = LocalDLMEmulator.newBuilder().zkPort(zkPort).numBookies(NUM_BOOKIES)
                        .build();
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        try {
                            localDlm.teardown();
                            log.info("Shutting down bookkeeper");
                        } catch (Exception e) {
                            // do nothing
                            log.warn("Exception shutting down local bookkeeper emulator: " + e.getMessage());
                        }
                    }
                });
                localDlm.start();
                log.info("Started Bookkeeper Emulator");
                return;
            }

            final int controllerPort = Integer.parseInt(args[2]);
            final int hostPort = Integer.parseInt(args[3]);

            final File zkDir = IOUtils.createTempDir("distrlog", "zookeeper");

            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.getLoggerList().get(0).setLevel(Level.OFF);

            LocalHDFSEmulator localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder().zkPort(zkPort).controllerPort(
                    controllerPort).hostPort(hostPort).localHdfs(localHdfs).build();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        localPravega.close();
                        localHdfs.close();
                        FileUtils.deleteDirectory(zkDir);
                        System.out.println("ByeBye!");
                    } catch (Exception e) {
                        // do nothing
                        log.warn("Exception running local pravega emulator: " + e.getMessage());
                    }
                }
            });

            localHdfs.start();
            configureDLBinding(zkPort);
            localPravega.start();

            System.out.println(
                    String.format("Pravega Sandbox is running locally now. You could access it at %s:%d", "127.0.0.1",
                            controllerPort));
        } catch (Exception ex) {
            System.out.println("Exception occurred running emulator " + ex);
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
            ServiceBuilderConfig.set(p, HDFSStorageConfig.COMPONENT_CODE, HDFSStorageConfig.PROPERTY_HDFS_URL,
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

            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_ZK_URL, "localhost:" +
                    zkPort);
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_LISTENING_PORT,
                    Integer.toString(hostPort));
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTROLLER_URI,
                                     "tcp://localhost:" + controllerPort);

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
        String hostId;
        try {
            // On each controller process restart, it gets a fresh hostId,
            // which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:" + zkPort, new RetryOneTime(2000));
        client.start();

        StoreClient storeClient = new ZKStoreClient(client);

        ZKStreamMetadataStore streamStore = new ZKStreamMetadataStore(client, controllerExecutor);

        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore("localhost", hostPort,
                                                                             Config.HOST_STORE_CONTAINER_COUNT);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor);

        SegmentHelper segmentHelper = new SegmentHelper();

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                                                                          segmentHelper, controllerExecutor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelper, controllerExecutor, hostId);

        TimeoutService timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks, 100000, 100000);

        ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelper, controllerExecutor);

        // Start the RPC server.
        log.info("Starting gRPC server");
        GRPCServerConfig gRPCServerConfig = GRPCServerConfig.builder()
                .port(controllerPort)
                .build();
        GRPCServer server = new GRPCServer(controllerService, gRPCServerConfig);
        server.startAsync();

        // After completion of startAsync method, server is expected to be in RUNNING state.
        // If it is not in running state, we return.
        if (!server.isRunning()) {
            log.error("RPC server failed to start, state = {} ", server.state());
            return;
        }

        // Hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure.
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
}
