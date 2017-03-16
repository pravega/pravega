/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.local;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.server.ControllerServiceConfig;
import com.emc.pravega.controller.server.ControllerServiceStarter;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import com.emc.pravega.controller.server.rest.RESTServerConfig;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.emc.pravega.controller.store.client.StoreClientConfig;
import com.emc.pravega.controller.store.client.ZKClientConfig;
import com.emc.pravega.controller.store.host.HostMonitorConfig;
import com.emc.pravega.controller.timeout.TimeoutServiceConfig;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.service.server.host.ServiceStarter;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
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
            if (args.length < 3) {
                System.out.println("Usage: LocalPravegaEmulator <zk_port> <controller_port> <host_port>");
                System.exit(-1);
            }

            int zkPort = Integer.parseInt(args[0]);
            final int controllerPort = Integer.parseInt(args[1]);
            final int hostPort = Integer.parseInt(args[2]);

            final File zkDir = IOUtils.createTempDir("distrlog", "zookeeper");
            LocalDLMEmulator localDlm = LocalDLMEmulator.newBuilder().zkPort(zkPort).numBookies(NUM_BOOKIES).build();

            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            context.getLoggerList().get(0).setLevel(Level.OFF);

            LocalHDFSEmulator localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();

            final LocalPravegaEmulator localPravega = LocalPravegaEmulator.builder().controllerPort(
                    controllerPort).hostPort(hostPort).localHdfs(localHdfs).build();

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
                        // do nothing
                        log.warn("Exception running local pravega emulator: " + e.getMessage());
                    }
                }
            });

            localHdfs.start();
            localDlm.start();
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
            //On each controller report restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        ZKClientConfig zkClientConfig = ZKClientConfig.builder()
                .connectionString(Config.ZK_URL)
                .namespace("pravega/" + Config.CLUSTER_NAME)
                .initialSleepInterval(Config.ZK_RETRY_SLEEP_MS)
                .maxRetries(Config.ZK_MAX_RETRIES)
                .build();

        StoreClientConfig storeClientConfig = StoreClientConfig.withZKClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfig.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfig.builder()
                .scopeName("system")
                .commitStreamName("commitStream")
                .abortStreamName("abortStream")
                .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                .commitReaderGroupName("commitStreamReaders")
                .commitReaderGroupSize(1)
                .abortReaderGrouopName("abortStreamReaders")
                .abortReaderGroupSize(1)
                .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .build();

        GRPCServerConfig grpcServerConfig = GRPCServerConfig.builder().port(controllerPort).build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfig.builder()
                .host(hostId)
                .serviceThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .taskThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .eventProcThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .requestHandlerThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .requestHandlersEnabled(true)
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.<RESTServerConfig>empty())
                .build();

        ControllerServiceStarter controllerServiceStarter = new ControllerServiceStarter(serviceConfig);
        controllerServiceStarter.startAsync();
    }

}
