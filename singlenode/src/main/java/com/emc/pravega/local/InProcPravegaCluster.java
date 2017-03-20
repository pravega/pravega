/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.local;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.server.rest.RESTServer;
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
import com.emc.pravega.controller.util.ZKUtils;
import com.emc.pravega.service.server.host.ServiceStarter;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;
import static org.apache.bookkeeper.util.LocalBookKeeper.runZookeeper;

@Slf4j
public class InProcPravegaCluster implements AutoCloseable {

    private static final String THREADPOOL_SIZE = "20";
    private static final String CONTAINER_COUNT = "4";
    private final boolean isInMemStorage;

    /*Controller related variables*/
    private boolean isInprocController;
    private int controllerCount;
    private int[] controllerPorts;

    @Getter
    private String controllerURI;
    private ControllerService[] controllerServices = null;

    /*SSS related variables*/
    private boolean isInprocSSS;
    private int sssCount = 0;
    private int[] sssPorts;

    /*Distributed log related variables*/
    private boolean isInProcDL;
    private int bookieCount;
    private int initialBookiePort;


    /*ZK related variables*/
    private boolean isInProcZK;
    private int zkPort;
    private String zkHost;

    /*HDFS related variables*/
    private boolean isInProcHDFS;
    private String hdfsUrl;


    /* SSS configuration*/
    private String containerCount = "2";
    private ServiceStarter[] nodeServiceStarter = new ServiceStarter[sssCount];

    private LocalHDFSEmulator localHdfs;
    private LocalDLMEmulator localDlm;
    private ScheduledExecutorService[] controllerExecutors;
    @GuardedBy("$lock")
    private GRPCServer[] controllerServers;

    @Getter
    private String zkUrl;
    private boolean startRestServer = false;

    @Builder
    public InProcPravegaCluster(boolean isInProcZK, String zkUrl, int zkPort, boolean isInMemStorage,
                boolean isInProcHDFS, boolean isInProcDL, int initialBookiePort,
                boolean isInprocController, int controllerCount,
                boolean isInprocSSS, int sssCount, String containerCount, boolean startRestServer) {

        //Check for valid combinations of flags
        //For ZK
        Preconditions.checkState(isInProcZK || zkUrl != null, "ZkUrl must be specified");

        //For controller
        Preconditions.checkState( isInprocController || controllerURI != null,
                "ControllerURI should be defined for external controller");
        Preconditions.checkState(isInprocController || this.controllerPorts != null,
                "Controller ports not present");

        //For SSS
        Preconditions.checkState(  isInprocSSS || this.sssPorts != null, "SSS ports not declared");

        this.isInMemStorage = isInMemStorage;
        if ( isInMemStorage ) {
            this.isInProcHDFS = false;
            this.isInProcDL = false;
        } else {
            this.isInProcHDFS = isInProcHDFS;
            this.isInProcDL = isInProcDL;
        }
        this.isInProcZK = isInProcZK;
        this.zkUrl = zkUrl;
        this.zkPort = zkPort;
        this.initialBookiePort = initialBookiePort;
        this.isInprocController = isInprocController;
        this.controllerCount = controllerCount;
        this.isInprocSSS = isInprocSSS;
        this.sssCount = sssCount;
        this.containerCount = containerCount;
        this.startRestServer = startRestServer;
        checkAvailableFeatures();
    }

    @Synchronized
    public void setControllerPorts(int[] controllerPorts) {
        this.controllerPorts = Arrays.copyOf( controllerPorts, controllerPorts.length);
    }

    @Synchronized
    public void setSssPorts(int[] sssPorts) {
        this.sssPorts = Arrays.copyOf(sssPorts, sssPorts.length);

    }

    /**
     * Kicks off the cluster creation. right now it can be done only once in lifetime of a process.
     * @throws Exception Exception thrown by ZK/HDFS etc.
     */
    @Synchronized
    public void start() throws Exception {
        /*Check possible combinations of flags*/

        /*Start the ZK*/
        if (isInProcZK) {
            zkUrl = "localhost:" + zkPort;
            startLocalZK();
        } else {
            URI zkUri = new URI("temp://" + zkUrl);
            zkHost = zkUri.getHost();
            zkPort = zkUri.getPort();
        }

        if (isInProcHDFS) {
            startLocalHDFS();
            hdfsUrl = String.format("hdfs://localhost:%d/", localHdfs.getNameNodePort());
        }

        cleanUpZK();

        if (isInProcDL) {
            startLocalDL();
        }
        configureDLBinding(this.zkUrl);

        if (isInprocController) {
            controllerServices = new ControllerService[controllerCount];
            startLocalControllers();
        }

        if (isInprocSSS) {
            nodeServiceStarter = new ServiceStarter[sssCount];
            startLocalSSSs();
        }

    }

    private void checkAvailableFeatures() {
        if ( isInProcDL ) {
           //Can not instantiate DL in proc till DL-192 is fixed
           throw new NotImplementedException();
        }
    }

    private void startLocalZK() throws IOException {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "test");
        runZookeeper(1000, zkPort, zkTmpDir);
    }


    private void cleanUpZK() {
        String[] pathsTobeCleaned = {"/pravega", "/hostIndex", "/store", "/taskIndex"};

        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString(zkUrl)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(rp);
        @Cleanup
        CuratorFramework zclient = builder.build();
        zclient.start();
        for ( String path : pathsTobeCleaned ) {
            try {
                zclient.delete().guaranteed().deletingChildrenIfNeeded()
                        .forPath(path);
            } catch (Exception e) {
                log.warn("Not able to delete path {} . Exception {}", path, e.getMessage());
            }
        }
        zclient.close();
    }

    private void configureDLBinding(String zkUrl) {
        DistributedLogAdmin admin = new DistributedLogAdmin();
        String[] params = {"bind", "-dlzr", zkUrl, "-dlzw", zkUrl, "-s", zkUrl, "-bkzr", zkUrl,
                "-l", "/ledgers", "-i", "false", "-r", "true", "-c",
                "distributedlog://" + zkUrl + "/pravega/segmentstore/containers"};
        try {
            admin.run(params);
        } catch (Exception e) {
            log.warn("Exception {} while configuring the DL bindings.", e.getMessage());
        }
    }

    @Synchronized
    private void startLocalDL() throws Exception {
        localDlm = LocalDLMEmulator.newBuilder().shouldStartZK(false).zkHost(zkHost).
                    zkPort(zkPort).numBookies(this.bookieCount).build();
        localDlm.start();
    }


    private void startLocalHDFS() throws IOException {
        localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();
        localHdfs.start();
    }

    private void startLocalSSSs() {
        for (int i = 0; i < this.sssCount; i++) {
            startLocalSSS(i);
        }

    }

    /**
     * Starts a SSS with a SSS id. This is re-entrant. Eventually this will allow starting and stopping of
     * individual SSS instances. This is not possible right now.
     *
     * @param sssId id of the SSS.
     */
    @Synchronized
    private void startLocalSSS(int sssId) {

        try {
            Properties p = new Properties();

            if ( !isInMemStorage ) {
                ServiceBuilderConfig.set(p, HDFSStorageConfig.COMPONENT_CODE, HDFSStorageConfig.PROPERTY_HDFS_URL,
                        String.format("hdfs://localhost:%d/", localHdfs.getNameNodePort()));
                ServiceBuilderConfig.set(p, DistributedLogConfig.COMPONENT_CODE, DistributedLogConfig.PROPERTY_HOSTNAME,
                        "localhost");
                ServiceBuilderConfig.set(p, DistributedLogConfig.COMPONENT_CODE, DistributedLogConfig.PROPERTY_PORT,
                        Integer.toString(zkPort));
            }

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
                    Integer.toString(sssPorts[sssId]));
            ServiceBuilderConfig.set(p, ServiceConfig.COMPONENT_CODE, ServiceConfig.PROPERTY_CONTROLLER_URI,
                    "tcp://localhost:" + controllerPorts[0]);

            ServiceBuilderConfig props = new ServiceBuilderConfig(p);

            ServiceStarter.Options.OptionsBuilder optBuilder = ServiceStarter.Options.builder().rocksDb(true)
                    .zkSegmentManager(true);

            nodeServiceStarter[sssId] = new ServiceStarter(props, isInMemStorage? optBuilder.hdfs(false)
                    .distributedLog(false).build(): optBuilder.hdfs(true).distributedLog(true).build());
        } catch (Exception e) {
            log.error("Could not create a Service with default config, Aborting.", e);
            throw e;
        }
        nodeServiceStarter[sssId].start();
    }

    private void startLocalControllers() {
        controllerServers = new GRPCServer[this.controllerCount];
        controllerExecutors = new ScheduledExecutorService[this.controllerCount];

        for (int i = 0; i < this.controllerCount; i++) {
            startLocalController(i);
        }
        controllerURI = "localhost:" + controllerPorts[0];

    }

    @Synchronized
    private void startLocalController(int controllerId) {

        ScheduledExecutorService controllerExecutor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());
        if ( this.controllerServers[controllerId] == null || !this.controllerServers[controllerId].isRunning()) {

            String hostId;
            try {
                //On each controller report restart, it gets a fresh hostId,
                //which is a combination of hostname and random GUID.
                hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
            } catch (UnknownHostException e) {
                log.debug("Failed to get host address.", e);
                hostId = UUID.randomUUID().toString();
            }

            //1. LOAD configuration.
            CuratorFramework client = CuratorFrameworkFactory.newClient(zkUrl, new RetryOneTime(2000));
            client.start();

            StoreClient storeClient = new ZKStoreClient(client);

            ZKStreamMetadataStore streamStore = new ZKStreamMetadataStore(client, controllerExecutor);

            HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.Zookeeper);

            TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor);

            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore, ZKUtils.getCuratorClient(),
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.startAsync();

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
            GRPCServerConfig gRPCServerConfig = GRPCServerConfig.builder().port(this.controllerPorts[controllerId])
                    .build();
            GRPCServer server = new GRPCServer(controllerService, gRPCServerConfig);
            server.startAsync();

            this.controllerServers[controllerId] = server;
            this.controllerExecutors[controllerId] = controllerExecutor;
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

            RequestHandlersInit.bootstrapRequestHandlers(controllerService, streamStore, controllerExecutor);
            // 4. Start the REST server.
            if ( this.startRestServer ) {
                try {
                    log.info("Starting Pravega REST Service");
                    RESTServer.start(controllerService);
                } catch (Exception e) {
                    log.warn("Exception {} while starting REST server", e);
                }
            }
        }
    }

    @Override
    @Synchronized
    public void close() {
        if (isInprocSSS) {
            for ( ServiceStarter starter : this.nodeServiceStarter ) {
                starter.shutdown();
            }
        }
        if (isInprocController) {
            for ( GRPCServer controller : this.controllerServers ) {
                    controller.stopAsync();
                }
        }
    }
}
