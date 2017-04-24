/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.local;

import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.ControllerServiceMain;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.store.client.StoreClientConfig;
import io.pravega.controller.store.client.ZKClientConfig;
import io.pravega.controller.store.client.impl.StoreClientConfigImpl;
import io.pravega.controller.store.client.impl.ZKClientConfigImpl;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.util.Config;
import io.pravega.service.server.host.ServiceStarter;
import io.pravega.service.server.host.stat.AutoScalerConfig;
import io.pravega.service.server.logs.DurableLogConfig;
import io.pravega.service.server.reading.ReadIndexConfig;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.service.server.store.ServiceConfig;
import io.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import io.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.google.common.base.Preconditions;
import com.twitter.distributedlog.LocalDLMEmulator;
import com.twitter.distributedlog.admin.DistributedLogAdmin;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;

import lombok.Builder;
import lombok.Cleanup;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static org.apache.bookkeeper.util.LocalBookKeeper.runZookeeper;

@Slf4j
public class InProcPravegaCluster implements AutoCloseable {

    private static final int THREADPOOL_SIZE = 20;
    private final boolean isInMemStorage;

    /* Cluster name */
    private final String clusterName = "singlenode-" + UUID.randomUUID();

    /*Controller related variables*/
    private boolean isInProcController;
    private int controllerCount;
    private int[] controllerPorts = null;

    private String controllerURI = null;

    /*SegmentStore related variables*/
    private boolean isInProcSegmentStore;
    private int segmentStoreCount = 0;
    private int[] segmentStorePorts = null;

    /*Distributed log related variables*/
    private boolean isInProcDL;
    private int bookieCount;
    private final int initialBookiePort;


    /*ZK related variables*/
    private boolean isInProcZK;
    private int zkPort;
    private String zkHost;

    /*HDFS related variables*/
    private boolean isInProcHDFS;
    private String hdfsUrl;


    /* SegmentStore configuration*/
    private int containerCount = 4;
    private ServiceStarter[] nodeServiceStarter = new ServiceStarter[segmentStoreCount];

    private LocalHDFSEmulator localHdfs;
    private LocalDLMEmulator localDlm;
    @GuardedBy("$lock")
    private ControllerServiceMain[] controllerServers;

    private String zkUrl;
    private boolean startRestServer = false;

    @Builder
    public InProcPravegaCluster(boolean isInProcZK, String zkUrl, int zkPort, boolean isInMemStorage,
                                boolean isInProcHDFS, boolean isInProcBK, int initialBookiePort,
                                boolean isInProcController, int controllerCount, String controllerURI,
                                boolean isInProcSegmentStore, int segmentStoreCount, int containerCount, boolean startRestServer) {

        //Check for valid combinations of flags
        //For ZK
        Preconditions.checkState(isInProcZK || zkUrl != null, "ZkUrl must be specified");

        //For controller
        Preconditions.checkState( isInProcController || controllerURI != null,
                "ControllerURI should be defined for external controller");
        Preconditions.checkState(isInProcController || this.controllerPorts != null,
                "Controller ports not present");

        //For SegmentStore
        Preconditions.checkState(  isInProcSegmentStore || this.segmentStorePorts != null, "SegmentStore ports not declared");

        this.isInMemStorage = isInMemStorage;
        if ( isInMemStorage ) {
            this.isInProcHDFS = false;
            this.isInProcDL = false;
        } else {
            this.isInProcHDFS = isInProcHDFS;
            this.isInProcDL = isInProcBK;
        }
        this.isInProcZK = isInProcZK;
        this.zkUrl = zkUrl;
        this.zkPort = zkPort;
        this.initialBookiePort = initialBookiePort;
        this.isInProcController = isInProcController;
        this.controllerURI = controllerURI;
        this.controllerCount = controllerCount;
        this.isInProcSegmentStore = isInProcSegmentStore;
        this.segmentStoreCount = segmentStoreCount;
        this.containerCount = containerCount;
        this.startRestServer = startRestServer;
        checkAvailableFeatures();
    }

    @Synchronized
    public void setControllerPorts(int[] controllerPorts) {
        this.controllerPorts = Arrays.copyOf( controllerPorts, controllerPorts.length);
    }

    @Synchronized
    public void setSegmentStorePorts(int[] segmentStorePorts) {
        this.segmentStorePorts = Arrays.copyOf(segmentStorePorts, segmentStorePorts.length);

    }

    /**
     * Kicks off the cluster creation. right now it can be done only once in lifetime of a process.
     * @throws Exception Exception thrown by ZK/HDFS etc.
     */
    @Synchronized
    public void start() throws Exception {

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

        if (isInProcController) {
            startLocalControllers();
        }

        if (isInProcSegmentStore) {
            nodeServiceStarter = new ServiceStarter[segmentStoreCount];
            startLocalSegmentStores();
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
                "distributedlog://" + zkUrl  + "/pravega/" + clusterName + "/segmentstore/containers"};
        try {
            admin.run(params);
        } catch (Exception e) {
            log.warn("Exception {} while configuring the DL bindings.", e.getMessage());
        }
    }

    private void startLocalDL() throws Exception {
        localDlm = LocalDLMEmulator.newBuilder().shouldStartZK(false).zkHost(zkHost).
                    zkPort(zkPort).numBookies(this.bookieCount).build();
        localDlm.start();
    }


    private void startLocalHDFS() throws IOException {
        localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();
        localHdfs.start();
    }

    private void startLocalSegmentStores() throws IOException {
        for (int i = 0; i < this.segmentStoreCount; i++) {
            startLocalSegmentStore(i);
        }

    }

    /**
     * Starts a SegmentStore with the given id. This is re-entrant. Eventually this will allow starting and stopping of
     * individual SegmentStore instances. This is not possible right now.
     *
     * @param segmentStoreId id of the SegmentStore.
     */
    private void startLocalSegmentStore(int segmentStoreId) throws IOException {

        try {
                ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                    .builder()
                    .include("config.properties")
                    .include(System.getProperties())
                    .include(ServiceConfig.builder()
                                          .with(ServiceConfig.CONTAINER_COUNT, containerCount)
                                          .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE)
                                          .with(ServiceConfig.ZK_URL, "localhost:" + zkPort)
                                          .with(ServiceConfig.LISTENING_PORT, this.segmentStorePorts[segmentStoreId])
                                          .with(ServiceConfig.CLUSTER_NAME, this.clusterName))
                    .include(DurableLogConfig.builder()
                                          .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                                          .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                                          .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                    .include(ReadIndexConfig.builder()
                                          .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 60 * 1000)
                                          .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 128 * 1024 * 1024L))
                    .include(AutoScalerConfig.builder().with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:" + controllerPorts[0]));

            if ( !isInMemStorage ) {
                    configBuilder = configBuilder.include(HDFSStorageConfig.builder()
                        .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/",
                                localHdfs.getNameNodePort())))
                            .include(DistributedLogConfig.builder()
                                    .with(DistributedLogConfig.HOSTNAME, "localhost")
                                    .with(DistributedLogConfig.PORT, zkPort)
                                    .with(DistributedLogConfig.NAMESPACE, "/pravega/"
                                                                            + clusterName
                                                                            + "/segmentstore/containers"));
            }

            ServiceStarter.Options.OptionsBuilder optBuilder = ServiceStarter.Options.builder().rocksDb(true)
                    .zkSegmentManager(true);

            nodeServiceStarter[segmentStoreId] = new ServiceStarter(configBuilder.build(), optBuilder.hdfs(!isInMemStorage)
                    .distributedLog(!isInMemStorage).build());
        } catch (Exception e) {
            throw e;
        }
        nodeServiceStarter[segmentStoreId].start();
    }

    private void startLocalControllers() {
        controllerServers = new ControllerServiceMain[this.controllerCount];

        for (int i = 0; i < this.controllerCount; i++) {
            controllerServers[i] = startLocalController(i);
        }
        controllerURI = "localhost:" + controllerPorts[0];

    }

    private ControllerServiceMain startLocalController(int controllerId) {

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
                .connectionString(zkUrl)
                .namespace("pravega/" + clusterName)
                .initialSleepInterval(2000)
                .maxRetries(1)
                .build();

        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withZKClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(Config.HOST_STORE_CONTAINER_COUNT)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .maxScaleGracePeriod(Config.MAX_SCALE_GRACE_PERIOD)
                .build();

        ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfigImpl.withDefault();

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl.builder()
                .port(this.controllerPorts[controllerId])
                .publishedRPCHost("localhost")
                .publishedRPCPort(this.controllerPorts[controllerId])
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .serviceThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .taskThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .eventProcThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .requestHandlerThreadPoolSize(Config.ASYNC_TASK_POOL_SIZE / 2)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .controllerClusterListenerConfig(Optional.empty())
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .requestHandlersEnabled(true)
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.empty())
                .build();

        ControllerServiceMain controllerService = new ControllerServiceMain(serviceConfig);
        controllerService.startAsync();
        return controllerService;
    }

    @Synchronized
    public String getControllerURI() {
        return controllerURI;
    }

    @Synchronized
    public String getZkUrl() {
        return zkUrl;
    }

    @Override
    @Synchronized
    public void close() {
        if (isInProcSegmentStore) {
            for ( ServiceStarter starter : this.nodeServiceStarter ) {
                starter.shutdown();
            }
        }
        if (isInProcController) {
            for ( ControllerServiceMain controller : this.controllerServers ) {
                    controller.stopAsync();
                }
        }
    }
}
