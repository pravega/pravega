/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import com.google.common.base.Preconditions;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.ControllerServiceMain;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
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
import io.pravega.segmentstore.server.host.ServiceStarter;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

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

    /*REST server related variables*/
    private int restServerPort;

    /*SegmentStore related variables*/
    private boolean isInProcSegmentStore;
    private int segmentStoreCount = 0;
    private int[] segmentStorePorts = null;


    /*ZK related variables*/
    private boolean isInProcZK;
    private int zkPort;
    private String zkHost;
    private ZooKeeperServiceRunner zkService;

    /*HDFS related variables*/
    private boolean isInProcHDFS;
    private String hdfsUrl;


    /* SegmentStore configuration*/
    private int containerCount = 4;
    private ServiceStarter[] nodeServiceStarter = new ServiceStarter[segmentStoreCount];

    private LocalHDFSEmulator localHdfs;
    @GuardedBy("$lock")
    private ControllerServiceMain[] controllerServers;

    private String zkUrl;
    private boolean startRestServer = true;

    @Builder
    public InProcPravegaCluster(boolean isInProcZK, String zkUrl, int zkPort, boolean isInMemStorage,
                                boolean isInProcHDFS,
                                boolean isInProcController, int controllerCount, String controllerURI,
                                boolean isInProcSegmentStore, int segmentStoreCount, int containerCount,
                                boolean startRestServer, int restServerPort) {

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
        } else {
            this.isInProcHDFS = isInProcHDFS;
        }
        this.isInProcZK = isInProcZK;
        this.zkUrl = zkUrl;
        this.zkPort = zkPort;
        this.isInProcController = isInProcController;
        this.controllerURI = controllerURI;
        this.controllerCount = controllerCount;
        this.isInProcSegmentStore = isInProcSegmentStore;
        this.segmentStoreCount = segmentStoreCount;
        this.containerCount = containerCount;
        this.startRestServer = startRestServer;
        this.restServerPort = restServerPort;
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

        if (isInProcController) {
            startLocalControllers();
        }

        if (isInProcSegmentStore) {
            nodeServiceStarter = new ServiceStarter[segmentStoreCount];
            startLocalSegmentStores();
        }

    }

    private void startLocalZK() throws Exception {
        zkService = new ZooKeeperServiceRunner(zkPort);
        zkService.start();
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

    private void startLocalHDFS() throws IOException {
        localHdfs = LocalHDFSEmulator.newBuilder().baseDirName("temp").build();
        localHdfs.start();
    }

    private void startLocalSegmentStores() throws Exception {
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
    private void startLocalSegmentStore(int segmentStoreId) throws Exception {

        try {
                ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                    .builder()
                    .include(System.getProperties())
                    .include(ServiceConfig.builder()
                                          .with(ServiceConfig.CONTAINER_COUNT, containerCount)
                                          .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE)
                                          .with(ServiceConfig.ZK_URL, "localhost:" + zkPort)
                                          .with(ServiceConfig.LISTENING_PORT, this.segmentStorePorts[segmentStoreId])
                                          .with(ServiceConfig.CLUSTER_NAME, this.clusterName)
                                          .with(ServiceConfig.STORAGE_IMPLEMENTATION, isInMemStorage ?
                                                 ServiceConfig.StorageTypes.INMEMORY.toString() :
                                                 ServiceConfig.StorageTypes.FILESYSTEM.toString()))
                    .include(DurableLogConfig.builder()
                                          .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                                          .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                                          .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                    .include(ReadIndexConfig.builder()
                                          .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 60 * 1000)
                                          .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 128 * 1024 * 1024L))
                    .include(AutoScalerConfig.builder()
                                             .with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:" + controllerPorts[0]));

            ServiceStarter.Options.OptionsBuilder optBuilder = ServiceStarter.Options.builder().rocksDb(true)
                    .zkSegmentManager(true);

            nodeServiceStarter[segmentStoreId] = new ServiceStarter(configBuilder.build(),
                    optBuilder.bookKeeper(!isInMemStorage).build());
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
                .sessionTimeoutMs(10 * 1000)
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

        RESTServerConfig restServerConfig = RESTServerConfigImpl.builder()
                .host("localhost")
                .port(this.restServerPort)
                .build();

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .threadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .controllerClusterListenerEnabled(false)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.of(restServerConfig))
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
    public void close() throws Exception {
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

        if (this.zkService != null) {
            this.zkService.close();
            this.zkService = null;
        }
    }
}
