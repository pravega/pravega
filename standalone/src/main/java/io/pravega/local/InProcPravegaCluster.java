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
import io.pravega.common.auth.ZKTLSUtils;
import com.google.common.base.Strings;
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
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.shared.metrics.MetricsConfig;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

@Slf4j
@Builder
public class InProcPravegaCluster implements AutoCloseable {

    private static final int THREADPOOL_SIZE = 20;
    private boolean isInMemStorage;

    /* Cluster name */
    private final String clusterName = "singlenode-" + UUID.randomUUID();
    @Builder.Default
    private boolean enableMetrics = false;

    /*Enabling this will configure security for the singlenode with hardcoded cert files and creds.*/
    @Builder.Default
    private boolean enableAuth = false;
    @Builder.Default
    private boolean enableTls = false;

    /*Controller related variables*/
    private boolean isInProcController;
    private int controllerCount;
    @Builder.Default
    private int[] controllerPorts = null;
    @Builder.Default
    private String controllerURI = null;

    /*REST server related variables*/
    private int restServerPort;

    /*SegmentStore related variables*/
    private boolean isInProcSegmentStore;
    @Builder.Default
    private int segmentStoreCount = 0;
    @Builder.Default
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
    @Builder.Default
    private int containerCount = 4;
    private ServiceStarter[] nodeServiceStarter;

    private LocalHDFSEmulator localHdfs;
    @GuardedBy("$lock")
    private ControllerServiceMain[] controllerServers;

    private String zkUrl;

    @Builder.Default
    private boolean enableRestServer = true;
    private String userName;
    private String passwd;
    private String certFile;
    private String keyFile;
    private String jksTrustFile;
    private String passwdFile;
    private boolean secureZK;
    private String keyPasswordFile;
    private String jksKeyFile;

    public static final class InProcPravegaClusterBuilder {
        public InProcPravegaCluster build() {
            //Check for valid combinations of flags
            //For ZK
            Preconditions.checkState(isInProcZK || zkUrl != null, "ZkUrl must be specified");

            //For controller
            Preconditions.checkState(isInProcController || controllerURI != null,
                    "ControllerURI should be defined for external controller");
            Preconditions.checkState(isInProcController || this.controllerPorts != null,
                    "Controller ports not present");

            //For SegmentStore
            Preconditions.checkState(isInProcSegmentStore || this.segmentStorePorts != null, "SegmentStore ports not declared");

            //Check TLS related parameters
            Preconditions.checkState(!enableTls || (!Strings.isNullOrEmpty(this.keyFile) && !Strings.isNullOrEmpty(this.certFile)),
                    "TLS parameters not set");

            if (this.isInMemStorage) {
                this.isInProcHDFS = false;
            }
            return new InProcPravegaCluster(isInMemStorage, enableMetrics, enableAuth, enableTls,
                    isInProcController, controllerCount, controllerPorts, controllerURI,
                    restServerPort, isInProcSegmentStore, segmentStoreCount, segmentStorePorts, isInProcZK, zkPort, zkHost,
                    zkService, isInProcHDFS, hdfsUrl, containerCount, nodeServiceStarter, localHdfs, controllerServers, zkUrl,
                    enableRestServer, userName, passwd, certFile, keyFile, jksTrustFile, passwdFile, secureZK, keyPasswordFile, jksKeyFile);
        }
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
        zkService = new ZooKeeperServiceRunner(zkPort, secureZK, jksKeyFile, keyPasswordFile, jksTrustFile);
        zkService.initialize();
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
        if (secureZK) {
            ZKTLSUtils.setSecureZKClientProperties(jksTrustFile, "1111_aaaa");
        }

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
        Properties authProps = new Properties();
        authProps.setProperty("pravega.client.auth.method", "Default");
        authProps.setProperty("pravega.client.auth.userName", "arvind");
        authProps.setProperty("pravega.client.auth.password", "1111_aaaa");

        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(System.getProperties())
                .include(authProps)
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, containerCount)
                        .with(ServiceConfig.THREAD_POOL_SIZE, THREADPOOL_SIZE)
                        .with(ServiceConfig.ZK_URL, "localhost:" + zkPort)
                        .with(ServiceConfig.SECURE_ZK, this.secureZK)
                        .with(ServiceConfig.ZK_TRUSTSTORE_LOCATION, jksTrustFile)
                        .with(ServiceConfig.ZK_TRUST_STORE_PASSWORD_PATH, keyPasswordFile)
                        .with(ServiceConfig.LISTENING_PORT, this.segmentStorePorts[segmentStoreId])
                        .with(ServiceConfig.CLUSTER_NAME, this.clusterName)
                        .with(ServiceConfig.ENABLE_TLS, this.enableTls)
                        .with(ServiceConfig.KEY_FILE, Optional.ofNullable(this.keyFile))
                        .with(ServiceConfig.CERT_FILE, Optional.ofNullable(this.certFile))
                        .with(ServiceConfig.DATALOG_IMPLEMENTATION, isInMemStorage ?
                                ServiceConfig.DataLogType.INMEMORY :
                                ServiceConfig.DataLogType.BOOKKEEPER)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, isInMemStorage ?
                                ServiceConfig.StorageType.INMEMORY :
                                ServiceConfig.StorageType.FILESYSTEM))
                .include(DurableLogConfig.builder()
                        .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                        .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 100)
                        .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 100 * 1024 * 1024L))
                .include(AutoScalerConfig.builder()
                        .with(AutoScalerConfig.CONTROLLER_URI, (this.enableTls ? "tls" : "tcp") + "://localhost:"
                                                                                + controllerPorts[0])
                                         .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                                         .with(AutoScalerConfig.AUTH_ENABLED, this.enableAuth)
                                         .with(AutoScalerConfig.TLS_ENABLED, this.enableTls)
                                         .with(AutoScalerConfig.TLS_CERT_FILE, this.certFile))
                .include(MetricsConfig.builder()
                        .with(MetricsConfig.ENABLE_STATISTICS, enableMetrics));

        nodeServiceStarter[segmentStoreId] = new ServiceStarter(configBuilder.build());
        nodeServiceStarter[segmentStoreId].start();
    }

    private void startLocalControllers() {
        controllerServers = new ControllerServiceMain[this.controllerCount];

        for (int i = 0; i < this.controllerCount; i++) {
            controllerServers[i] = startLocalController(i);
        }
        controllerURI = (this.enableTls ? "tls" : "tcp") + "://localhost:" + controllerPorts[0];
        for (int i = 1; i < this.controllerCount; i++) {
            controllerURI += ",localhost:" + controllerPorts[i];
        }

    }

    private ControllerServiceMain startLocalController(int controllerId) {

        ZKClientConfig zkClientConfig = ZKClientConfigImpl.builder()
                .connectionString(zkUrl)
                .namespace("pravega/" + clusterName)
                .initialSleepInterval(2000)
                .maxRetries(1)
                .sessionTimeoutMs(10 * 1000)
                .secureConnectionToZooKeeper(this.secureZK)
                .trustStorePath(jksTrustFile)
                .trustStorePasswordPath(keyPasswordFile)
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

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl
                .builder()
                .port(this.controllerPorts[controllerId])
                .publishedRPCHost("localhost")
                .publishedRPCPort(this.controllerPorts[controllerId])
                .authorizationEnabled(this.enableAuth)
                .tlsEnabled(this.enableTls)
                .tlsTrustStore(this.certFile)
                .tlsCertFile(this.certFile)
                .tlsKeyFile(this.keyFile)
                .userPasswordFile(this.passwdFile)
                .tokenSigningKey("secret")
                .replyWithStackTraceOnError(false)
                .requestTracingEnabled(true)
                .build();

        RESTServerConfig restServerConfig = null;
        if (this.enableRestServer) {
            restServerConfig = RESTServerConfigImpl.builder()
                    .host("0.0.0.0")
                    .port(this.restServerPort)
                    .tlsEnabled(this.enableTls)
                    .keyFilePath(this.jksKeyFile)
                    .keyFilePasswordPath(this.keyPasswordFile)
                    .build();
        }

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .threadPoolSize(Config.ASYNC_TASK_POOL_SIZE)
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .controllerClusterListenerEnabled(false)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.ofNullable(restServerConfig))
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
