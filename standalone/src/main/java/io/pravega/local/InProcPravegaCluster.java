/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.local;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.security.TLSProtocolVersion;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.common.function.Callbacks;
import io.pravega.common.security.ZKTLSUtils;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.ControllerServiceMain;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.impl.ControllerEventProcessorConfigImpl;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.shared.rest.impl.RESTServerConfigImpl;
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
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.security.auth.Credentials;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

import lombok.Builder;
import lombok.Cleanup;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

@Slf4j
@Builder
@ToString
public class InProcPravegaCluster implements AutoCloseable {

    private static final String ALL_INTERFACES = "0.0.0.0";
    private static final int THREADPOOL_SIZE = 20;
    private boolean isInMemStorage;

    /* Cluster name */
    private final String clusterName = "singlenode-" + UUID.randomUUID();

    /*Enabling this will configure security for the singlenode with hardcoded cert files and creds.*/
    private boolean enableAuth;
    private boolean enableTls;
    private String[] tlsProtocolVersion;

    private boolean enableTlsReload;

    /* Metrics related variables*/
    private boolean enableMetrics;
    private boolean enableInfluxDB;
    private boolean enablePrometheus;
    private int metricsReportInterval;

    /*Controller related variables*/
    private boolean isInProcController;
    private int controllerCount;
    private int[] controllerPorts;
    private String controllerURI;

    /*REST server related variables*/
    private int restServerPort;

    /*SegmentStore related variables*/
    private boolean isInProcSegmentStore;
    private int segmentStoreCount;
    private int[] segmentStorePorts;


    /*ZK related variables*/
    private boolean isInProcZK;
    private int zkPort;
    private String zkHost;
    private ZooKeeperServiceRunner zkService;

    /*HDFS related variables*/
    private boolean isInProcHDFS;
    private String hdfsUrl;


    /* SegmentStore configuration*/
    private int containerCount;
    private ServiceStarter[] nodeServiceStarter;

    private LocalHDFSEmulator localHdfs;
    @GuardedBy("$lock")
    private ControllerServiceMain[] controllerServers;

    private String zkUrl;

    private boolean enableRestServer;
    private String userName;
    private String passwd;
    private String certFile;
    private String keyFile;
    private String jksTrustFile;
    private String passwdFile;
    private boolean secureZK;
    private String keyPasswordFile;
    private String jksKeyFile;

    private boolean enableAdminGateway;
    private int adminGatewayPort;
    private boolean replyWithStackTraceOnError;

    public static final class InProcPravegaClusterBuilder {

        // default values
        private int containerCount = 4;
        private boolean enableRestServer = true;
        private boolean replyWithStackTraceOnError = true;
        private String[] tlsProtocolVersion = new TLSProtocolVersion(SingleNodeConfig.TLS_PROTOCOL_VERSION.getDefaultValue()).getProtocols();

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
            Preconditions.checkState(!enableTls ||
                            (!Strings.isNullOrEmpty(this.keyFile)
                                    && !Strings.isNullOrEmpty(this.certFile)
                                    && !Strings.isNullOrEmpty(this.jksKeyFile)
                                    && !Strings.isNullOrEmpty(this.jksTrustFile)
                                    && !Strings.isNullOrEmpty(this.keyPasswordFile)),
                    "TLS enabled, but not all parameters set");

            this.isInProcHDFS = !this.isInMemStorage;
            return new InProcPravegaCluster(isInMemStorage, enableAuth, enableTls, tlsProtocolVersion, enableTlsReload,
                    enableMetrics, enableInfluxDB, enablePrometheus, metricsReportInterval,
                    isInProcController, controllerCount, controllerPorts, controllerURI,
                    restServerPort, isInProcSegmentStore, segmentStoreCount, segmentStorePorts, isInProcZK, zkPort, zkHost,
                    zkService, isInProcHDFS, hdfsUrl, containerCount, nodeServiceStarter, localHdfs, controllerServers, zkUrl,
                    enableRestServer, userName, passwd, certFile, keyFile, jksTrustFile, passwdFile, secureZK, keyPasswordFile,
                    jksKeyFile, enableAdminGateway, adminGatewayPort, replyWithStackTraceOnError);
        }
    }

    @Synchronized
    public void setControllerPorts(int[] controllerPorts) {
        this.controllerPorts = Arrays.copyOf(controllerPorts, controllerPorts.length);
    }

    @Synchronized
    public void setSegmentStorePorts(int[] segmentStorePorts) {
        this.segmentStorePorts = Arrays.copyOf(segmentStorePorts, segmentStorePorts.length);

    }

    /**
     * Kicks off the cluster creation. right now it can be done only once in lifetime of a process.
     *
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
        for (String path : pathsTobeCleaned) {
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
        String baseDir = "temp";
        log.info("Starting HDFS Emulator @ {}/{}", System.getProperty("java.io.tmpdir"), baseDir);
        localHdfs = LocalHDFSEmulator.newBuilder().baseDirName(baseDir).build();
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
        if (this.enableAuth) {
            setAuthSystemProperties();
        }

        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
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
                        .with(ServiceConfig.TLS_PROTOCOL_VERSION, Arrays.stream(this.tlsProtocolVersion).collect(Collectors.joining(",")))
                        .with(ServiceConfig.KEY_FILE, this.keyFile)
                        .with(ServiceConfig.REST_KEYSTORE_FILE, this.jksKeyFile)
                        .with(ServiceConfig.REST_KEYSTORE_PASSWORD_FILE, this.keyPasswordFile)
                        .with(ServiceConfig.CERT_FILE, this.certFile)
                        .with(ServiceConfig.ENABLE_TLS_RELOAD, this.enableTlsReload)
                        .with(ServiceConfig.LISTENING_IP_ADDRESS, ALL_INTERFACES)
                        .with(ServiceConfig.CACHE_POLICY_MAX_TIME, 60)
                        .with(ServiceConfig.CACHE_POLICY_MAX_SIZE, 128 * 1024 * 1024L)
                        .with(ServiceConfig.DATALOG_IMPLEMENTATION, isInMemStorage ?
                                ServiceConfig.DataLogType.INMEMORY :
                                ServiceConfig.DataLogType.BOOKKEEPER)
                        .with(ServiceConfig.STORAGE_LAYOUT, StorageLayoutType.CHUNKED_STORAGE)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, isInMemStorage ?
                                ServiceConfig.StorageType.INMEMORY.name() :
                                ServiceConfig.StorageType.FILESYSTEM.name())
                        .with(ServiceConfig.ENABLE_ADMIN_GATEWAY, this.enableAdminGateway)
                        .with(ServiceConfig.ADMIN_GATEWAY_PORT, this.adminGatewayPort)
                        .with(ServiceConfig.REPLY_WITH_STACK_TRACE_ON_ERROR, this.replyWithStackTraceOnError)
                        .with(ServiceConfig.REST_LISTENING_PORT, ServiceConfig.REST_LISTENING_PORT.getDefaultValue() + segmentStoreId)
                        .with(ServiceConfig.REST_LISTENING_ENABLE, this.enableRestServer))
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
                        .with(AutoScalerConfig.TLS_CERT_FILE, this.certFile)
                        .with(AutoScalerConfig.VALIDATE_HOSTNAME, false))
                .include(MetricsConfig.builder()
                        .with(MetricsConfig.ENABLE_STATISTICS, enableMetrics)
                        .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, enableInfluxDB)
                        .with(MetricsConfig.ENABLE_PROMETHEUS, enablePrometheus)
                        .with(MetricsConfig.OUTPUT_FREQUENCY, metricsReportInterval));

        String configFile = System.getProperty(SingleNodeConfig.PROPERTY_FILE);
        if (configFile != null) {
            configBuilder.include(configFile);
        }
        configBuilder.include(System.getProperties());

        nodeServiceStarter[segmentStoreId] = new ServiceStarter(configBuilder.build());
        nodeServiceStarter[segmentStoreId].start();
    }

    private void setAuthSystemProperties() {
        if (authPropertiesAlreadySet()) {
            log.debug("Auth params already specified via system properties or environment variables.");
        } else {
            if (!Strings.isNullOrEmpty(this.userName)) {
                Credentials credentials = new DefaultCredentials(this.passwd, this.userName);
                System.setProperty("pravega.client.auth.loadDynamic", "false");
                System.setProperty("pravega.client.auth.method", credentials.getAuthenticationType());
                System.setProperty("pravega.client.auth.token", credentials.getAuthenticationToken());
                log.debug("Done setting auth params via system properties.");
            } else {
                log.debug("Cannot set auth params as username is null or empty");
            }
        }
    }

    private boolean authPropertiesAlreadySet() {
        return !Strings.isNullOrEmpty(System.getProperty("pravega.client.auth.method"))
                || !Strings.isNullOrEmpty(System.getenv("pravega_client_auth_method"));
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

        StoreClientConfig storeClientConfig = StoreClientConfigImpl.withPravegaTablesClient(zkClientConfig);

        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .containerCount(containerCount)
                .hostMonitorMinRebalanceInterval(1)
                .build();

        TimeoutServiceConfig timeoutServiceConfig = TimeoutServiceConfig.builder()
                .maxLeaseValue(Config.MAX_LEASE_VALUE)
                .build();

        ControllerEventProcessorConfig eventProcessorConfig = ControllerEventProcessorConfigImpl
                .withDefaultBuilder()
                .shutdownTimeout(Duration.ofMillis(1000))
                .build();

        GRPCServerConfig grpcServerConfig = GRPCServerConfigImpl
                .builder()
                .port(this.controllerPorts[controllerId])
                .publishedRPCHost("localhost")
                .publishedRPCPort(this.controllerPorts[controllerId])
                .authorizationEnabled(this.enableAuth)
                .tlsEnabled(this.enableTls)
                .tlsProtocolVersion(this.tlsProtocolVersion)
                .tlsTrustStore(this.certFile)
                .tlsCertFile(this.certFile)
                .tlsKeyFile(this.keyFile)
                .userPasswordFile(this.passwdFile)
                .tokenSigningKey("secret")
                .accessTokenTTLInSeconds(600)
                .replyWithStackTraceOnError(false)
                .requestTracingEnabled(true)
                .build();

        RESTServerConfig restServerConfig = null;
        if (this.enableRestServer) {
            restServerConfig = RESTServerConfigImpl.builder()
                    .host("0.0.0.0")
                    .port(this.restServerPort)
                    .tlsEnabled(this.enableTls)
                    .tlsProtocolVersion(this.tlsProtocolVersion)
                    .keyFilePath(this.jksKeyFile)
                    .keyFilePasswordPath(this.keyPasswordFile)
                    .build();
        }

        ControllerServiceConfig serviceConfig = ControllerServiceConfigImpl.builder()
                .threadPoolSize(Runtime.getRuntime().availableProcessors())
                .storeClientConfig(storeClientConfig)
                .hostMonitorConfig(hostMonitorConfig)
                .controllerClusterListenerEnabled(false)
                .timeoutServiceConfig(timeoutServiceConfig)
                .eventProcessorConfig(Optional.of(eventProcessorConfig))
                .grpcServerConfig(Optional.of(grpcServerConfig))
                .restServerConfig(Optional.ofNullable(restServerConfig))
                .shutdownTimeout(Duration.ofMillis(1000))
                .minBucketRedistributionIntervalInSeconds(10)
                .build();

        ControllerServiceMain controllerService = new ControllerServiceMain(serviceConfig);
        try {
            controllerService.startAsync().awaitRunning();
            return controllerService;
        } catch (Throwable ex) {
            Callbacks.invokeSafely(controllerService::close, ex2 -> log.error("Unable to clean up controller startup.", ex2));
            throw ex;
        }
    }

    @Synchronized
    public String getControllerURI() {
        return controllerURI;
    }

    @Override
    @Synchronized
    public void close() throws Exception {
        if (isInProcSegmentStore) {
            for (ServiceStarter starter : this.nodeServiceStarter) {
                starter.shutdown();
            }
        }
        if (isInProcController) {
            for (ControllerServiceMain controller : this.controllerServers) {
                Callbacks.invokeSafely(controller::close, ex -> log.error("Unable to shut down Controller.", ex));
            }
        }

        if (this.zkService != null) {
            this.zkService.close();
            this.zkService = null;
        }
    }

    // Assumes toString is autogenerated by Lombok.
    public String print() {
        String auto = this.toString();
        int start = auto.indexOf("(") + 1;
        int end = auto.indexOf(")");
        String[] opts = auto.substring(start, end).split(",");
        StringBuilder result = new StringBuilder(String.format("%s%n", auto.substring(0, start)));
        for (String opt : opts) {
            result.append(String.format("\t%s%n", opt.trim()));
        }
        result.append(auto.substring(end, auto.length()));
        return result.toString();
    }
}
