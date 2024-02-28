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
package io.pravega.controller.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.function.Callbacks;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.util.BooleanUtils;
import io.pravega.controller.fault.ControllerClusterListener;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.fault.SegmentContainerMonitor;
import io.pravega.controller.fault.UniformContainerBalancer;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.server.bucket.BucketManager;
import io.pravega.controller.server.bucket.BucketServiceFactory;
import io.pravega.controller.server.bucket.PeriodicRetention;
import io.pravega.controller.server.bucket.PeriodicWatermarking;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.shared.health.bindings.resources.HealthImpl;
import io.pravega.controller.server.rest.resources.PingImpl;
import io.pravega.controller.server.rest.resources.StreamMetadataResourceImpl;
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.rest.RESTServer;
import io.pravega.controller.server.health.ClusterListenerHealthContributor;
import io.pravega.controller.server.health.EventProcessorHealthContributor;
import io.pravega.controller.server.health.GRPCServerHealthContributor;
import io.pravega.controller.server.health.RetentionServiceHealthContributor;
import io.pravega.controller.server.health.SegmentContainerMonitorHealthContributor;
import io.pravega.controller.server.health.WatermarkingServiceHealthContributor;
import io.pravega.controller.server.rpc.grpc.GRPCServer;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreType;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.RequestSweeper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.controller.task.TaskSweeper;
import io.pravega.controller.util.Config;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractIdleService implements AutoCloseable {
    private final ControllerServiceConfig serviceConfig;
    private final StoreClient storeClient;
    private final String objectId;

    private ScheduledExecutorService controllerExecutor;
    private ScheduledExecutorService eventExecutor;
    private ScheduledExecutorService retentionExecutor;
    private ScheduledExecutorService watermarkingExecutor;

    private ConnectionFactory connectionFactory;
    private ConnectionPool connectionPool;
    private StreamMetadataStore streamStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private KVTableMetadataStore kvtMetadataStore;
    private TableMetadataTasks kvtMetadataTasks;
    private BucketManager retentionService;
    private BucketManager watermarkingService;
    private PeriodicWatermarking watermarkingWork;
    private SegmentContainerMonitor monitor;
    private ControllerClusterListener controllerClusterListener;
    private SegmentHelper segmentHelper;
    private HealthServiceManager healthServiceManager;
    private ControllerService controllerService;

    private LocalController localController;
    private ControllerEventProcessors controllerEventProcessors;
    /**
     * ControllerReadyLatch is released once localController, streamTransactionMetadataTasks and controllerService
     * variables are initialized in the startUp method.
     */
    private final CountDownLatch controllerReadyLatch;

    private GRPCServer grpcServer;
    private RESTServer restServer;

    private Cluster cluster = null;

    private final Optional<SegmentHelper> segmentHelperRef;
    private final Optional<ConnectionFactory> connectionFactoryRef;
    private final Optional<StreamMetadataStore> streamMetadataStoreRef;
    private final Optional<KVTableMetadataStore> kvtMetaStoreRef;
    
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final CompletableFuture<Void> storeClientFailureFuture;
    
    public ControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient) {
        this(serviceConfig, storeClient, null);
    }

    @VisibleForTesting
    ControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient, SegmentHelper segmentHelper) {
        this(serviceConfig, storeClient, segmentHelper, null, null, null);
    }

    @VisibleForTesting
    ControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient, SegmentHelper segmentHelper,
                             ConnectionFactory connectionFactory, StreamMetadataStore streamStore, KVTableMetadataStore kvtStore) {
        this.serviceConfig = serviceConfig;
        this.storeClient = storeClient;
        this.objectId = "ControllerServiceStarter";
        this.controllerReadyLatch = new CountDownLatch(1);
        this.segmentHelperRef = Optional.ofNullable(segmentHelper);
        this.connectionFactoryRef = Optional.ofNullable(connectionFactory);
        this.streamMetadataStoreRef = Optional.ofNullable(streamStore);
        this.kvtMetaStoreRef = Optional.ofNullable(kvtStore);
        this.storeClientFailureFuture = new CompletableFuture<>();
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        log.info("Initiating controller service startUp");

        log.info("Controller serviceConfig = {}", serviceConfig.toString());
        log.info("Event processors enabled = {}", serviceConfig.getEventProcessorConfig().isPresent());
        log.info("Cluster listener enabled = {}", serviceConfig.isControllerClusterListenerEnabled());
        log.info("    Host monitor enabled = {}", serviceConfig.getHostMonitorConfig().isHostMonitorEnabled());
        log.info("     gRPC server enabled = {}", serviceConfig.getGRPCServerConfig().isPresent());
        log.info("     REST server enabled = {}", serviceConfig.getRestServerConfig().isPresent());

        final BucketStore bucketStore;
        final TaskMetadataStore taskMetadataStore;
        final HostControllerStore hostStore;
        final CheckpointStore checkpointStore;

        try {
            //Initialize the executor service.
            controllerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                                                                               "controllerpool");
            eventExecutor = ExecutorServiceHelpers.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                                                                               "eventprocessor");

            retentionExecutor = ExecutorServiceHelpers.newScheduledThreadPool(Config.RETENTION_THREAD_POOL_SIZE,
                                                                               "retentionpool");

            watermarkingExecutor = ExecutorServiceHelpers.newScheduledThreadPool(Config.WATERMARKING_THREAD_POOL_SIZE,
                                                                               "watermarkingpool");

            bucketStore = StreamStoreFactory.createBucketStore(storeClient, controllerExecutor);
            log.info("Created the bucket store.");

            taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor);
            log.info("Created the task store.");

            hostStore = HostStoreFactory.createStore(serviceConfig.getHostMonitorConfig(), storeClient);
            log.info("Created the host store.");

            checkpointStore = CheckpointStoreFactory.create(storeClient);
            log.info("Created the checkpoint store.");

            // Initialize Stream and Transaction metrics.
            StreamMetrics.initialize();
            TransactionMetrics.initialize();

            // On each controller process restart, we use a fresh hostId,
            // which is a combination of hostname and random GUID.
            String hostName = getHostName();
            Host host = new Host(hostName, getPort(), UUID.randomUUID().toString());

            // Create a RequestTracker instance to trace client requests end-to-end.
            GRPCServerConfig grpcServerConfig = serviceConfig.getGRPCServerConfig().get();
            RequestTracker requestTracker = new RequestTracker(grpcServerConfig.isRequestTracingEnabled());

            // Create a Health Service Manager instance.
            healthServiceManager = new HealthServiceManager(serviceConfig.getHealthCheckFrequency());

            if (serviceConfig.getHostMonitorConfig().isHostMonitorEnabled()) {
                //Start the Segment Container Monitor.
                monitor = new SegmentContainerMonitor(hostStore, (CuratorFramework) storeClient.getClient(),
                        new UniformContainerBalancer(),
                        serviceConfig.getHostMonitorConfig().getHostMonitorMinRebalanceInterval());
                monitor.startAsync();
                log.info("Started Segment Container Monitor service.");
                SegmentContainerMonitorHealthContributor segmentContainerMonitorHC = new SegmentContainerMonitorHealthContributor("segmentContainerMonitor", monitor );
                healthServiceManager.register(segmentContainerMonitorHC);
            }

            // This client config is used by the segment store helper (SegmentHelper) to connect to the segment store.
            ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder()
                    .controllerURI(URI.create((grpcServerConfig.isTlsEnabled() ?
                            "tls://" : "tcp://") + "localhost:" + grpcServerConfig.getPort()))
                    .trustStore(grpcServerConfig.getTlsTrustStore())
                    .validateHostName(false);

            Optional<Boolean> tlsEnabledForSegmentStore = BooleanUtils.extract(serviceConfig.getTlsEnabledForSegmentStore());
            if (tlsEnabledForSegmentStore.isPresent()) {
                clientConfigBuilder.enableTlsToSegmentStore(tlsEnabledForSegmentStore.get());
            }
            
            // Use one connection per Segment Store to save up resources.
            ClientConfig clientConfig = clientConfigBuilder.maxConnectionsPerSegmentStore(1).build();
            connectionFactory = connectionFactoryRef.orElseGet(() -> new SocketConnectionFactoryImpl(clientConfig));
            connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
            segmentHelper = segmentHelperRef.orElseGet(() -> new SegmentHelper(connectionPool, hostStore, controllerExecutor));

            GrpcAuthHelper authHelper = new GrpcAuthHelper(serviceConfig.getGRPCServerConfig().get().isAuthorizationEnabled(),
                                                           grpcServerConfig.getTokenSigningKey(),
                                                           grpcServerConfig.getAccessTokenTTLInSeconds());

            streamStore = streamMetadataStoreRef.orElseGet(() -> StreamStoreFactory.createStore(storeClient, segmentHelper, authHelper, controllerExecutor));
            log.info("Created the stream store.");

            streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore,
                    segmentHelper, controllerExecutor, eventExecutor, host.getHostId(), authHelper,
                    serviceConfig.getRetentionFrequency().toMillis());
            streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                    segmentHelper, controllerExecutor, eventExecutor, host.getHostId(), serviceConfig.getTimeoutServiceConfig(), authHelper);

            BucketServiceFactory bucketServiceFactory = new BucketServiceFactory(host.getHostId(), bucketStore, 1000,
                    serviceConfig.getMinBucketRedistributionIntervalInSeconds());
            Duration executionDurationRetention = serviceConfig.getRetentionFrequency();

            PeriodicRetention retentionWork = new PeriodicRetention(streamStore, streamMetadataTasks, retentionExecutor, requestTracker);
            retentionService = bucketServiceFactory.createRetentionService(executionDurationRetention, retentionWork::retention, retentionExecutor);

            retentionService.startAsync();
            retentionService.awaitRunning();
            log.info("Started background periodic service for Retention.");
            RetentionServiceHealthContributor retentionServiceHC = new RetentionServiceHealthContributor("retentionService", retentionService);
            healthServiceManager.register(retentionServiceHC);

            Duration executionDurationWatermarking = Duration.ofSeconds(Config.MINIMUM_WATERMARKING_FREQUENCY_IN_SECONDS);
            watermarkingWork = new PeriodicWatermarking(streamStore, bucketStore,
                    clientConfig, watermarkingExecutor, requestTracker);
            watermarkingService = bucketServiceFactory.createWatermarkingService(executionDurationWatermarking, 
                    watermarkingWork::watermark, watermarkingExecutor);

            watermarkingService.startAsync();
            watermarkingService.awaitRunning();
            log.info("Started background periodic service for Watermarking.");
            WatermarkingServiceHealthContributor watermarkingServiceHC = new WatermarkingServiceHealthContributor("watermarkingService", watermarkingService);
            healthServiceManager.register(watermarkingServiceHC);

            // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
            // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
            // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
            // are processed and deleted, that failed HostId is removed from FH folder.
            // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
            // controllers and starts sweeping tasks orphaned by those hostIds.
            TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, host.getHostId(), controllerExecutor,
                    streamMetadataTasks);

            TxnSweeper txnSweeper = new TxnSweeper(streamStore, streamTransactionMetadataTasks,
                    serviceConfig.getTimeoutServiceConfig().getMaxLeaseValue(), controllerExecutor);
            RequestSweeper requestSweeper = new RequestSweeper(streamStore, controllerExecutor,
                    streamMetadataTasks);

            if (serviceConfig.isControllerClusterListenerEnabled()) {
                cluster = new ClusterZKImpl((CuratorFramework) storeClient.getClient(), ClusterType.CONTROLLER);
            }

            kvtMetadataStore = kvtMetaStoreRef.orElseGet(() -> KVTableStoreFactory.createStore(storeClient, segmentHelper,
                    authHelper, controllerExecutor, streamStore));
            kvtMetadataTasks = new TableMetadataTasks(kvtMetadataStore, segmentHelper, controllerExecutor,
                    eventExecutor, host.getHostId(), authHelper);
            controllerService = new ControllerService(kvtMetadataStore, kvtMetadataTasks, streamStore, bucketStore,
                    streamMetadataTasks, streamTransactionMetadataTasks, segmentHelper, controllerExecutor,
                    cluster, requestTracker);

            // Setup event processors.
            setController(new LocalController(controllerService, grpcServerConfig.isAuthorizationEnabled(),
                    grpcServerConfig.getTokenSigningKey()));

            CompletableFuture<Void> eventProcessorFuture = CompletableFuture.completedFuture(null); 
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // Create ControllerEventProcessor object.
                controllerEventProcessors = new ControllerEventProcessors(host.getHostId(),
                        serviceConfig.getEventProcessorConfig().get(), localController, checkpointStore, streamStore,
                        bucketStore, connectionPool, streamMetadataTasks, streamTransactionMetadataTasks, kvtMetadataStore,
                        kvtMetadataTasks, eventExecutor);

                // Bootstrap and start it asynchronously.
                eventProcessorFuture = controllerEventProcessors.bootstrap(streamTransactionMetadataTasks,
                        streamMetadataTasks, kvtMetadataTasks)
                        .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventExecutor);
                EventProcessorHealthContributor eventProcessorHC = new EventProcessorHealthContributor("eventProcessor", controllerEventProcessors);
                healthServiceManager.register(eventProcessorHC);
            }

            // Setup and start controller cluster listener after all sweepers have been initialized.
            if (serviceConfig.isControllerClusterListenerEnabled()) {
                List<FailoverSweeper> failoverSweepers = new ArrayList<>();
                failoverSweepers.add(taskSweeper);
                failoverSweepers.add(txnSweeper);
                failoverSweepers.add(requestSweeper);
                if (serviceConfig.getEventProcessorConfig().isPresent()) {
                    assert controllerEventProcessors != null;
                    failoverSweepers.add(controllerEventProcessors);
                }

                controllerClusterListener = new ControllerClusterListener(host, cluster, controllerExecutor, failoverSweepers);
                controllerClusterListener.startAsync();
                ClusterListenerHealthContributor clusterListenerHC = new ClusterListenerHealthContributor("clusterListener", controllerClusterListener);
                healthServiceManager.register(clusterListenerHC);
            }

            // Start the Health Service.
            healthServiceManager.start();

            // Start RPC server.
            if (serviceConfig.getGRPCServerConfig().isPresent()) {
                grpcServer = new GRPCServer(controllerService, grpcServerConfig, requestTracker);
                grpcServer.startAsync();
                grpcServer.awaitRunning();
                GRPCServerHealthContributor grpcServerHC = new GRPCServerHealthContributor("GRPCServer", grpcServer);
                healthServiceManager.register(grpcServerHC);
            }

            // Start REST server.
            if (serviceConfig.getRestServerConfig().isPresent()) {
                List<Object> resources = new ArrayList<>();
                resources.add(new StreamMetadataResourceImpl(this.localController,
                                controllerService,
                                grpcServer.getAuthHandlerManager(),
                                connectionFactory,
                                clientConfig));
                resources.add(new HealthImpl(grpcServer.getAuthHandlerManager(), healthServiceManager.getEndpoint()));
                resources.add(new PingImpl());
                MetricsProvider.getMetricsProvider().prometheusResource().ifPresent(resources::add);
                restServer = new RESTServer(serviceConfig.getRestServerConfig().get(), Set.copyOf(resources));
                restServer.startAsync();
                restServer.awaitRunning();
            }

            // Wait for controller event processors to start.
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // if store client has failed because of session expiration, there are two possibilities where 
                // controllerEventProcessors.awaitRunning may be stuck forever -
                // 1. stream creation is retried indefinitely and cannot complete because of zk session expiration
                // 2. event writer after stream creation throws exception. 
                // In both of above cases controllerEventProcessors.startAsync may not get called. 
                CompletableFuture.anyOf(storeClientFailureFuture, 
                        eventProcessorFuture.thenAccept(x -> controllerEventProcessors.awaitRunning())).join();
            }

            // Wait for controller cluster listeners to start.
            if (serviceConfig.isControllerClusterListenerEnabled()) {
                controllerClusterListener.awaitRunning();
            }
        } catch (Exception e) {
            log.error("Failed trying to start controller services", e);
            throw e;
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "shutDown");
        log.info("Initiating controller service shutDown....");

        try {
            if (healthServiceManager != null) {
                healthServiceManager.close();
                log.info("HealthService shutdown.");
            }

            if (restServer != null) {
                restServer.stopAsync();
            }
            if (grpcServer != null) {
                grpcServer.stopAsync();
            }
            if (controllerEventProcessors != null) {
                controllerEventProcessors.stopAsync();
            }
            if (monitor != null) {
                monitor.stopAsync();
            }
            if (controllerClusterListener != null) {
                controllerClusterListener.stopAsync();
            }

            if (retentionService != null) {
                retentionService.stopAsync();

            }

            if (watermarkingService != null) {
                watermarkingService.stopAsync();
            }

            close(watermarkingWork);

            if (streamMetadataTasks != null) {
                streamMetadataTasks.close();
                log.debug("StreamMetadataTasks closed.");
            }

            if (streamTransactionMetadataTasks != null) {
                streamTransactionMetadataTasks.close();
                log.debug("StreamTransactionMetadataTasks closed.");
            }

            // Await termination of all services
            if (restServer != null) {
                restServer.awaitTerminated();
                log.info("REST Server shutdown.");
            }

            if (grpcServer != null) {
                grpcServer.awaitTerminated();
                log.info("GRPC Server shutdown.");
            }

            if (controllerEventProcessors != null) {
                controllerEventProcessors.awaitTerminated();
                log.info("Controller Event Processors shutdown.");
            }

            if (monitor != null) {
                monitor.awaitTerminated();
                log.info("Segment Container Monitor shutdown.");
            }

            if (controllerClusterListener != null) {
                controllerClusterListener.awaitTerminated();
                log.info("Controller Cluster Listener shutdown.");
            }

            if (retentionService != null) {
                retentionService.awaitTerminated();
                log.info("Retention service shutdown.");
            }

            if (watermarkingService != null) {
                watermarkingService.awaitTerminated();
                log.info("Watermarking service shutdown.");
            }
        } catch (Exception e) {
            log.error("Controller Service Starter threw exception during shutdown", e);
            throw e;
        } finally {
            // We will stop our executors in `finally` so that even if an exception is thrown, we are not left with
            // lingering threads that prevent our process from exiting.

            // Next stop all executors
            log.debug("Shutting down executor thread pools....");
            ExecutorServiceHelpers.shutdown(Duration.ofSeconds(5), controllerExecutor, retentionExecutor, watermarkingExecutor, eventExecutor);

            if (cluster != null) {
                cluster.close();
                log.debug("Closed controller cluster instance.");
            }

            if (segmentHelper != null) {
                segmentHelper.close();
                log.debug("Closed segment helper.");
            }

            close(kvtMetadataStore);
            close(kvtMetadataTasks);
            log.debug("Closed KVT Store and Tasks.");
            close(connectionPool);
            log.debug("Closed connection pool.");
            close(connectionFactory);
            log.debug("Closed connection factory.");
            close(storeClient);
            log.debug("Closing storeClient");
            close(streamStore);
            log.debug("Closed stream store.");
            close(controllerEventProcessors);
            log.debug("Closed controllerEventProcessors.");

            // Close metrics.
            StreamMetrics.reset();
            TransactionMetrics.reset();

            log.info("Completed controller service shutDown.");
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }

    void notifySessionExpiration() {
        assert storeClient.getType().equals(StoreType.Zookeeper) || storeClient.getType().equals(StoreType.PravegaTable);
        storeClientFailureFuture.completeExceptionally(new ZkSessionExpirationException("Zookeeper Session Expired"));
    }

    @VisibleForTesting
    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        controllerReadyLatch.await();
        return this.streamTransactionMetadataTasks.awaitInitialization(timeout, timeUnit);
    }

    @VisibleForTesting
    public ControllerService getControllerService() throws InterruptedException {
        controllerReadyLatch.await();
        return this.controllerService;
    }

    @VisibleForTesting
    public LocalController getController() throws InterruptedException {
        controllerReadyLatch.await();
        return this.localController;
    }

    private void setController(LocalController controller) {
        this.localController = controller;
        controllerReadyLatch.countDown();
    }

    private String getHostName() {
        String hostName = null;
        if (serviceConfig.getGRPCServerConfig().isPresent()) {
            hostName = serviceConfig.getGRPCServerConfig().get().getPublishedRPCHost().orElse(null);
        }

        if (StringUtils.isEmpty(hostName)) {
            try {
                hostName = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                log.warn("Failed to get host address, defaulting to localhost: {}", e);
                hostName = "localhost";
            }
        }
        return hostName;
    }

    private int getPort() {
        int port = 0;
        if (serviceConfig.getGRPCServerConfig().isPresent()) {
            port = serviceConfig.getGRPCServerConfig().get().getPublishedRPCPort().orElse(
                    serviceConfig.getGRPCServerConfig().get().getPort());
        }
        return port;
    }

    @Override
    public void close() {
        Callbacks.invokeSafely(() -> {
            stopAsync().awaitTerminated(serviceConfig.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }, ex -> log.error("Exception while forcefully shutting down.", ex));
        close(watermarkingWork);
        close(streamMetadataTasks);
        close(streamTransactionMetadataTasks);
        close(controllerEventProcessors);
        ExecutorServiceHelpers.shutdown(serviceConfig.getShutdownTimeout(), controllerExecutor, retentionExecutor, watermarkingExecutor, eventExecutor);
        close(cluster);
        close(segmentHelper);
        close(kvtMetadataStore);
        close(kvtMetadataTasks);
        close(connectionPool);
        close(connectionFactory);
        close(storeClient);
        close(streamStore);
        close(healthServiceManager);
    }

    private void close(AutoCloseable closeable) {
        if (closeable != null) {
            Callbacks.invokeSafely(closeable::close, null);
        }
    }
}
