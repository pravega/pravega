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
import io.pravega.controller.server.rest.RESTServer;
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
            
            log.info("Creating the bucket store");
            bucketStore = StreamStoreFactory.createBucketStore(storeClient, controllerExecutor);

            log.info("Creating the task store");
            taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor);

            log.info("Creating the host store");
            hostStore = HostStoreFactory.createStore(serviceConfig.getHostMonitorConfig(), storeClient);

            log.info("Creating the checkpoint store");
            checkpointStore = CheckpointStoreFactory.create(storeClient);

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

            if (serviceConfig.getHostMonitorConfig().isHostMonitorEnabled()) {
                //Start the Segment Container Monitor.
                monitor = new SegmentContainerMonitor(hostStore, (CuratorFramework) storeClient.getClient(),
                        new UniformContainerBalancer(),
                        serviceConfig.getHostMonitorConfig().getHostMonitorMinRebalanceInterval());
                log.info("Starting segment container monitor");
                monitor.startAsync();
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
            
            ClientConfig clientConfig = clientConfigBuilder.build();
            connectionFactory = connectionFactoryRef.orElseGet(() -> new SocketConnectionFactoryImpl(clientConfig));
            connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
            segmentHelper = segmentHelperRef.orElseGet(() -> new SegmentHelper(connectionPool, hostStore, controllerExecutor));

            GrpcAuthHelper authHelper = new GrpcAuthHelper(serviceConfig.getGRPCServerConfig().get().isAuthorizationEnabled(),
                                                           grpcServerConfig.getTokenSigningKey(),
                                                           grpcServerConfig.getAccessTokenTTLInSeconds());

            log.info("Creating the stream store");
            streamStore = streamMetadataStoreRef.orElseGet(() -> StreamStoreFactory.createStore(storeClient, segmentHelper, authHelper, controllerExecutor));

            streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore,
                    segmentHelper, controllerExecutor, eventExecutor, host.getHostId(), authHelper, requestTracker, 
                    serviceConfig.getRetentionFrequency().toMillis());
            streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                    segmentHelper, controllerExecutor, eventExecutor, host.getHostId(), serviceConfig.getTimeoutServiceConfig(), authHelper);

            BucketServiceFactory bucketServiceFactory = new BucketServiceFactory(host.getHostId(), bucketStore, 1000);
            Duration executionDurationRetention = serviceConfig.getRetentionFrequency();

            PeriodicRetention retentionWork = new PeriodicRetention(streamStore, streamMetadataTasks, retentionExecutor, requestTracker);
            retentionService = bucketServiceFactory.createRetentionService(executionDurationRetention, retentionWork::retention, retentionExecutor);

            log.info("starting background periodic service for retention");
            retentionService.startAsync();
            retentionService.awaitRunning();

            Duration executionDurationWatermarking = Duration.ofSeconds(Config.MINIMUM_WATERMARKING_FREQUENCY_IN_SECONDS);
            watermarkingWork = new PeriodicWatermarking(streamStore, bucketStore,
                    clientConfig, watermarkingExecutor);
            watermarkingService = bucketServiceFactory.createWatermarkingService(executionDurationWatermarking, 
                    watermarkingWork::watermark, watermarkingExecutor);

            log.info("starting background periodic service for watermarking");
            watermarkingService.startAsync();
            watermarkingService.awaitRunning();

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

            kvtMetadataStore = kvtMetaStoreRef.orElseGet(() -> KVTableStoreFactory.createStore(storeClient, segmentHelper, authHelper, controllerExecutor, streamStore));
            kvtMetadataTasks = new TableMetadataTasks(kvtMetadataStore, segmentHelper, controllerExecutor, eventExecutor, host.getHostId(), authHelper, requestTracker);
            controllerService = new ControllerService(kvtMetadataStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                    streamTransactionMetadataTasks, segmentHelper, controllerExecutor, cluster);

            // Setup event processors.
            setController(new LocalController(controllerService, grpcServerConfig.isAuthorizationEnabled(),
                    grpcServerConfig.getTokenSigningKey()));

            CompletableFuture<Void> eventProcessorFuture = CompletableFuture.completedFuture(null); 
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // Create ControllerEventProcessor object.
                controllerEventProcessors = new ControllerEventProcessors(host.getHostId(),
                        serviceConfig.getEventProcessorConfig().get(), localController, checkpointStore, streamStore,
                        bucketStore, connectionPool, streamMetadataTasks, streamTransactionMetadataTasks, kvtMetadataStore, kvtMetadataTasks,
                        eventExecutor);

                // Bootstrap and start it asynchronously.
                log.info("Starting event processors");
                eventProcessorFuture = controllerEventProcessors.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks, kvtMetadataTasks)
                        .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventExecutor);
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

                log.info("Starting controller cluster listener");
                controllerClusterListener.startAsync();
            }

            // Start RPC server.
            if (serviceConfig.getGRPCServerConfig().isPresent()) {
                grpcServer = new GRPCServer(controllerService, grpcServerConfig, requestTracker);
                grpcServer.startAsync();
                log.info("Awaiting start of rpc server");
                grpcServer.awaitRunning();
            }

            // Start REST server.
            if (serviceConfig.getRestServerConfig().isPresent()) {
                restServer = new RESTServer(this.localController,
                        controllerService,
                        grpcServer.getAuthHandlerManager(),
                        serviceConfig.getRestServerConfig().get(),
                        connectionFactory);
                restServer.startAsync();
                log.info("Awaiting start of REST server");
                restServer.awaitRunning();
            }

            // Wait for controller event processors to start.
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                log.info("Awaiting start of controller event processors");
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
                log.info("Awaiting start of controller cluster listener");
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
        log.info("Initiating controller service shutDown");

        try {
            if (restServer != null) {
                restServer.stopAsync();
            }
            if (grpcServer != null) {
                grpcServer.stopAsync();
            }
            if (controllerEventProcessors != null) {
                log.info("Stopping controller event processors");
                controllerEventProcessors.stopAsync();
            }
            if (monitor != null) {
                log.info("Stopping the segment container monitor");
                monitor.stopAsync();
            }
            if (controllerClusterListener != null) {
                log.info("Stopping controller cluster listener");
                controllerClusterListener.stopAsync();
                log.info("Controller cluster listener shutdown");
            }

            if (retentionService != null) {
                log.info("Stopping auto retention service");
                retentionService.stopAsync();
            }

            if (watermarkingService != null) {
                log.info("Stopping watermarking service");
                watermarkingService.stopAsync();
            }

            close(watermarkingWork);

            if (streamMetadataTasks != null) {
                log.info("Closing stream metadata tasks");
                streamMetadataTasks.close();
            }

            if (streamTransactionMetadataTasks != null) {
                log.info("Closing stream transaction metadata tasks");
                streamTransactionMetadataTasks.close();
            }

            // Await termination of all services
            if (restServer != null) {
                log.info("Awaiting termination of REST server");
                restServer.awaitTerminated();
            }

            if (grpcServer != null) {
                log.info("Awaiting termination of gRPC server");
                grpcServer.awaitTerminated();
            }

            if (controllerEventProcessors != null) {
                log.info("Awaiting termination of controller event processors");
                controllerEventProcessors.awaitTerminated();
            }

            if (monitor != null) {
                log.info("Awaiting termination of segment container monitor");
                monitor.awaitTerminated();
            }

            if (controllerClusterListener != null) {
                log.info("Awaiting termination of controller cluster listener");
                controllerClusterListener.awaitTerminated();
            }

            if (retentionService != null) {
                log.info("Awaiting termination of auto retention");
                retentionService.awaitTerminated();
            }

            if (watermarkingService != null) {
                log.info("Awaiting termination of watermarking service");
                watermarkingService.awaitTerminated();
            }
        } catch (Exception e) {
            log.error("Controller Service Starter threw exception during shutdown", e);
            throw e;
        } finally {
            // We will stop our executors in `finally` so that even if an exception is thrown, we are not left with
            // lingering threads that prevent our process from exiting.

            // Next stop all executors
            log.info("Stopping controller executors");
            ExecutorServiceHelpers.shutdown(Duration.ofSeconds(5), controllerExecutor, retentionExecutor, watermarkingExecutor, eventExecutor);

            if (cluster != null) {
                log.info("Closing controller cluster instance");
                cluster.close();
            }

            if (segmentHelper != null) {
                log.info("closing segment helper");
                segmentHelper.close();
            }

            close(kvtMetadataStore);
            close(kvtMetadataTasks);

            log.info("Closing connection pool");
            close(connectionPool);

            log.info("Closing connection factory");
            close(connectionFactory);

            log.info("Closing storeClient");
            close(storeClient);

            log.info("Closing store");
            close(streamStore);

            close(controllerEventProcessors);

            // Close metrics.
            StreamMetrics.reset();
            TransactionMetrics.reset();

            log.info("Finishing controller service shutDown");
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
        Callbacks.invokeSafely(() -> stopAsync().awaitTerminated(serviceConfig.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS), ex -> log.error("Exception while forcefully shutting down.", ex));
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
    }

    private void close(AutoCloseable closeable) {
        if (closeable != null) {
            Callbacks.invokeSafely(closeable::close, null);
        }
    }
}
