/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterType;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.fault.ControllerClusterListener;
import com.emc.pravega.controller.fault.ControllerClusterListenerConfig;
import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.server.eventProcessor.RequestHandlers;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServer;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.timeout.TimeoutService;
import com.emc.pravega.controller.timeout.TimerWheelTimeoutService;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractIdleService {
    private final ControllerServiceConfig serviceConfig;
    private final StoreClient storeClient;
    private final String objectId;

    private ScheduledExecutorService controllerServiceExecutor;
    private ScheduledExecutorService taskExecutor;
    private ScheduledExecutorService storeExecutor;
    private ScheduledExecutorService requestExecutor;
    private ScheduledExecutorService eventProcExecutor;
    private ExecutorService clusterListenerExecutor;

    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private CheckpointStore checkpointStore;

    private ConnectionFactory connectionFactory;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private SegmentContainerMonitor monitor;
    private ControllerClusterListener controllerClusterListener;

    private TimeoutService timeoutService;
    private ControllerService controllerService;

    private LocalController localController;
    private ControllerEventProcessors controllerEventProcessors;
    private RequestHandlers requestHandlers;
    /**
     * ControllerReadyLatch is released once localController, streamTransactionMetadataTasks and controllerService
     * variables are initialized in the startUp method.
     */
    private final CountDownLatch controllerReadyLatch;

    private GRPCServer grpcServer;
    private RESTServer restServer;

    private Cluster cluster = null;

    public ControllerServiceStarter(ControllerServiceConfig serviceConfig, StoreClient storeClient) {
        this.serviceConfig = serviceConfig;
        this.storeClient = storeClient;
        this.objectId = "ControllerServiceStarter";
        this.controllerReadyLatch = new CountDownLatch(1);
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.objectId, "startUp");
        log.info("Initiating controller service startUp");
        log.info("Event processors enabled = {}", serviceConfig.getEventProcessorConfig().isPresent());
        log.info("Request handlers enabled = {}", serviceConfig.isRequestHandlersEnabled());
        log.info("Cluster listener enabled = {}", serviceConfig.getControllerClusterListenerConfig().isPresent());
        log.info("    Host monitor enabled = {}", serviceConfig.getHostMonitorConfig().isHostMonitorEnabled());
        log.info("     gRPC server enabled = {}", serviceConfig.getGRPCServerConfig().isPresent());
        log.info("     REST server enabled = {}", serviceConfig.getRestServerConfig().isPresent());

        try {
            //Initialize the executor service.
            controllerServiceExecutor = Executors.newScheduledThreadPool(serviceConfig.getServiceThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("servicepool-%d").build());

            taskExecutor = Executors.newScheduledThreadPool(serviceConfig.getTaskThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

            storeExecutor = Executors.newScheduledThreadPool(serviceConfig.getStoreThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("storepool-%d").build());

            requestExecutor = Executors.newScheduledThreadPool(serviceConfig.getRequestHandlerThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("requestpool-%d").build());

            eventProcExecutor = Executors.newScheduledThreadPool(serviceConfig.getEventProcThreadPoolSize(),
                    new ThreadFactoryBuilder().setNameFormat("eventprocpool-%d").build());

            log.info("Creating the stream store");
            streamStore = StreamStoreFactory.createStore(storeClient, storeExecutor);

            log.info("Creating the task store");
            taskMetadataStore = TaskStoreFactory.createStore(storeClient, taskExecutor);

            log.info("Creating the host store");
            hostStore = HostStoreFactory.createStore(serviceConfig.getHostMonitorConfig(), storeClient);

            log.info("Creating the checkpoint store");
            checkpointStore = CheckpointStoreFactory.create(storeClient);

            // On each controller process restart, we use a fresh hostId,
            // which is a combination of hostname and random GUID.
            String hostName = getHostName();
            Host host = new Host(hostName, getPort(), UUID.randomUUID().toString());

            if (serviceConfig.getHostMonitorConfig().isHostMonitorEnabled()) {
                //Start the Segment Container Monitor.
                monitor = new SegmentContainerMonitor(hostStore, (CuratorFramework) storeClient.getClient(),
                        new UniformContainerBalancer(),
                        serviceConfig.getHostMonitorConfig().getHostMonitorMinRebalanceInterval());
                log.info("Starting segment container monitor");
                monitor.startAsync();
            }

            connectionFactory = new ConnectionFactoryImpl(false);
            SegmentHelper segmentHelper = new SegmentHelper();
            streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                    segmentHelper, taskExecutor, host.getHostId(), connectionFactory);
            streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                    hostStore, taskMetadataStore, segmentHelper, taskExecutor, host.getHostId(), connectionFactory);
            timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                    serviceConfig.getTimeoutServiceConfig());

            // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
            // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
            // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
            // are processed and deleted, that failed HostId is removed from FH folder.
            // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
            // controllers and starts sweeping tasks orphaned by those hostIds.
            TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, host.getHostId(), taskExecutor,
                    streamMetadataTasks, streamTransactionMetadataTasks);

            // Setup and start controller cluster listener.
            if (serviceConfig.getControllerClusterListenerConfig().isPresent()) {
                ControllerClusterListenerConfig controllerClusterListenerConfig = serviceConfig.getControllerClusterListenerConfig().get();
                clusterListenerExecutor = new ThreadPoolExecutor(controllerClusterListenerConfig.getMinThreads(),
                        controllerClusterListenerConfig.getMaxThreads(), controllerClusterListenerConfig.getIdleTime(),
                        controllerClusterListenerConfig.getIdleTimeUnit(),
                        new ArrayBlockingQueue<>(controllerClusterListenerConfig.getMaxQueueSize()),
                        new ThreadFactoryBuilder().setNameFormat("clusterlistenerpool-%d").build());

                cluster = new ClusterZKImpl((CuratorFramework) storeClient.getClient(), ClusterType.CONTROLLER);
                controllerClusterListener = new ControllerClusterListener(host, cluster,
                        Optional.ofNullable(controllerEventProcessors),
                        Optional.ofNullable(requestHandlers),
                        taskSweeper, clusterListenerExecutor);

                log.info("Starting controller cluster listener");
                controllerClusterListener.startAsync();
            }

            controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                    streamTransactionMetadataTasks, timeoutService, new SegmentHelper(), controllerServiceExecutor,
                    cluster);

            // Setup event processors.
            setController(new LocalController(controllerService));

            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // Create ControllerEventProcessor object.
                controllerEventProcessors = new ControllerEventProcessors(host.getHostId(),
                        serviceConfig.getEventProcessorConfig().get(), localController, checkpointStore, streamStore,
                        hostStore, segmentHelper, connectionFactory, eventProcExecutor);

                // Bootstrap and start it asynchronously.
                log.info("Starting event processors");
                controllerEventProcessors.bootstrap(streamTransactionMetadataTasks)
                        .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventProcExecutor);
            }

            // Start RPC server.
            if (serviceConfig.getGRPCServerConfig().isPresent()) {
                grpcServer = new GRPCServer(controllerService, serviceConfig.getGRPCServerConfig().get());
                grpcServer.startAsync();
                log.info("Awaiting start of rpc server");
                grpcServer.awaitRunning();
            }

            // Start REST server.
            if (serviceConfig.getRestServerConfig().isPresent()) {
                restServer = new RESTServer(controllerService, serviceConfig.getRestServerConfig().get());
                restServer.startAsync();
                log.info("Awaiting start of REST server");
                restServer.awaitRunning();
            }

            // Wait for controller event processors to start.
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                log.info("Awaiting start of controller event processors");
                controllerEventProcessors.awaitRunning();
            }

            // Wait for controller cluster listeners to start.
            if (serviceConfig.getControllerClusterListenerConfig().isPresent()) {
                log.info("Awaiting start of controller cluster listener");
                controllerClusterListener.awaitRunning();
            }

            // Start request handlers
            if (serviceConfig.isRequestHandlersEnabled()) {

                log.info("Starting request handlers");
                requestHandlers = new RequestHandlers(controllerService,
                        checkpointStore, host.getHostId(), requestExecutor);
                requestHandlers.startAsync();
                log.info("Awaiting start of request handlers");
                requestHandlers.awaitRunning();
                log.info("Done starting of request handlers");
            }
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
            if (serviceConfig.isRequestHandlersEnabled()) {
                log.info("Shutting down request handlers");
                requestHandlers.stopAsync();
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
            timeoutService.stopAsync();

            log.info("Closing stream metadata tasks");
            streamMetadataTasks.close();

            log.info("Closing stream transaction metadata tasks");
            streamTransactionMetadataTasks.close();

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

            // Next stop all executors
            log.info("Stopping executors");
            eventProcExecutor.shutdownNow();
            requestExecutor.shutdownNow();
            storeExecutor.shutdownNow();
            taskExecutor.shutdownNow();
            controllerServiceExecutor.shutdownNow();
            if (clusterListenerExecutor != null) {
                clusterListenerExecutor.shutdownNow();
            }

            log.info("Awaiting termination of eventProc executor");
            eventProcExecutor.awaitTermination(5, TimeUnit.SECONDS);

            log.info("Awaiting termination of requestHandler executor");
            requestExecutor.awaitTermination(5, TimeUnit.SECONDS);

            log.info("Awaiting termination of store executor");
            storeExecutor.awaitTermination(5, TimeUnit.SECONDS);

            log.info("Awaiting termination of task executor");
            taskExecutor.awaitTermination(5, TimeUnit.SECONDS);

            log.info("Awaiting termination of controllerService executor");
            controllerServiceExecutor.awaitTermination(5, TimeUnit.SECONDS);

            if (clusterListenerExecutor != null) {
                log.info("Awaiting termination of controller cluster listener executor");
                clusterListenerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }

            if (cluster != null) {
                log.info("Closing controller cluster instance");
                cluster.close();
            }

            log.info("Closing connection factory");
            connectionFactory.close();

            log.info("Closing storeClient");
            storeClient.close();
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
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
            hostName = serviceConfig.getGRPCServerConfig().get().getPublishedRPCHost();
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
            port = serviceConfig.getGRPCServerConfig().get().getPublishedRPCPort();
        }
        return port;
    }
}
