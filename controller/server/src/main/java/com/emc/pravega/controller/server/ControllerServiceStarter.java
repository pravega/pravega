/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServer;
import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import com.emc.pravega.controller.store.client.StoreClient;
import com.emc.pravega.controller.store.client.StoreClientFactory;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractIdleService {
    private final ControllerServiceConfig serviceConfig;
    private final String objectId;

    private ScheduledExecutorService controllerServiceExecutor;
    private ScheduledExecutorService taskExecutor;
    private ScheduledExecutorService storeExecutor;
    private ScheduledExecutorService requestExecutor;
    private ScheduledExecutorService eventProcExecutor;

    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private CheckpointStore checkpointStore;

    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private SegmentContainerMonitor monitor;

    private TimeoutService timeoutService;
    private ControllerService controllerService;

    private LocalController localController;
    private CountDownLatch controllerReadyLatch;
    private ControllerEventProcessors controllerEventProcessors;

    private GRPCServer grpcServer;
    private RESTServer restServer;

    public ControllerServiceStarter(ControllerServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
        this.objectId = "ControllerServiceStarter";
        this.controllerReadyLatch = new CountDownLatch(1);
    }

    @Override
    protected void startUp() {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "startUp");
        log.info("Initiating controller service startUp");
        log.info("Event processors enabled = {}", serviceConfig.getEventProcessorConfig().isPresent());
        log.info("Request handlers enabled = {}", serviceConfig.isRequestHandlersEnabled());
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

            log.info("Creating store client");
            StoreClient storeClient = StoreClientFactory.createStoreClient(serviceConfig.getStoreClientConfig());

            log.info("Creating the stream store");
            streamStore = StreamStoreFactory.createStore(storeClient, storeExecutor);

            log.info("Creating the task store");
            taskMetadataStore = TaskStoreFactory.createStore(storeClient, taskExecutor);

            log.info("Creating the host store");
            hostStore = HostStoreFactory.createStore(serviceConfig.getHostMonitorConfig(), storeClient);

            log.info("Creating the checkpoint store");
            checkpointStore = CheckpointStoreFactory.create(storeClient);

            String hostId;
            try {
                //On each controller process restart, it gets a fresh hostId,
                //which is a combination of hostname and random GUID.
                hostId = InetAddress.getLocalHost().getHostAddress() + "-" + UUID.randomUUID().toString();
            } catch (UnknownHostException e) {
                log.warn("Failed to get host address.", e);
                hostId = UUID.randomUUID().toString();
            }

            if (serviceConfig.getHostMonitorConfig().isHostMonitorEnabled()) {
                //Start the Segment Container Monitor.
                monitor = new SegmentContainerMonitor(hostStore, (CuratorFramework) storeClient.getClient(),
                        new UniformContainerBalancer(),
                        serviceConfig.getHostMonitorConfig().getHostMonitorMinRebalanceInterval());
                log.info("Starting segment container monitor");
                monitor.startAsync();
            }

            SegmentHelper segmentHelper = new SegmentHelper();
            streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                    segmentHelper, taskExecutor, hostId);
            streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                    hostStore, taskMetadataStore, segmentHelper, taskExecutor, hostId);
            timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                    serviceConfig.getTimeoutServiceConfig());
            controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                    streamTransactionMetadataTasks, timeoutService, new SegmentHelper(), controllerServiceExecutor);

            // Setup event processors.
            setController(new LocalController(controllerService));

            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // Create ControllerEventProcessor object.
                controllerEventProcessors = new ControllerEventProcessors(hostId,
                        serviceConfig.getEventProcessorConfig().get(), localController, checkpointStore, streamStore,
                        hostStore, segmentHelper, eventProcExecutor);

                // Bootstrap and start it asynchronously.
                log.info("Starting event processors");
                ControllerEventProcessors.bootstrap(localController, serviceConfig.getEventProcessorConfig().get(),
                        streamTransactionMetadataTasks, eventProcExecutor)
                        .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventProcExecutor);
            }

            // Start request handlers
            if (serviceConfig.isRequestHandlersEnabled()) {
                log.info("Starting request handlers");
                RequestHandlersInit.bootstrapRequestHandlers(controllerService, streamStore, requestExecutor);
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

            // Finally wait for controller event processors to start
            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                log.info("Awaiting start of controller event processors");
                controllerEventProcessors.awaitRunning();
            }
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, this.objectId, "shutDown");
        log.info("Initiating controller service shutDown");

        try {
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
                log.info("Stopping the segment container monitor");
                monitor.stopAsync();
            }
            timeoutService.stopAsync();

            // Next stop all executors
            log.info("Stopping executors");
            eventProcExecutor.shutdown();
            requestExecutor.shutdown();
            storeExecutor.shutdown();
            taskExecutor.shutdown();
            controllerServiceExecutor.shutdown();

            // Finally, await termination of all services
            if (restServer != null) {
                log.info("Awaiting termination of REST server");
                restServer.awaitTerminated();
            }

            if (grpcServer != null) {
                log.info("Awaiting termination of rpc server");
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
        } finally {
            LoggerHelpers.traceLeave(log, this.objectId, "shutDown", traceId);
        }
    }

    @VisibleForTesting
    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.streamTransactionMetadataTasks.awaitInitialization(timeout, timeUnit);
    }

    @VisibleForTesting
    public ControllerService getControllerService() {
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
}
