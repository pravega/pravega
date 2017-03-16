/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.requesthandler.RequestHandlersInit;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rpc.grpc.GRPCServer;
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
import com.emc.pravega.stream.impl.Controller;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractService {
    private final ControllerServiceConfig serviceConfig;

    private ScheduledExecutorService controllerServiceExecutor;
    private ScheduledExecutorService taskExecutor;
    private ScheduledExecutorService storeExecutor;
    private ScheduledExecutorService requestExecutor;
    private ScheduledExecutorService eventProcExecutor;

    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;

    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private SegmentContainerMonitor monitor;

    private TimeoutService timeoutService;
    private ControllerService controllerService;

    private LocalController localController;
    private ControllerEventProcessors controllerEventProcessors;

    private GRPCServer grpcServer;
    private RESTServer restServer;

    public ControllerServiceStarter(ControllerServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;
    }

    @Override
    protected void doStart() {
        log.info("Initiating controller services startup");

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
                segmentHelper, taskExecutor, serviceConfig.getHost());
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, segmentHelper, taskExecutor, serviceConfig.getHost());
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                serviceConfig.getTimeoutServiceConfig());
        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, new SegmentHelper(), controllerServiceExecutor);

        // Setup event processors.
        localController = new LocalController(controllerService);

        if (serviceConfig.getEventProcessorConfig().isPresent()) {
            // Create ControllerEventProcessor object.
            controllerEventProcessors = new ControllerEventProcessors(serviceConfig.getHost(),
                    serviceConfig.getEventProcessorConfig().get(), localController, storeClient, streamStore,
                    hostStore, segmentHelper, eventProcExecutor);

            // Bootstrap and start it asynchronously.
            ControllerEventProcessors.bootstrap(localController, serviceConfig.getEventProcessorConfig().get(),
                    streamTransactionMetadataTasks, eventProcExecutor)
                    .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventProcExecutor);
        }

        // Start request handlers
        if (serviceConfig.isRequestHandlersEnabled()) {
            RequestHandlersInit.bootstrapRequestHandlers(controllerService, streamStore, requestExecutor);
        }

        // Start RPC server.
        if (serviceConfig.getGRPCServerConfig().isPresent()) {
            grpcServer = new GRPCServer(controllerService, serviceConfig.getGRPCServerConfig().get());
            grpcServer.startAsync();
            // After completion of startAsync method, server is expected to be in RUNNING state.
            // If it is not in running state, we return.
            if (!grpcServer.isRunning()) {
                log.error("RPC server failed to start, state = {} ", grpcServer.state());
                throw new RuntimeException("Error starting gRPC server");
            }
        }

        // Start REST server.
        if (serviceConfig.getRestServerConfig().isPresent()) {
            restServer = new RESTServer(controllerService, serviceConfig.getRestServerConfig().get());
            restServer.startAsync();
            // After completion of startAsync method, server is expected to be in RUNNING state.
            // If it is not in running state, we return.
            if (!restServer.isRunning()) {
                log.error("REST server failed to start, state = {} ", restServer.state());
                throw new RuntimeException("Error starting REST server");
            }
        }

        // Hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure.
        // todo: hook up TaskSweeper.sweepOrphanedTasks with Failover support feature
        // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
        // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
        // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
        // are processed and deleted, that failed HostId is removed from FH folder.
        // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
        // controllers and starts sweeping tasks orphaned by those hostIds.
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, serviceConfig.getHost(), streamMetadataTasks,
                streamTransactionMetadataTasks);

        notifyStarted();
    }

    @Override
    protected void doStop() {
        log.info("Initiating controller services shutdown");

        if (restServer != null) {
            log.info("Stopping the segment container monitor");
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
        timeoutService.stopAsync();

        // Finally stop all executors
        log.info("Stopping executors");
        eventProcExecutor.shutdown();
        requestExecutor.shutdown();
        storeExecutor.shutdown();
        taskExecutor.shutdown();
        controllerServiceExecutor.shutdown();
    }

    public boolean awaitTasksModuleInitialization(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.streamTransactionMetadataTasks.awaitInitialization(timeout, timeUnit);
    }

    public ControllerService getControllerService() {
        return this.controllerService;
    }

    public Controller getController() {
        return this.localController;
    }
}
