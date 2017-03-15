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
import com.emc.pravega.controller.util.ZKUtils;
import com.emc.pravega.stream.impl.Controller;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractService {
    private final ControllerServiceConfig serviceConfig;

    private final ScheduledExecutorService controllerServiceExecutor;
    private final ScheduledExecutorService taskExecutor;
    private final ScheduledExecutorService storeExecutor;
    private final ScheduledExecutorService requestExecutor;
    private final ScheduledExecutorService eventProcExecutor;

    private final StreamMetadataStore streamStore;
    private final TaskMetadataStore taskMetadataStore;
    private final HostControllerStore hostStore;

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final SegmentContainerMonitor monitor;

    private final TimeoutService timeoutService;
    private final ControllerService controllerService;

    private final LocalController localController;
    private final ControllerEventProcessors controllerEventProcessors;

    private final GRPCServer grpcServer;
    private final RESTServer restServer;

    public ControllerServiceStarter(ControllerServiceConfig serviceConfig) {
        this.serviceConfig = serviceConfig;

        //Initialize the executor service.
        controllerServiceExecutor = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("servicepool-%d").build());

        taskExecutor = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        storeExecutor = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("storepool-%d").build());

        requestExecutor = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize() / 2,
                new ThreadFactoryBuilder().setNameFormat("requestpool-%d").build());

        eventProcExecutor = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize() / 2,
                new ThreadFactoryBuilder().setNameFormat("eventprocpool-%d").build());

        log.info("Creating the stream store");
        streamStore = StreamStoreFactory.createStore(serviceConfig.getStoreClient(), storeExecutor);

        log.info("Creating the task store");
        taskMetadataStore = TaskStoreFactory.createStore(serviceConfig.getStoreClient(), taskExecutor);

        if (serviceConfig.getHostMonitorConfig().isHostMonitorEnabled()) {
            //Host monitor is not required for a single node local setup.

            log.info("Creating the host store");
            hostStore = HostStoreFactory.createStore(serviceConfig.getStoreClient());

            //Start the Segment Container Monitor.
            monitor = new SegmentContainerMonitor(hostStore, ZKUtils.getCuratorClient(), new UniformContainerBalancer(),
                    serviceConfig.getHostMonitorConfig().getHostMonitorMinRebalanceInterval());
            monitor.startAsync();
        } else {
            log.info("Creating in-memory host store");
            hostStore = HostStoreFactory.createInMemoryStore(
                    serviceConfig.getHostMonitorConfig().getSssHost(),
                    serviceConfig.getHostMonitorConfig().getSssPort(),
                    serviceConfig.getHostMonitorConfig().getContainerCount());
            monitor = null;
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

        if (serviceConfig.isEventProcessorsEnabled()) {
            controllerEventProcessors = new ControllerEventProcessors(serviceConfig.getHost(), localController,
                    serviceConfig.getStoreClient(), streamStore, hostStore, segmentHelper, eventProcExecutor);
        } else {
            controllerEventProcessors = null;
        }

        // Setup RPC server.
        if (serviceConfig.isGRPCServerEnabled()) {
            grpcServer = new GRPCServer(controllerService, serviceConfig.getGRPCServerConfig());
        } else {
            grpcServer = null;
        }

        // Setup REST server.
        if (serviceConfig.isRestServerEnabled()) {
            restServer = new RESTServer(controllerService, serviceConfig.getRestServerConfig());
        } else {
            restServer = null;
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
    }

    @Override
    protected void doStart() {
        log.info("Initiating controller services startup");
        if (monitor != null) {
            log.info("Starting the segment container monitor");
            monitor.startAsync();
        }
        if (controllerEventProcessors != null) {
            ControllerEventProcessors.bootstrap(localController, streamTransactionMetadataTasks, eventProcExecutor)
                    .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), eventProcExecutor);
        }
        if (serviceConfig.isRequestHandlersEnabled()) {
            RequestHandlersInit.bootstrapRequestHandlers(controllerService, streamStore, requestExecutor);
        }
        if (grpcServer != null) {
            grpcServer.startAsync();
            // After completion of startAsync method, server is expected to be in RUNNING state.
            // If it is not in running state, we return.
            if (!grpcServer.isRunning()) {
                log.error("RPC server failed to start, state = {} ", grpcServer.state());
                throw new RuntimeException("Error starting gRPC server");
            }
        }
        if (restServer != null) {
            restServer.startAsync();
            // After completion of startAsync method, server is expected to be in RUNNING state.
            // If it is not in running state, we return.
            if (!restServer.isRunning()) {
                log.error("REST server failed to start, state = {} ", restServer.state());
                throw new RuntimeException("Error starting REST server");
            }
        }
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
