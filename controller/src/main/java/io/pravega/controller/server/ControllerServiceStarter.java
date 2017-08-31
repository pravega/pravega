/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.fault.ControllerClusterListener;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.fault.SegmentContainerMonitor;
import io.pravega.controller.fault.UniformContainerBalancer;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.RESTServer;
import io.pravega.controller.server.rpc.grpc.GRPCServer;
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.TaskSweeper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.task.Stream.TxnSweeper;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;

/**
 * Creates the controller service, given the service configuration.
 */
@Slf4j
public class ControllerServiceStarter extends AbstractIdleService {
    private final ControllerServiceConfig serviceConfig;
    private final StoreClient storeClient;
    private final String objectId;

    private ScheduledExecutorService controllerExecutor;

    private ConnectionFactory connectionFactory;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private SegmentContainerMonitor monitor;
    private ControllerClusterListener controllerClusterListener;

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
        log.info("Cluster listener enabled = {}", serviceConfig.isControllerClusterListenerEnabled());
        log.info("    Host monitor enabled = {}", serviceConfig.getHostMonitorConfig().isHostMonitorEnabled());
        log.info("     gRPC server enabled = {}", serviceConfig.getGRPCServerConfig().isPresent());
        log.info("     REST server enabled = {}", serviceConfig.getRestServerConfig().isPresent());

        final StreamMetadataStore streamStore;
        final TaskMetadataStore taskMetadataStore;
        final HostControllerStore hostStore;
        final CheckpointStore checkpointStore;

        try {
            //Initialize the executor service.
            controllerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(serviceConfig.getThreadPoolSize(),
                                                                               "controllerpool");

            log.info("Creating the stream store");
            streamStore = StreamStoreFactory.createStore(storeClient, controllerExecutor);

            log.info("Creating the task store");
            taskMetadataStore = TaskStoreFactory.createStore(storeClient, controllerExecutor);

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
                    segmentHelper, controllerExecutor, host.getHostId(), connectionFactory);
            streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                    hostStore, segmentHelper, controllerExecutor, host.getHostId(), connectionFactory);

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

            if (serviceConfig.isControllerClusterListenerEnabled()) {
                cluster = new ClusterZKImpl((CuratorFramework) storeClient.getClient(), ClusterType.CONTROLLER);
            }

            controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                    streamTransactionMetadataTasks, new SegmentHelper(), controllerExecutor, cluster);

            // Setup event processors.
            setController(new LocalController(controllerService));

            if (serviceConfig.getEventProcessorConfig().isPresent()) {
                // Create ControllerEventProcessor object.
                controllerEventProcessors = new ControllerEventProcessors(host.getHostId(),
                        serviceConfig.getEventProcessorConfig().get(), localController, checkpointStore, streamStore,
                        hostStore, segmentHelper, connectionFactory, streamMetadataTasks, controllerExecutor);

                // Bootstrap and start it asynchronously.
                log.info("Starting event processors");
                controllerEventProcessors.bootstrap(streamTransactionMetadataTasks, streamMetadataTasks)
                        .thenAcceptAsync(x -> controllerEventProcessors.startAsync(), controllerExecutor);
            }

            // Setup and start controller cluster listener after all sweepers have been initialized.
            if (serviceConfig.isControllerClusterListenerEnabled()) {
                List<FailoverSweeper> failoverSweepers = new ArrayList<>();
                failoverSweepers.add(taskSweeper);
                failoverSweepers.add(txnSweeper);
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
            if (serviceConfig.isControllerClusterListenerEnabled()) {
                log.info("Awaiting start of controller cluster listener");
                controllerClusterListener.awaitRunning();
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
            if (monitor != null) {
                log.info("Stopping the segment container monitor");
                monitor.stopAsync();
            }
            if (controllerClusterListener != null) {
                log.info("Stopping controller cluster listener");
                controllerClusterListener.stopAsync();
                log.info("Controller cluster listener shutdown");
            }

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
            log.info("Stopping controller executor");
            controllerExecutor.shutdownNow();

            log.info("Awaiting termination of controller executor");
            controllerExecutor.awaitTermination(5, TimeUnit.SECONDS);

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
}
