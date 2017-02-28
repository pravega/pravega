/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;
import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STORE_TYPE;

import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.server.eventProcessor.LocalController;
import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
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
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        //1. LOAD configuration.
        //Initialize the executor service.
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.valueOf(STORE_TYPE));

        log.info("Creating the stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE), executor);

        log.info("Creating zk based task store");
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(
                HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE));

        //Host monitor is not required for a single node local setup.
        if (Config.HOST_MONITOR_ENABLED) {
            //Start the Segment Container Monitor.
            log.info("Starting the segment container monitor");
            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore, ZKUtils.getCuratorClient(),
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.startAsync();
        }

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);
        TimeoutService timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                Config.MAX_LEASE_VALUE, Config.MAX_SCALE_GRACE_PERIOD);

        ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService);

        //2. set up Event Processors

        //region Setup Event Processors

        LocalController localController = new LocalController(controllerService);

        ControllerEventProcessors controllerEventProcessors = new ControllerEventProcessors(hostId, localController,
                ZKUtils.getCuratorClient(), streamStore, hostStore);

        try {
            controllerEventProcessors.initialize();
        } catch (Exception e) {
            log.error("Error initializing event processors", e);
            throw new RuntimeException(e);
        }

        streamTransactionMetadataTasks.initializeStreamWriters(localController);

        //endregion

        //3. Start the RPC server.
        log.info("Starting RPC server");
        RPCServer.start(new ControllerServiceAsyncImpl(controllerService));

        //3. Hook up TaskSweeper.sweepOrphanedTasks as a callback on detecting some controller node failure.
        // todo: hook up TaskSweeper.sweepOrphanedTasks with Failover support feature
        // Controller has a mechanism to track the currently active controller host instances. On detecting a failure of
        // any controller instance, the failure detector stores the failed HostId in a failed hosts directory (FH), and
        // invokes the taskSweeper.sweepOrphanedTasks for each failed host. When all resources under the failed hostId
        // are processed and deleted, that failed HostId is removed from FH folder.
        // Moreover, on controller process startup, it detects any hostIds not in the currently active set of
        // controllers and starts sweeping tasks orphaned by those hostIds.
        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks,
                streamTransactionMetadataTasks);

        // 4. Start the REST server.
        log.info("Starting Pravega REST Service");
        RESTServer.start(controllerService);
    }
}
