/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.fault.SegmentContainerMonitor;
import com.emc.pravega.controller.fault.UniformContainerBalancer;
import com.emc.pravega.controller.requesthandler.RequestReader;
import com.emc.pravega.controller.requesthandler.ScaleRequestHandler;
import com.emc.pravega.controller.requesthandler.TransactionTimer;
import com.emc.pravega.controller.requesthandler.TxTimeoutStreamScheduler;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.requests.TxTimeoutRequest;
import com.emc.pravega.controller.server.rest.RESTServer;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.controller.util.ZKUtils;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;
import static com.emc.pravega.controller.util.Config.HOST_STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STORE_TYPE;
import static com.emc.pravega.controller.util.Config.STREAM_STORE_TYPE;

/**
 * Entry point of controller server.
 */
@Slf4j
public class Main {

    private static final StreamConfiguration REQUEST_STREAM_CONFIG = new StreamConfigurationImpl(Config.INTERNAL_SCOPE,
            Config.SCALE_STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 3));

    private static final StreamConfiguration TXN_TIMER_STREAM_CONFIG = new StreamConfigurationImpl(Config.INTERNAL_SCOPE,
            Config.TXN_TIMER_STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 3));

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
            SegmentContainerMonitor monitor = new SegmentContainerMonitor(hostStore,
                    ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient(), Config.CLUSTER_NAME,
                    new UniformContainerBalancer(), Config.CLUSTER_MIN_REBALANCE_INTERVAL);
            monitor.startAsync();
        }

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId, new TxTimeoutStreamScheduler());
        ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks);

        //2. Start the RPC server.
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
        ClientFactory clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE, URI.create(String.format("tcp://localhost:%d", Config.SERVER_PORT)));

        CompletableFuture<Void> createStream = new CompletableFuture<>();
        CompletableFuture<Void> createTxnReader = new CompletableFuture<>();
        CompletableFuture<Void> createScaleReader = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> createStreams(controllerService, executor, createStream));
        createStream.thenCompose(x -> CompletableFuture.runAsync(() -> startTxnReader(clientFactory, streamStore, streamTransactionMetadataTasks, executor, createTxnReader)));
        createTxnReader.thenCompose(x -> CompletableFuture.runAsync(() -> startScaleReader(clientFactory, streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor, createScaleReader)));
    }

    private static void retry(Supplier<Void> supplier, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        try {
            supplier.get();
            result.complete(null);
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            executor.schedule(() -> retry(supplier, executor, result), 1, TimeUnit.MINUTES);
        }
    }

    private static void createStreams(ControllerService controllerService, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retry(() -> {
            FutureHelpers.await(CompletableFuture.allOf(
                    streamCreationCompletionCallback(
                            controllerService.createStream(REQUEST_STREAM_CONFIG, System.currentTimeMillis())),
                    streamCreationCompletionCallback(
                            controllerService.createStream(TXN_TIMER_STREAM_CONFIG, System.currentTimeMillis()))));
            return null;
        }, executor, result);
    }

    private static void startTxnReader(ClientFactory clientFactory, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retry(() -> {
            final TransactionTimer txnHandler = new TransactionTimer(streamTransactionMetadataTasks);
            final RequestReader<TxTimeoutRequest, TransactionTimer> txnreader = new RequestReader<>(
                    clientFactory,
                    Config.TXN_TIMER_STREAM_NAME,
                    Config.TXN_READER_ID,
                    Config.TXN_READER_GROUP, null, streamStore, txnHandler);
            CompletableFuture.runAsync(txnreader);
            return null;
        }, executor, result);
    }

    private static void startScaleReader(ClientFactory clientFactory, StreamMetadataTasks streamMetadataTasks, StreamMetadataStore streamStore, StreamTransactionMetadataTasks streamTransactionMetadataTasks, ScheduledExecutorService executor, CompletableFuture<Void> result) {
        retry(() -> {
            final ScaleRequestHandler handler = new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks);
            final RequestReader<ScaleRequest, ScaleRequestHandler> reader = new RequestReader<>(
                    clientFactory,
                    Config.SCALE_STREAM_NAME,
                    Config.SCALE_READER_ID,
                    Config.SCALE_READER_GROUP, null, streamStore, handler);
            CompletableFuture.runAsync(reader);
            return null;
        }, executor, result);
    }

    private static CompletableFuture<CreateStreamStatus> streamCreationCompletionCallback(CompletableFuture<CreateStreamStatus> createFuture) {
        return createFuture.whenComplete((res, ex) -> {
            if (ex != null && !(ex instanceof StreamAlreadyExistsException)) {
                // fail and exit
                throw new RuntimeException(ex);
            }
            if (res != null && res.equals(CreateStreamStatus.FAILURE)) {
                throw new RuntimeException("Failed to create stream while starting controller");
            }
        });
    }
}
