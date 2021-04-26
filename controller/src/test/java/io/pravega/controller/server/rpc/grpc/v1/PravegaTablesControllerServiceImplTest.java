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
package io.pravega.controller.server.rpc.grpc.v1;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.mocks.ControllerEventTableWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.DeleteTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactoryForTests;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * PravegaTables stream store configuration.
 */
public class PravegaTablesControllerServiceImplTest extends ControllerServiceImplTest {

    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private StoreClient storeClient;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamRequestHandler streamRequestHandler;
    private TaskMetadataStore taskMetadataStore;

    private ScheduledExecutorService executorService;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private Cluster cluster;
    private StreamMetadataStore streamStore;
    private SegmentHelper segmentHelper;

    private KVTableMetadataStore kvtStore;
    private TableMetadataTasks kvtMetadataTasks;
    private TableRequestHandler tableRequestHandler;

    @Override
    public ControllerService getControllerService() throws Exception {
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        storeClient = StoreClientFactory.createZKStoreClient(zkClient);
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executorService);
        taskMetadataStore = TaskStoreFactoryForTests.createStore(storeClient, executorService);
        streamStore = StreamStoreFactory.createPravegaTablesStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(), 
                zkClient, executorService);
        BucketStore bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executorService);
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executorService, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executorService, "host", GrpcAuthHelper.getDisabledAuthHelper(), requestTracker, helperMock);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                executorService, "host", GrpcAuthHelper.getDisabledAuthHelper());
        this.streamRequestHandler = spy(new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executorService),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executorService),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executorService),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executorService),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executorService),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executorService),
                streamStore,
                executorService));

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executorService));

        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        // KVTable
        this.kvtStore = KVTableStoreFactory.createPravegaTablesStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(),
                zkClient, executorService);
        EventHelper tableEventHelper = EventHelperMock.getEventHelperMock(executorService, "host",
                ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        this.kvtMetadataTasks = new TableMetadataTasks(kvtStore, segmentHelper, executorService, executorService,
                                "host", GrpcAuthHelper.getDisabledAuthHelper(), requestTracker, tableEventHelper);
        this.tableRequestHandler = new TableRequestHandler(new CreateTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), new DeleteTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), this.kvtStore, executorService);
        tableEventHelper.setRequestEventWriter(new ControllerEventTableWriterMock(tableRequestHandler, executorService));

        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        final CountDownLatch latch = new CountDownLatch(1);
        cluster.addListener((type, host) -> latch.countDown());
        cluster.registerHost(new Host("localhost", 9090, null));
        latch.await();

        return new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelper, executorService, cluster);
    }

    @After
    public void tearDown() throws Exception {
        if (executorService != null) {
            ExecutorServiceHelpers.shutdown(executorService);
        }
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
        streamStore.close();
        if (cluster != null) {
            cluster.close();
        }
        storeClient.close();
        zkClient.close();
        zkServer.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
    }

    @Test
    public void testTimeout() {
        streamMetadataTasks.setCompletionTimeoutMillis(500L);
        String stream = "timeoutStream";
        createScopeAndStream(SCOPE1, stream, ScalingPolicy.fixed(2));

        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamRequestHandler).processUpdateStream(any());
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build();
        ResultObserver<Controller.UpdateStreamStatus> result = new ResultObserver<>();
        this.controllerService.updateStream(ModelHelper.decode(SCOPE1, stream, configuration2), result);
        Predicate<Throwable> deadlineExceededPredicate = e -> {
            Throwable unwrap = Exceptions.unwrap(e);
            return unwrap instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) unwrap).getStatus().getCode().equals(Status.DEADLINE_EXCEEDED.getCode());
        };
        AssertExtensions.assertThrows("Timeout did not happen", result::get, deadlineExceededPredicate);
        reset(streamRequestHandler);

        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamRequestHandler).processTruncateStream(any());
        result = new ResultObserver<>();
        this.controllerService.truncateStream(Controller.StreamCut.newBuilder()
                                                                  .setStreamInfo(Controller.StreamInfo.newBuilder()
                                                                                                      .setScope(SCOPE1)
                                                                                                      .setStream(stream)
                                                                                                      .build())
                                                                  .putCut(0, 0).putCut(1, 0).build(), result);
        AssertExtensions.assertThrows("Timeout did not happen", result::get, deadlineExceededPredicate);
        reset(streamRequestHandler);

        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamRequestHandler).processSealStream(any());
        result = new ResultObserver<>();
        this.controllerService.sealStream(ModelHelper.createStreamInfo(SCOPE1, stream), result);
        AssertExtensions.assertThrows("Timeout did not happen", result::get, deadlineExceededPredicate);
        reset(streamRequestHandler);

        streamStore.setState(SCOPE1, stream, State.SEALED, null, executorService).join();
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamRequestHandler).processDeleteStream(any());
        ResultObserver<Controller.DeleteStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.deleteStream(ModelHelper.createStreamInfo(SCOPE1, stream), result2);
        AssertExtensions.assertThrows("Timeout did not happen", result2::get, deadlineExceededPredicate);
        reset(streamRequestHandler);
        streamMetadataTasks.setCompletionTimeoutMillis(Duration.ofMinutes(2).toMillis());
    }
}
