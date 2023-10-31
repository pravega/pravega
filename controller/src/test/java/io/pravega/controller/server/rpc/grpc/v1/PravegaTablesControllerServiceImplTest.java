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
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.mocks.ControllerEventTableWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.bucket.BucketManager;
import io.pravega.controller.server.bucket.BucketServiceFactory;
import io.pravega.controller.server.bucket.PeriodicRetention;
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
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.DeleteTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * PravegaTables stream store configuration.
 */
public class PravegaTablesControllerServiceImplTest extends ControllerServiceImplTest {

    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource();
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
    private BucketManager retentionService;
    private BucketStore bucketStore;

    @Override
    public ControllerService getControllerService() throws Exception {
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        executorService = ExecutorServiceHelpers.newScheduledThreadPool(20, "testpool");
        segmentHelper = SegmentHelperMock.getSegmentHelperMockForTables(executorService);
        taskMetadataStore = TaskStoreFactoryForTests.createStore(PRAVEGA_ZK_CURATOR_RESOURCE.storeClient, executorService);
        streamStore = StreamStoreFactory.createPravegaTablesStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executorService);
        // KVTable
        kvtStore = KVTableStoreFactory.createPravegaTablesStore(segmentHelper, GrpcAuthHelper.getDisabledAuthHelper(),
                PRAVEGA_ZK_CURATOR_RESOURCE.client, executorService);
        EventHelper tableEventHelper = EventHelperMock.getEventHelperMock(executorService, "host",
                ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        this.kvtMetadataTasks = new TableMetadataTasks(kvtStore, segmentHelper, executorService, executorService,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), tableEventHelper);
        this.tableRequestHandler = new TableRequestHandler(new CreateTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), new DeleteTableTask(this.kvtStore, this.kvtMetadataTasks,
                executorService), this.kvtStore, executorService);
        bucketStore = StreamStoreFactory.createZKBucketStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executorService);
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executorService, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executorService, "host", GrpcAuthHelper.getDisabledAuthHelper(), helperMock);
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
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtMetadataTasks, executorService),
                executorService));

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executorService));

        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        tableEventHelper.setRequestEventWriter(new ControllerEventTableWriterMock(tableRequestHandler, executorService));

        cluster = new ClusterZKImpl(PRAVEGA_ZK_CURATOR_RESOURCE.client, ClusterType.CONTROLLER);
        final CountDownLatch latch = new CountDownLatch(1);
        cluster.addListener((type, host) -> latch.countDown());
        cluster.registerHost(new Host("localhost", 9090, null));
        latch.await();

        BucketServiceFactory bucketServiceFactory = new BucketServiceFactory("host", bucketStore, 1000,
                1);
        PeriodicRetention retentionWork = new PeriodicRetention(streamStore, streamMetadataTasks, executorService, requestTracker);
        retentionService = bucketServiceFactory.createRetentionService(Duration.ofMinutes(30), retentionWork::retention, executorService);

        return new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelper, executorService, cluster, requestTracker);
    }

    @Override
    BucketManager getBucketManager() {
        return retentionService;
    }

    @Override
    protected BucketStore getBucketStore() {
        return bucketStore;
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
        kvtStore.close();
        if (cluster != null) {
            cluster.close();
        }
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

    @Test
    public void streamsInScopeForTagTest() {
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build();
        final StreamConfiguration cfgWithTags = StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .tag("tag1")
                                                                   .tag("tag2")
                                                                   .build();
        final StreamConfiguration cfgWithTag = StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(2))
                                                                  .tag("tag1")
                                                                  .build();

        // Create Scope
        ResultObserver<Controller.CreateScopeStatus> result = new ResultObserver<>();
        Controller.ScopeInfo scopeInfo = Controller.ScopeInfo.newBuilder().setScope(SCOPE1).build();
        this.controllerService.createScope(scopeInfo, result);
        Assert.assertEquals(result.get().getStatus(), Controller.CreateScopeStatus.Status.SUCCESS);

        // Create Streams
        ResultObserver<Controller.CreateStreamStatus> createStreamStatus1 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(SCOPE1, STREAM1, configuration), createStreamStatus1);
        Controller.CreateStreamStatus status = createStreamStatus1.get();
        Assert.assertEquals(status.getStatus(), Controller.CreateStreamStatus.Status.SUCCESS);

        ResultObserver<Controller.CreateStreamStatus> createStreamStatus2 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(SCOPE1, STREAM2, cfgWithTag), createStreamStatus2);
        status = createStreamStatus2.get();
        Assert.assertEquals(status.getStatus(), Controller.CreateStreamStatus.Status.SUCCESS);

        ResultObserver<Controller.CreateStreamStatus> createStreamStatus3 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(SCOPE1, STREAM3, cfgWithTags), createStreamStatus3);
        status = createStreamStatus3.get();
        Assert.assertEquals(status.getStatus(), Controller.CreateStreamStatus.Status.SUCCESS);

        // Verify with valid tag.
        Controller.StreamsInScopeWithTagRequest request = Controller.StreamsInScopeWithTagRequest.newBuilder().setScope(scopeInfo).setTag("tag2").build();
        ResultObserver<Controller.StreamsInScopeResponse> response = new ResultObserver<>();
        this.controllerService.listStreamsInScopeForTag(request, response);

        List<Controller.StreamInfo> streams = response.get().getStreamsList();
        assertEquals(1, streams.size());
        assertEquals(STREAM3, streams.get(0).getStream());

        // Verify with missing tag.
        request = Controller.StreamsInScopeWithTagRequest.newBuilder().setScope(scopeInfo).setTag("tagx").build();
        response = new ResultObserver<>();
        this.controllerService.listStreamsInScopeForTag(request, response);
        streams = response.get().getStreamsList();
        assertEquals(0, streams.size());

        // Verify with nonExistentScope
        request = Controller.StreamsInScopeWithTagRequest.newBuilder().setScope(Controller.ScopeInfo.newBuilder()
                                                                                                    .setScope("NonExistent")
                                                                                                    .build())
                                                         .setTag("tagx").build();
        response = new ResultObserver<>();
        this.controllerService.listStreamsInScopeForTag(request, response);
        assertEquals(response.get().getStatus(), Controller.StreamsInScopeResponse.Status.SCOPE_NOT_FOUND);

        // Verify with multiple streams.
        request = Controller.StreamsInScopeWithTagRequest.newBuilder().setScope(scopeInfo).setTag("tag1").build();
        response = new ResultObserver<>();
        this.controllerService.listStreamsInScopeForTag(request, response);

        List<Controller.StreamInfo> resultStreams = new ArrayList<>();
        resultStreams.addAll(response.get().getStreamsList());
        if (resultStreams.size() == 1) {
            request = Controller.StreamsInScopeWithTagRequest.newBuilder().setScope(scopeInfo).setTag("tag1").setContinuationToken(response.get().getContinuationToken()).build();
            response = new ResultObserver<>();
            this.controllerService.listStreamsInScopeForTag(request, response);
            resultStreams.addAll(response.get().getStreamsList());
        }
        assertEquals(2, resultStreams.size());
        Controller.StreamInfo streamInfo = Controller.StreamInfo.newBuilder().setStream(STREAM3).setScope(SCOPE1).build();
        assertTrue(resultStreams.contains(streamInfo));
        streamInfo = Controller.StreamInfo.newBuilder().setStream(STREAM2).setScope(SCOPE1).build();
        assertTrue(resultStreams.contains(streamInfo));
    }

    @Test
    public void deleteStreamWithTagsTest() {
        final StreamConfiguration cfgWithTags = StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(2))
                                                                   .tag("tag1")
                                                                   .tag("tag2")
                                                                   .build();

        // Create Scope
        ResultObserver<Controller.CreateScopeStatus> result = new ResultObserver<>();
        Controller.ScopeInfo scopeInfo = Controller.ScopeInfo.newBuilder().setScope(SCOPE1).build();
        this.controllerService.createScope(scopeInfo, result);
        Assert.assertEquals(result.get().getStatus(), Controller.CreateScopeStatus.Status.SUCCESS);

        // Create Stream
        ResultObserver<Controller.CreateStreamStatus> createStreamStatus1 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(SCOPE1, STREAM1, cfgWithTags), createStreamStatus1);
        Controller.CreateStreamStatus status = createStreamStatus1.get();
        Assert.assertEquals(status.getStatus(), Controller.CreateStreamStatus.Status.SUCCESS);

        Controller.StreamInfo streamInfo = Controller.StreamInfo.newBuilder().setScope(SCOPE1).setStream(STREAM1).build();

        // Get Stream Configuration
        ResultObserver<Controller.StreamConfig> getStreamConfigResult = new ResultObserver<>();
        this.controllerService.getStreamConfiguration(streamInfo, getStreamConfigResult);
        assertEquals(cfgWithTags, ModelHelper.encode(getStreamConfigResult.get()));

        // Seal Stream.
        ResultObserver<Controller.UpdateStreamStatus> sealStreamResult = new ResultObserver<>();
        this.controllerService.sealStream(streamInfo, sealStreamResult);
        assertEquals(Controller.UpdateStreamStatus.Status.SUCCESS, sealStreamResult.get().getStatus());

        // Delete Stream
        ResultObserver<Controller.DeleteStreamStatus> deleteStreamResult = new ResultObserver<>();
        this.controllerService.deleteStream(streamInfo, deleteStreamResult);
        assertEquals(Controller.DeleteStreamStatus.Status.SUCCESS, deleteStreamResult.get().getStatus());
    }
}
