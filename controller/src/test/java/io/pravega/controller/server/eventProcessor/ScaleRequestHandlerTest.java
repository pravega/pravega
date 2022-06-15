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
package io.pravega.controller.server.eventProcessor;

import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.controller.MetricsTestUtil;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class ScaleRequestHandlerTest {
    protected ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected CuratorFramework zkClient;
    protected StreamMetadataStore streamStore;
    protected StreamMetadataTasks streamMetadataTasks;

    private final String scope = "scope";
    private final String stream = "stream";

    private BucketStore bucketStore;
    private TaskMetadataStore taskMetadataStore;

    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;
    private EventStreamClientFactory clientFactory;
    private ConnectionFactory connectionFactory;

    private RequestTracker requestTracker = new RequestTracker(true);
    private StatsProvider statsProvider = null;

    @Before
    public void setup() throws Exception {
        StreamMetrics.initialize();
        TransactionMetrics.initialize();
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(20, 1, 50));

        zkClient.start();

        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        streamStore = spy(getStore());
        bucketStore = StreamStoreFactory.createZKBucketStore(zkClient, executor);

        taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = mock(EventStreamClientFactory.class);
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());
        streamMetadataTasks.initializeStreamWriters(clientFactory, Config.SCALE_STREAM_NAME);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, 
                segmentHelper, executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());

        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope, null, executor).get();
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 3))
                                                        .build();
        streamMetadataTasks.createStream(scope, stream, config, createTimestamp, 0L).get();
        // set minimum number of segments to 1 so that we can also test scale downs
        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 1))
                                    .build();
        streamStore.startUpdateConfiguration(scope, stream, config, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStore.getConfigurationRecord(scope, stream, null, executor).join();
        streamStore.completeUpdateConfiguration(scope, stream, configRecord, null, executor).join();

        statsProvider = MetricsTestUtil.getInitializedStatsProvider();
        statsProvider.startWithoutExporting();
    }

    @After
    public void tearDown() throws Exception {
        clientFactory.close();
        connectionFactory.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        zkClient.close();
        zkServer.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
        }
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        AutoScaleTask requestHandler = new AutoScaleTask(streamMetadataTasks, streamStore, executor);
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler multiplexer = new StreamRequestHandler(requestHandler, scaleRequestHandler,
                null, null, null, null, null, null, null, streamStore, null, executor);
        // Send number of splits = 1
        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        AutoScaleEvent scaleUpEvent = new AutoScaleEvent(scope, stream, 2, AutoScaleEvent.UP, System.currentTimeMillis(),
                1, false, System.currentTimeMillis());
        assertTrue(Futures.await(multiplexer.process(scaleUpEvent, () -> false)));

        // verify that one scaleOp event is written into the stream
        assertEquals(1, writer.queue.size());
        ControllerEvent event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleOpEvent = (ScaleOpEvent) event;
        double start = 2.0 / 3.0;
        double end = 1.0;
        double middle = (start + end) / 2;
        assertEquals(2, scaleOpEvent.getNewRanges().size());
        double delta = 0.0000000000001;
        assertEquals(start, scaleOpEvent.getNewRanges().get(0).getKey(), delta);
        assertEquals(middle, scaleOpEvent.getNewRanges().get(0).getValue(), delta);
        assertEquals(middle, scaleOpEvent.getNewRanges().get(1).getKey(), delta);
        assertEquals(end, scaleOpEvent.getNewRanges().get(1).getValue(), delta);
        assertEquals(1, scaleOpEvent.getSegmentsToSeal().size());
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(2L));

        assertTrue(Futures.await(multiplexer.process(scaleOpEvent, () -> false)));

        // verify that the event is processed successfully
        List<StreamSegmentRecord> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.segmentId() == 2L));
        // verify that two splits are created even when we sent 1 as numOfSplits in AutoScaleEvent.
        long three = computeSegmentId(3, 1);
        long four = computeSegmentId(4, 1);
        assertTrue(activeSegments.stream().anyMatch(z -> z.segmentId() == three));
        assertTrue(activeSegments.stream().anyMatch(z -> z.segmentId() == four));
        assertTrue(activeSegments.size() == 4);

        // process first scale down event. it should only mark the segment as cold
        AutoScaleEvent scaleDownEvent = new AutoScaleEvent(scope, stream, four, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis());
        assertTrue(Futures.await(multiplexer.process(scaleDownEvent, () -> false)));
        assertTrue(writer.queue.isEmpty());

        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();
        assertTrue(activeSegments.stream().anyMatch(z -> z.segmentId() == four));
        assertTrue(activeSegments.size() == 4);
        assertTrue(streamStore.isCold(scope, stream, four, null, executor).join());

        AutoScaleEvent scaleDownEvent2 = new AutoScaleEvent(scope, stream, three, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis());
        assertTrue(Futures.await(multiplexer.process(scaleDownEvent2, () -> false)));
        assertTrue(streamStore.isCold(scope, stream, three, null, executor).join());

        // verify that a new event has been posted
        assertEquals(1, writer.queue.size());
        event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        scaleOpEvent = (ScaleOpEvent) event;
        assertEquals(1, scaleOpEvent.getNewRanges().size());
        assertEquals(start, scaleOpEvent.getNewRanges().get(0).getKey(), delta);
        assertEquals(end, scaleOpEvent.getNewRanges().get(0).getValue(), delta);
        assertEquals(2, scaleOpEvent.getSegmentsToSeal().size());
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(three));
        assertTrue(scaleOpEvent.getSegmentsToSeal().contains(four));

        // process scale down event
        assertTrue(Futures.await(multiplexer.process(scaleOpEvent, () -> false)));
        long five = computeSegmentId(5, 2);

        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.segmentId() == three));
        assertTrue(activeSegments.stream().noneMatch(z -> z.segmentId() == four));
        assertTrue(activeSegments.stream().anyMatch(z -> z.segmentId() == five));
        assertTrue(activeSegments.size() == 3);

        // make it throw a non retryable failure so that test does not wait for number of retries.
        // This will bring down the test duration drastically because a retryable failure can keep retrying for few seconds.
        // And if someone changes retry durations and number of attempts in retry helper, it will impact this test's running time.
        // hence sending incorrect segmentsToSeal list which will result in a non retryable failure and this will fail immediately
        assertFalse(Futures.await(multiplexer.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(five),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.5, 1.0)), false, System.currentTimeMillis(), System.currentTimeMillis()), () -> false)));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();
        assertTrue(activeSegments.stream().noneMatch(z -> z.segmentId() == three));
        assertTrue(activeSegments.stream().noneMatch(z -> z.segmentId() == four));
        assertTrue(activeSegments.stream().anyMatch(z -> z.segmentId() == five));
        assertTrue(activeSegments.size() == 3);

        assertFalse(Futures.await(multiplexer.process(new AbortEvent(scope, stream, 0, UUID.randomUUID(), 11L), () -> false)));
        assertTrue(MetricsTestUtil.getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_AUTO_SCALE_STREAM_LATENCY) > 0);
    }

    @Test(timeout = 30000)
    public void testScaleRequestWithMinimumSegment() throws ExecutionException, InterruptedException {
        AutoScaleTask requestHandler = new AutoScaleTask(streamMetadataTasks, streamStore, executor);
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler multiplexer = new StreamRequestHandler(requestHandler, scaleRequestHandler,
                null, null, null, null, null,
                null, null, streamStore, null, executor);
        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        String stream = "mystream";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 5))
                                                        .build();
        streamMetadataTasks.createStream(scope, stream, config, System.currentTimeMillis(), 0L).get();

        // change stream configuration to min segment count = 4
        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 4))
                                    .build();
        streamStore.startUpdateConfiguration(scope, stream, config, null, executor).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = streamStore.getConfigurationRecord(scope, stream, 
                null, executor).join();
        streamStore.completeUpdateConfiguration(scope, stream, configRecord, null, executor).join();
        
        // process first auto scale down event. it should only mark the segment as cold
        multiplexer.process(new AutoScaleEvent(scope, stream, 1L, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis()), () -> false).join();
        assertTrue(writer.queue.isEmpty());

        assertTrue(streamStore.isCold(scope, stream, 1L, null, executor).join());

        // process second auto scale down event. since its not for an immediate neighbour so it should only mark the segment as cold
        multiplexer.process(new AutoScaleEvent(scope, stream, 3L, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis()), () -> false).join();
        assertTrue(streamStore.isCold(scope, stream, 3L, null, executor).join());
        // no scale event should be posted
        assertTrue(writer.queue.isEmpty());

        // process third auto scale down event. This should result in a scale op event being posted to merge segments 0, 1 
        multiplexer.process(new AutoScaleEvent(scope, stream, 0L, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis()), () -> false).join();
        assertTrue(streamStore.isCold(scope, stream, 0L, null, executor).join());
        
        // verify that a new event has been posted
        assertEquals(1, writer.queue.size());
        ControllerEvent event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleDownEvent1 = (ScaleOpEvent) event;
        assertEquals(1, scaleDownEvent1.getNewRanges().size());
        assertEquals(2, scaleDownEvent1.getSegmentsToSeal().size());
        assertTrue(scaleDownEvent1.getSegmentsToSeal().contains(0L));
        assertTrue(scaleDownEvent1.getSegmentsToSeal().contains(1L));

        // process fourth auto scale down event. This should result in a scale op event being posted to merge segments 3, 4 
        multiplexer.process(new AutoScaleEvent(scope, stream, 4L, AutoScaleEvent.DOWN, System.currentTimeMillis(),
                0, false, System.currentTimeMillis()), () -> false).join();
        assertTrue(streamStore.isCold(scope, stream, 4L, null, executor).join());
        // verify that a new event has been posted
        assertEquals(1, writer.queue.size());
        event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleDownEvent2 = (ScaleOpEvent) event;
        assertEquals(1, scaleDownEvent2.getNewRanges().size());
        assertEquals(2, scaleDownEvent2.getSegmentsToSeal().size());
        assertTrue(scaleDownEvent2.getSegmentsToSeal().contains(3L));
        assertTrue(scaleDownEvent2.getSegmentsToSeal().contains(4L));

        // process first scale down event, this should submit scale and scale the stream down to 4 segments
        multiplexer.process(scaleDownEvent1, () -> false).join();
        EpochRecord activeEpoch = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        List<StreamSegmentRecord> segments = activeEpoch.getSegments();
        assertEquals(1, activeEpoch.getEpoch());
        assertEquals(4, segments.size());
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 2));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 3));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 4));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 5));

        // process second scale down event, this should submit scale and scale the stream down to 4 segments
        multiplexer.process(scaleDownEvent2, () -> false).join();
        // verify that no scale has happened
        activeEpoch = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        // verify that no scale has happened. 
        assertEquals(1, activeEpoch.getEpoch());
        assertEquals(4, segments.size());
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 2));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 3));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 4));
        assertTrue(segments.stream().anyMatch(x -> x.getSegmentNumber() == 5));
    }

    @Test(timeout = 30000)
    public void testScaleWithTransactionRequest() throws InterruptedException {
        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler requestHandler = new StreamRequestHandler(null, scaleRequestHandler,
                null, null, null, null, null, null, null, streamStore, null, executor);
        CommitRequestHandler commitRequestHandler = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);

        // 1 create transaction on old epoch and set it to committing
        UUID txnIdOldEpoch = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(scope, stream, txnIdOldEpoch, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnData.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        EpochRecord epochZero = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(0, epochZero.getEpoch());

        // 2. start scale
        requestHandler.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(0L, 1L, 2L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), false, System.currentTimeMillis(), System.currentTimeMillis()), () -> false).join();

        // 3. verify that scale is complete
        State state = streamStore.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, state);

        EpochRecord epochOne = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(1, epochOne.getEpoch());

        // 4. create transaction -> verify that this is created on new epoch
        UUID txnIdNewEpoch = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnDataNew = streamStore.createTransaction(scope, stream, txnIdNewEpoch, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnDataNew.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        // 5. commit on old epoch. this should roll over
        assertTrue(Futures.await(commitRequestHandler.processEvent(new CommitEvent(scope, stream, txnData.getEpoch()))));
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnIdOldEpoch, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, txnStatus);

        EpochRecord epochTwo = streamStore.getEpoch(scope, stream, 2, null, executor).join();
        EpochRecord epochThree = streamStore.getEpoch(scope, stream, 3, null, executor).join();
        assertEquals(0, epochTwo.getReferenceEpoch());
        assertEquals(epochZero.getSegments().size(), epochTwo.getSegments().size());
        assertEquals(epochZero.getSegments().stream().map(x -> NameUtils.getSegmentNumber(x.segmentId())).collect(Collectors.toSet()),
                epochTwo.getSegments().stream().map(x -> NameUtils.getSegmentNumber(x.segmentId())).collect(Collectors.toSet()));
        assertEquals(1, epochThree.getReferenceEpoch());
        assertEquals(epochOne.getSegments().size(), epochThree.getSegments().size());
        assertEquals(epochOne.getSegments().stream().map(x -> NameUtils.getSegmentNumber(x.segmentId())).collect(Collectors.toSet()),
                epochThree.getSegments().stream().map(x -> NameUtils.getSegmentNumber(x.segmentId())).collect(Collectors.toSet()));

        EpochRecord activeEpoch = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(epochThree, activeEpoch);

        // 6. commit on new epoch. This should happen on duplicate of new epoch successfully
        assertTrue(Futures.await(commitRequestHandler.processEvent(new CommitEvent(scope, stream, txnDataNew.getEpoch()))));
        txnStatus = streamStore.transactionStatus(scope, stream, txnIdNewEpoch, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, txnStatus);

        activeEpoch = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(epochThree, activeEpoch);
    }

    @Test(timeout = 30000)
    public void testInconsistentScaleRequestAfterRollingTxn() throws Exception {
        // This test checks a scenario where after rolling txn, if an outstanding scale request
        // was present, its epoch consistency should fail
        String stream = "newStream";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 2)).build();
        streamMetadataTasks.createStream(scope, stream, config, System.currentTimeMillis(), 0L).get();

        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler requestHandler = new StreamRequestHandler(null, scaleRequestHandler,
                null, null, null, null, null, null, null, streamStore, null, executor);
        CommitRequestHandler commitRequestHandler = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);

        // 1 create transaction on old epoch and set it to committing
        UUID txnIdOldEpoch = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(scope, stream, txnIdOldEpoch, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnData.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        UUID txnIdOldEpoch2 = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnData2 = streamStore.createTransaction(scope, stream, txnIdOldEpoch2, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnData2.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        EpochRecord epochZero = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(0, epochZero.getEpoch());

        // 2. start scale
        requestHandler.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.25), new AbstractMap.SimpleEntry<>(0.25, 0.5)),
                false, System.currentTimeMillis(), System.currentTimeMillis()), () -> false).join();

        // 3. verify that scale is complete
        State state = streamStore.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, state);

        // 4. just submit a new scale. don't let it run. this should create an epoch transition. state should still be active
        streamStore.submitScale(scope, stream, Lists.newArrayList(1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)),
        System.currentTimeMillis(), null, null, executor).join();

        // 5. commit on old epoch. this should roll over.
        assertTrue(Futures.await(commitRequestHandler.processEvent(new CommitEvent(scope, stream, txnData.getEpoch()))));
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnIdOldEpoch, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, txnStatus);

        // 6. run scale. this should fail in scaleCreateNewEpochs with IllegalArgumentException with epochTransitionConsistent
        AssertExtensions.assertFutureThrows("epoch transition should be inconsistent", requestHandler.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)),
                false, System.currentTimeMillis(), System.currentTimeMillis()), () -> false), e -> Exceptions.unwrap(e) instanceof IllegalStateException);

        state = streamStore.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, state);
    }

    @Test(timeout = 30000)
    public void testMigrateManualScaleRequestAfterRollingTxn() throws Exception {
        // This test checks a scenario where after rolling txn, if an outstanding scale request
        // was present, its epoch consistency should fail
        String stream = "newStream";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 2)).build();
        streamMetadataTasks.createStream(scope, stream, config, System.currentTimeMillis(), 0L).get();

        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler requestHandler = new StreamRequestHandler(null, scaleRequestHandler,
                null, null, null, null, null, null, null, streamStore, null, executor);
        CommitRequestHandler commitRequestHandler = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);

        // 1 create transaction on old epoch and set it to committing
        UUID txnIdOldEpoch = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(scope, stream, txnIdOldEpoch, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnData.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        UUID txnIdOldEpoch2 = streamStore.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnData2 = streamStore.createTransaction(scope, stream, txnIdOldEpoch2, 10000, 10000,
                null, executor).join();
        streamStore.sealTransaction(scope, stream, txnData2.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();

        EpochRecord epochZero = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(0, epochZero.getEpoch());

        // 2. start scale
        requestHandler.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 0.25), new AbstractMap.SimpleEntry<>(0.25, 0.5)),
                false, System.currentTimeMillis(), System.currentTimeMillis()), () -> false).join();

        // 3. verify that scale is complete
        State state = streamStore.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, state);

        // 4. just submit a new scale. don't let it run. this should create an epoch transition. state should still be active
        streamStore.submitScale(scope, stream, Lists.newArrayList(1L), Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)),
        System.currentTimeMillis(), null, null, executor).join();

        // 5. commit on old epoch. this should roll over.
        assertTrue(Futures.await(commitRequestHandler.processEvent(new CommitEvent(scope, stream, txnData.getEpoch()))));
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnIdOldEpoch, null, executor).join();
        assertEquals(TxnStatus.COMMITTED, txnStatus);

        // 6. run scale against old record but with manual scale flag set to true. This should be migrated to new epoch and processed.
        requestHandler.process(new ScaleOpEvent(scope, stream, Lists.newArrayList(1L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.5, 0.75), new AbstractMap.SimpleEntry<>(0.75, 1.0)),
                true, System.currentTimeMillis(), System.currentTimeMillis()), () -> false).join();

        state = streamStore.getState(scope, stream, true, null, executor).join();
        assertEquals(State.ACTIVE, state);
        EpochRecord epoch = streamStore.getActiveEpoch(scope, stream, null, true, executor).join();
        assertEquals(4, epoch.getEpoch());
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testConcurrentIdempotentManualScaleRequest() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("startScale", 0);
        map.put("scaleCreateNewEpochs", 0);
        map.put("scaleSegmentsSealed", 0);
        map.put("completeScale", 0);
        map.put("updateVersionedState", 1);

        concurrentIdenticalScaleRun("stream0", "updateVersionedState", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        map.put("startScale", 1);
        concurrentIdenticalScaleRun("stream1", "startScale", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        map.put("scaleCreateNewEpochs", 1);
        map.put("scaleSegmentsSealed", 1);
        map.put("completeScale", 1);
        concurrentIdenticalScaleRun("stream2", "scaleCreateNewEpochs", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);
        
        concurrentIdenticalScaleRun("stream4", "scaleSegmentsSealed", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        concurrentIdenticalScaleRun("stream5", "completeScale", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, true,
                e -> Exceptions.unwrap(e) instanceof IllegalStateException, map);
    }
    
    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testConcurrentIdempotentAutoScaleRequest() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("startScale", 0);
        map.put("scaleCreateNewEpochs", 0);
        map.put("scaleSegmentsSealed", 0);
        map.put("completeScale", 0);
        map.put("updateVersionedState", 1);

        // second scale should complete scale.
        // when first scale resumes it should fail with write conflict in its attempt to update state.
        concurrentIdenticalScaleRun("autostream0", "updateVersionedState", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        map.put("startScale", 1);
        // second scale should complete scale.
        // when first scale resumes start scale should attempt to discard epoch transition and in its attempt fail and 
        // throw write conflict
        concurrentIdenticalScaleRun("autostream1", "startScale", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        map.put("scaleCreateNewEpochs", 1);
        map.put("scaleSegmentsSealed", 1);
        map.put("completeScale", 1);
        // second scale should complete scale.
        // when first scale resumes both scaleCreateNewEpochs and scalesealedSegments should succeed (idempotent with no changes)
        // and complete scale should fail with write conflict
        concurrentIdenticalScaleRun("autostream2", "scaleCreateNewEpochs", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        // second scale should complete scale.
        // when first scale resumes scalesealedSegments should succeed (idempotent with no changes)
        // and complete scale should fail with write conflict
        concurrentIdenticalScaleRun("autostream4", "scaleSegmentsSealed", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, false, e -> false, map);

        // second scale should find epoch transition to be inconsistent and reset it.
        // when first scale resumes it should attempt to update epoch transition and fail with write conflict
        concurrentIdenticalScaleRun("autostream5", "completeScale", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, true, 
                e -> Exceptions.unwrap(e) instanceof IllegalStateException, map);
    }
    
    private void concurrentIdenticalScaleRun(String stream, String func, boolean isManual,
                                             Predicate<Throwable> firstExceptionPredicate,
                                             boolean expectFailureOnSecondJob,
                                             Predicate<Throwable> secondExceptionPredicate,
                                             Map<String, Integer> invocationCount) throws Exception {
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();
        
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L), 
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), isManual, System.currentTimeMillis(), System.currentTimeMillis());
        if (isManual) {
            streamStore1.submitScale(scope, stream, Lists.newArrayList(0L),
                    Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), System.currentTimeMillis(), null, null, executor).join();
        }
        
        StreamMetadataStore streamStore2 = getStore();

        ScaleOperationTask scaleRequestHandler1 = new ScaleOperationTask(streamMetadataTasks, streamStore1Spied, executor);
        ScaleOperationTask scaleRequestHandler2 = new ScaleOperationTask(streamMetadataTasks, streamStore2, executor);
        
        setMockLatch(streamStore1, streamStore1Spied, func, signal, wait);
        
        // the processing will stall at start scale
        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> scaleRequestHandler1.execute(event), executor);
        signal.join();
        
        // let this run to completion. this should succeed 
        if (!expectFailureOnSecondJob) {
            scaleRequestHandler2.execute(event).join();
        } else {
            AssertExtensions.assertSuppliedFutureThrows("second job should fail", () -> scaleRequestHandler2.execute(event), 
                    secondExceptionPredicate);
        }
        // verify that scale is complete
        // now complete wait latch.
        wait.complete(null);
        
        AssertExtensions.assertSuppliedFutureThrows(
                "first scale should fail", () -> future1, firstExceptionPredicate);
        verify(streamStore1Spied, times(invocationCount.get("startScale"))).startScale(anyString(), anyString(), anyBoolean(), any(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("scaleCreateNewEpochs"))).scaleCreateNewEpochs(anyString(), anyString(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("scaleSegmentsSealed"))).scaleSegmentsSealed(anyString(), anyString(), any(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("completeScale"))).completeScale(anyString(), anyString(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("updateVersionedState"))).updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
        
        // validate scale done
        VersionedMetadata<EpochTransitionRecord> versioned = streamStore1.getEpochTransition(scope, stream, null, executor).join();
        assertEquals(EpochTransitionRecord.EMPTY, versioned.getObject());
        assertEquals(2, getVersionNumber(versioned));
        assertEquals(1, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        streamStore1.close();
        streamStore2.close();
    }

    abstract <T> Number getVersionNumber(VersionedMetadata<T> versioned); 

    abstract StreamMetadataStore getStore();

    private void setMockLatch(StreamMetadataStore store, StreamMetadataStore spied, 
                             String func, CompletableFuture<Void> signal, CompletableFuture<Void> waitOn) {
        switch (func) {
            case "startScale" : doAnswer(x -> {
                signal.complete(null);
                waitOn.join();
                return store.startScale(x.getArgument(0), x.getArgument(1),
                        x.getArgument(2), x.getArgument(3),
                        x.getArgument(4), x.getArgument(5), x.getArgument(6));
            }).when(spied).startScale(anyString(), anyString(), anyBoolean(), any(),  any(), any(), any());
            break;
            case "scaleCreateNewEpochs" : doAnswer(x -> {
                signal.complete(null);
                waitOn.join();
                return store.scaleCreateNewEpochs(x.getArgument(0), x.getArgument(1),
                        x.getArgument(2), x.getArgument(3), x.getArgument(4));
            }).when(spied).scaleCreateNewEpochs(anyString(), anyString(), any(), any(), any());
                break;
            case "scaleSegmentsSealed" : doAnswer(x -> {
                signal.complete(null);
                waitOn.join();
                return store.scaleSegmentsSealed(x.getArgument(0), x.getArgument(1),
                        x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
            }).when(spied).scaleSegmentsSealed(anyString(), anyString(), any(), any(), any(), any());
                break;
            case "completeScale" : doAnswer(x -> {
                signal.complete(null);
                waitOn.join();
                return store.completeScale(x.getArgument(0), x.getArgument(1),
                        x.getArgument(2), x.getArgument(3), x.getArgument(4));
            }).when(spied).completeScale(anyString(), anyString(), any(), any(), any());
                break;
            case "updateVersionedState" : doAnswer(x -> {
                signal.complete(null);
                waitOn.join();
                return store.updateVersionedState(x.getArgument(0), x.getArgument(1),
                        x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
            }).when(spied).updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
                break;
            default:
                break;
        }
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testConcurrentDistinctManualScaleRequest() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("startScale", 0);
        map.put("scaleCreateNewEpochs", 0);
        map.put("scaleSegmentsSealed", 0);
        map.put("completeScale", 0);
        map.put("updateVersionedState", 1);

        concurrentDistinctScaleRun("stream0", "updateVersionedState", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);

        map.put("startScale", 1);
        concurrentDistinctScaleRun("stream1", "startScale", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);

        map.put("scaleCreateNewEpochs", 1);
        map.put("scaleSegmentsSealed", 1);
        map.put("completeScale", 1);
        concurrentDistinctScaleRun("stream2", "scaleCreateNewEpochs", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);
        
        concurrentDistinctScaleRun("stream4", "scaleSegmentsSealed", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testConcurrentDistinctAutoScaleRequest() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("startScale", 0);
        map.put("scaleCreateNewEpochs", 0);
        map.put("scaleSegmentsSealed", 0);
        map.put("completeScale", 0);
        map.put("updateVersionedState", 1);

        concurrentDistinctScaleRun("autostream0", "updateVersionedState", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);

        map.put("startScale", 1);
        concurrentDistinctScaleRun("autostream1", "startScale", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);

        map.put("scaleCreateNewEpochs", 1);
        map.put("scaleSegmentsSealed", 1);
        map.put("completeScale", 1);
        concurrentDistinctScaleRun("autostream2", "scaleCreateNewEpochs", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);

        concurrentDistinctScaleRun("autostream4", "scaleSegmentsSealed", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map);
    }

    // concurrent run of scale 1 intermixed with scale 2 
    private void concurrentDistinctScaleRun(String stream, String funcToWaitOn, boolean isManual,
                                    Predicate<Throwable> firstExceptionPredicate,
                                    Map<String, Integer> invocationCount) throws Exception {
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), isManual, System.currentTimeMillis(), System.currentTimeMillis());
        if (isManual) {
            streamStore1.submitScale(scope, stream, Lists.newArrayList(0L),
                    Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), System.currentTimeMillis(), null, null, executor).join();
        }

        StreamMetadataStore streamStore2 = getStore();

        ScaleOperationTask scaleRequestHandler1 = new ScaleOperationTask(streamMetadataTasks, streamStore1Spied, executor);
        ScaleOperationTask scaleRequestHandler2 = new ScaleOperationTask(streamMetadataTasks, streamStore2, executor);

        setMockLatch(streamStore1, streamStore1Spied, funcToWaitOn, signal, wait);

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> scaleRequestHandler1.execute(event), executor);
        signal.join();

        // let this run to completion. this should succeed 
        scaleRequestHandler2.execute(event).join();

        long one = NameUtils.computeSegmentId(1, 1);
        ScaleOpEvent event2 = new ScaleOpEvent(scope, stream, Lists.newArrayList(one),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), isManual, System.currentTimeMillis(), System.currentTimeMillis());
        if (isManual) {
            streamStore1.submitScale(scope, stream, Lists.newArrayList(one),
                    Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), System.currentTimeMillis(), null, null, executor).join();
        }

        scaleRequestHandler2.execute(event2).join();
        
        // now complete wait latch.
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows(
                "first scale should fail", () -> future1, firstExceptionPredicate);
        verify(streamStore1Spied, times(invocationCount.get("startScale"))).startScale(anyString(), anyString(), anyBoolean(), any(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("scaleCreateNewEpochs"))).scaleCreateNewEpochs(anyString(), anyString(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("scaleSegmentsSealed"))).scaleSegmentsSealed(anyString(), anyString(), any(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("completeScale"))).completeScale(anyString(), anyString(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("updateVersionedState"))).updateVersionedState(anyString(), anyString(), any(), any(), any(), any());

        // validate scale done
        VersionedMetadata<EpochTransitionRecord> versioned = streamStore1.getEpochTransition(scope, stream, null, executor).join();
        assertEquals(EpochTransitionRecord.EMPTY, versioned.getObject());
        assertEquals(4, getVersionNumber(versioned));
        assertEquals(2, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        streamStore1.close();
        streamStore2.close();
    }

    @Test(timeout = 30000)
    public void testScaleStateReset() {
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        String stream = "testResetState";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore.setState(scope, stream, State.ACTIVE, null, executor).join();

        ArrayList<Map.Entry<Double, Double>> newRange = new ArrayList<>();
        newRange.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        
        // start with manual scale
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                newRange, true, System.currentTimeMillis(), System.currentTimeMillis());
        streamStore.submitScale(scope, stream, Lists.newArrayList(0L),
                new ArrayList<>(newRange), System.currentTimeMillis(), null, null, executor).join();
        
        // perform scaling
        scaleRequestHandler.execute(event).join();
        long one = NameUtils.computeSegmentId(1, 1);
        assertEquals(State.ACTIVE, streamStore.getState(scope, stream, true, null, executor).join());
        assertEquals(1, streamStore.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        
        // now set the state to SCALING
        this.streamStore.setState(scope, stream, State.SCALING, null, executor).join();
        
        // rerun same manual scaling job. It should succeed after simply resetting the state back to active.
        scaleRequestHandler.execute(event).join();
        // verify that state is reset
        assertEquals(State.ACTIVE, streamStore.getState(scope, stream, true, null, executor).join());
        assertEquals(1, streamStore.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());

        // rerun same manual scaling job. This time it should not do anything at all. 
        scaleRequestHandler.execute(event).join();
        
        // run scale 2.. this time auto scale
        ScaleOpEvent event2 = new ScaleOpEvent(scope, stream, Lists.newArrayList(one),
                newRange, false, System.currentTimeMillis(), System.currentTimeMillis());
        scaleRequestHandler.execute(event2).join();
        this.streamStore.setState(scope, stream, State.SCALING, null, executor).join();

        // rerun same auto scaling job. 
        scaleRequestHandler.execute(event2).join();
        assertEquals(State.ACTIVE, streamStore.getState(scope, stream, true, null, executor).join());
        assertEquals(2, streamStore.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());

        // now set the state to SCALING and run a new scaling job. This should succeed.
        this.streamStore.setState(scope, stream, State.SCALING, null, executor).join();

        long two = NameUtils.computeSegmentId(2, 2);
        ScaleOpEvent event3 = new ScaleOpEvent(scope, stream, Lists.newArrayList(two), newRange, false,
                System.currentTimeMillis(), System.currentTimeMillis());
        scaleRequestHandler.execute(event3).join();
        assertEquals(State.ACTIVE, streamStore.getState(scope, stream, true, null, executor).join());
        assertEquals(3, streamStore.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
    }

    @Test(timeout = 30000)
    public void testScaleRange() throws ExecutionException, InterruptedException {
        // key range values taken from issue #2543
        StreamSegmentRecord segment = new StreamSegmentRecord(2, 1, 100L, 0.1706574888245243, 0.7085170563088633);
        doReturn(CompletableFuture.completedFuture(segment)).when(streamStore).getSegment(any(), any(), anyLong(), any(), any());

        AutoScaleTask requestHandler = new AutoScaleTask(streamMetadataTasks, streamStore, executor);
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        StreamRequestHandler multiplexer = new StreamRequestHandler(requestHandler, scaleRequestHandler, null,
                null, null, null, null, null, null, streamStore, null, executor);
        // Send number of splits = 1
        EventWriterMock writer = new EventWriterMock();
        streamMetadataTasks.setRequestEventWriter(writer);

        AutoScaleEvent scaleUpEvent = new AutoScaleEvent(scope, stream, NameUtils.computeSegmentId(2, 1),
                AutoScaleEvent.UP, System.currentTimeMillis(), 1, false, System.currentTimeMillis());
        assertTrue(Futures.await(multiplexer.process(scaleUpEvent, () -> false)));

        reset(streamStore);
        // verify that one scaleOp event is written into the stream
        assertEquals(1, writer.queue.size());
        ControllerEvent event = writer.queue.take();
        assertTrue(event instanceof ScaleOpEvent);
        ScaleOpEvent scaleOpEvent = (ScaleOpEvent) event;

        assertEquals(2, scaleOpEvent.getNewRanges().size());
        assertEquals(0.1706574888245243, scaleOpEvent.getNewRanges().get(0).getKey(), 0.0);
        assertEquals(0.7085170563088633, scaleOpEvent.getNewRanges().get(1).getValue(), 0.0);
        assertTrue(scaleOpEvent.getNewRanges().get(0).getValue().doubleValue() == scaleOpEvent.getNewRanges().get(1).getKey().doubleValue());
    }

    @Test(timeout = 30000)
    public void testWithExistingEpochTransition() {
        ScaleOperationTask scaleRequestHandler = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        String stream = "testETR";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore.setState(scope, stream, State.ACTIVE, null, executor).join();

        ArrayList<Map.Entry<Double, Double>> newRange = new ArrayList<>();
        newRange.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));

        // submit scale request. that will create an ETR.
        streamStore.submitScale(scope, stream, Lists.newArrayList(0L),
                new ArrayList<>(newRange), System.currentTimeMillis(), null, null, executor).join();

        // now post a scale event with different input
        newRange = new ArrayList<>();
        newRange.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRange.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        // case 1: manual scale
        ScaleOpEvent manual = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                newRange, true, System.currentTimeMillis(), System.currentTimeMillis());
        // run scaling workflow. This should do nothing and complete. 
        scaleRequestHandler.execute(manual).join();

        // case 2: auto scale
        ScaleOpEvent auto = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                newRange, false, System.currentTimeMillis(), System.currentTimeMillis());
        // run scaling workflow. This should throw Conflict Exception. 
        AssertExtensions.assertSuppliedFutureThrows("", () -> scaleRequestHandler.execute(auto),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.ConflictException);

        // verify that neither event led to any processing and stream is still at epoch 0 and active state.
        assertEquals(State.ACTIVE, streamStore.getState(scope, stream, true, null, executor).join());
        assertEquals(0, streamStore.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
    }

    private static class EventWriterMock implements EventStreamWriter<ControllerEvent> {
        BlockingQueue<ControllerEvent> queue = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Void> writeEvent(ControllerEvent event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void>  writeEvent(String routingKey, ControllerEvent event) {
            queue.add(event);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> writeEvents(String routingKey, List<ControllerEvent> events) {
            throw new NotImplementedException("mock doesnt require this");
        }

        @Override
        public EventWriterConfig getConfig() {
            return null;
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public void noteTime(long timestamp) {
            
        }
    }
}
