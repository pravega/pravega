/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.server.controller.service.mocks.AckFutureMock;
import io.pravega.server.controller.service.mocks.SegmentHelperMock;
import io.pravega.server.controller.service.server.eventProcessor.AbortEvent;
import io.pravega.server.controller.service.server.eventProcessor.CommitEvent;
import io.pravega.server.controller.service.store.host.HostControllerStore;
import io.pravega.server.controller.service.store.host.HostStoreFactory;
import io.pravega.server.controller.service.store.host.impl.HostMonitorConfigImpl;
import io.pravega.server.controller.service.store.stream.StreamMetadataStore;
import io.pravega.server.controller.service.store.stream.StreamStoreFactory;
import io.pravega.server.controller.service.store.task.TaskMetadataStore;
import io.pravega.server.controller.service.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.server.controller.service.task.Stream.StreamMetadataTasks;
import io.pravega.server.controller.service.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.server.controller.service.task.Stream.WriteFailedException;
import io.pravega.server.controller.service.timeout.TimeoutService;
import io.pravega.server.controller.service.timeout.TimeoutServiceConfig;
import io.pravega.server.controller.service.timeout.TimerWheelTimeoutService;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.client.stream.impl.netty.ConnectionFactory;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;
import io.pravega.test.common.TestingServerStarter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for ControllerService With ZK Stream Store
 */
@Slf4j
public class ControllerServiceWithZKStreamTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService consumer;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionTasksMock streamTransactionMetadataTasks;
    private TimeoutService timeoutService;
    private ConnectionFactoryImpl connectionFactory;

    private static class SequenceAnswer<T> implements Answer<T> {

        private Iterator<T> resultIterator;

        // null is returned once the iterator is exhausted

        public SequenceAnswer(List<T> results) {
            this.resultIterator = results.iterator();
        }

        @Override
        public T answer(InvocationOnMock invocation) throws Throwable {
            if (resultIterator.hasNext()) {
                return resultIterator.next();
            } else {
                return null;
            }
        }
    }

    private static class StreamTransactionTasksMock extends StreamTransactionMetadataTasks {

        public StreamTransactionTasksMock(final StreamMetadataStore streamMetadataStore,
                                          final HostControllerStore hostControllerStore,
                                          final TaskMetadataStore taskMetadataStore,
                                          final SegmentHelper segmentHelper,
                                          final ScheduledExecutorService executor,
                                          final String hostId,
                                          final ConnectionFactory connectionFactory) {
            super(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper,
                    executor, hostId, connectionFactory);
        }

        public void initializeWriters(final List<AckFuture> commitWriterResponses,
                                      final List<AckFuture> abortWriterResponses) {
            EventStreamWriter<CommitEvent> mockCommitWriter = Mockito.mock(EventStreamWriter.class);
            Mockito.when(mockCommitWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(
                    commitWriterResponses));

            EventStreamWriter<AbortEvent> mockAbortWriter = Mockito.mock(EventStreamWriter.class);
            Mockito.when(mockAbortWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(
                    abortWriterResponses));

            this.commitStreamName = "commitStream";
            this.commitEventEventStreamWriter = mockCommitWriter;
            this.abortStreamName = "abortStream";
            this.abortEventEventStreamWriter = mockAbortWriter;
            this.setReady();
        }
    }

    @Before
    public void setup() {
        try {
            zkServer = new TestingServerStarter().start();
        } catch (Exception e) {
            log.error("Error starting ZK server", e);
        }
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory);
        streamTransactionMetadataTasks = new StreamTransactionTasksMock(streamStore, hostStore, taskMetadataStore,
                segmentHelperMock, executor, "host", connectionFactory);
        streamTransactionMetadataTasks.initializeWriters(getWriteResultSequence(5), getWriteResultSequence(5));
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelperMock, executor, null);
    }

    @After
    public void teardown() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        executor.shutdown();
    }

    @SneakyThrows
    private List<AckFuture> getWriteResultSequence(int count) {
        List<AckFuture> ackFutures = new ArrayList<>();
        for (int i = 0; i < count; i++) {

            AckFuture spy = Mockito.spy(new AckFutureMock(CompletableFuture.completedFuture(true)));
            Mockito.when(spy.get()).thenThrow(InterruptedException.class);
            ackFutures.add(spy);
            ackFutures.add(new AckFutureMock(FutureHelpers.failedFuture(new WriteFailedException())));
            ackFutures.add(new AckFutureMock(CompletableFuture.completedFuture(true)));
        }
        return ackFutures;
    }

    @Test(timeout = 5000)
    public void getSegmentsImmediatelyFollowingTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        //Start time  when stream is created.
        long start = System.currentTimeMillis();

        // Create stream and scope
        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE).join();
        assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());
        Controller.CreateStreamStatus streamStatus = consumer.createStream(configuration1, start).get();
        assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus.getStatus());

        List<Controller.SegmentRange> currentSegments = consumer.getCurrentSegments(SCOPE, STREAM).get();
        assertEquals(2, currentSegments.size());

        //scale segment 1 which has key range from 0.5 to 1.0 at time: start+20
        Map<Double, Double> keyRanges = new HashMap<>(2);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);

        Controller.ScaleResponse scaleStatus = consumer.scale(SCOPE, STREAM, Arrays.asList(1), keyRanges, start + 20)
                .get();
        assertEquals(Controller.ScaleResponse.ScaleStreamStatus.SUCCESS, scaleStatus.getStatus());

        //After scale the current number of segments is 3;
        List<Controller.SegmentRange> currentSegmentsAfterScale = consumer.getCurrentSegments(SCOPE, STREAM).get();
        assertEquals(3, currentSegmentsAfterScale.size());

        Map<Controller.SegmentRange, List<Integer>> successorsOfSeg1 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM, 1)).get();
        assertEquals(2, successorsOfSeg1.size()); //two segments follow segment 1

        Map<Controller.SegmentRange, List<Integer>> successorsOfSeg0 = consumer.getSegmentsImmediatelyFollowing(
                ModelHelper.createSegmentId(SCOPE, STREAM, 0)).get();
        assertEquals(0, successorsOfSeg0.size()); //no segments follow segment 0
    }
}
