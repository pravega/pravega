/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.task.Stream;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.mocks.AckFutureMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.test.common.TestingServerStarter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for StreamTransactionMetadataTasks.
 */
@Slf4j
public class StreamTransactionMetadataTasksTest {
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
            Mockito.when(mockCommitWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(commitWriterResponses));

            EventStreamWriter<AbortEvent> mockAbortWriter = Mockito.mock(EventStreamWriter.class);
            Mockito.when(mockAbortWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(abortWriterResponses));

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
    public void commitAbortTests() {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();

        // Create stream and scope

        Controller.CreateScopeStatus scopeStatus = consumer.createScope(SCOPE).join();
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, scopeStatus.getStatus());

        Controller.CreateStreamStatus.Status streamStatus = streamMetadataTasks.createStream(SCOPE, STREAM,
                configuration1, System.currentTimeMillis()).join();
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS, streamStatus);

        // Create 2 transactions
        final long lease = 5000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 10000;

        VersionedTransactionData txData1 = streamTransactionMetadataTasks.createTxn(SCOPE, STREAM, lease,
                maxExecutionTime, scaleGracePeriod, null).join().getKey();
        VersionedTransactionData txData2 = streamTransactionMetadataTasks.createTxn(SCOPE, STREAM, lease,
                maxExecutionTime, scaleGracePeriod, null).join().getKey();

        // Commit the first one
        TxnStatus status = streamTransactionMetadataTasks.commitTxn(SCOPE, STREAM, txData1.getId(), null).join();
        Assert.assertEquals(TxnStatus.COMMITTING, status);

        // Abort the second one
        status = streamTransactionMetadataTasks.abortTxn(SCOPE, STREAM, txData2.getId(),
                txData2.getVersion(), null).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }
}
