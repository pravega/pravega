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

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.mocks.AckFutureMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.AbortEventProcessor;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import io.pravega.controller.server.eventProcessor.CommitEventProcessor;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
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
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.netty.ConnectionFactory;
import io.pravega.shared.controller.event.ControllerEvent;
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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
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

    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;
    private SegmentHelper segmentHelperMock;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks txnTasks;
    private TimeoutService timeoutService;
    private ConnectionFactory connectionFactory;

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

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = Mockito.mock(ConnectionFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory);
    }

    @After
    public void teardown() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        streamMetadataTasks.close();
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
    @SuppressWarnings("unchecked")
    public void commitAbortTests() {
        // Create mock writer objects.
        final List<AckFuture> commitWriterResponses = getWriteResultSequence(5);
        final List<AckFuture> abortWriterResponses = getWriteResultSequence(5);
        EventStreamWriter<CommitEvent> commitWriter = Mockito.mock(EventStreamWriter.class);
        Mockito.when(commitWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(commitWriterResponses));
        EventStreamWriter<AbortEvent> abortWriter = Mockito.mock(EventStreamWriter.class);
        Mockito.when(abortWriter.writeEvent(anyString(), any())).thenAnswer(new SequenceAnswer<>(abortWriterResponses));

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, segmentHelperMock,
                executor, "host", connectionFactory);
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        // Create ControllerService.
        timeoutService = new TimerWheelTimeoutService(txnTasks, TimeoutServiceConfig.defaultConfig());
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                timeoutService, segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis()).join());

        // Create 2 transactions
        final long lease = 5000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 10000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease,
                maxExecutionTime, scaleGracePeriod, null).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease,
                maxExecutionTime, scaleGracePeriod, null).join().getKey();

        // Commit the first one
        TxnStatus status = txnTasks.commitTxn(SCOPE, STREAM, txData1.getId(), null).join();
        Assert.assertEquals(TxnStatus.COMMITTING, status);

        // Abort the second one
        status = txnTasks.abortTxn(SCOPE, STREAM, txData2.getId(),
                txData2.getVersion(), null).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test(timeout = 10000)
    public void idempotentOperationsTests() throws CheckpointStoreException, InterruptedException {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();
        EventStreamReader<CommitEvent> commitReader = commitWriter.getReader();
        EventStreamReader<AbortEvent> abortReader = abortWriter.getReader();

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, segmentHelperMock, executor, "host",
                connectionFactory);
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream", abortWriter);

        timeoutService = new TimerWheelTimeoutService(txnTasks, TimeoutServiceConfig.defaultConfig());
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks, timeoutService,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis()).join());

        // Create 2 transactions
        final long lease = 5000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 10000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease, maxExecutionTime, scaleGracePeriod,
                null).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease, maxExecutionTime, scaleGracePeriod,
                null).join().getKey();

        UUID tx1 = txData1.getId();
        UUID tx2 = txData2.getId();
        int tx2Version = txData2.getVersion();

        // Commit the first one
        Assert.assertEquals(TxnStatus.COMMITTING, txnTasks.commitTxn(SCOPE, STREAM, tx1, null).join());

        // Ensure that transaction state is COMMITTING.
        assertEquals(TxnStatus.COMMITTING, streamStore.transactionStatus(SCOPE, STREAM, tx1, null, executor).join());

        // Abort the second one
        Assert.assertEquals(TxnStatus.ABORTING, txnTasks.abortTxn(SCOPE, STREAM, tx2, tx2Version, null).join());

        // Ensure that transactions state is ABORTING.
        assertEquals(TxnStatus.ABORTING, streamStore.transactionStatus(SCOPE, STREAM, tx2, null, executor).join());

        // Ensure that commit (resp. abort) transaction tasks are idempotent
        // when transaction is in COMMITTING state (resp. ABORTING state).
        assertEquals(TxnStatus.COMMITTING, txnTasks.commitTxn(SCOPE, STREAM, tx1, null).join());
        assertEquals(TxnStatus.ABORTING, txnTasks.abortTxn(SCOPE, STREAM, tx2, null, null).join());

        // Create commit and abort event processors.
        ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", commitReader, commitWriter,
                () -> new CommitEventProcessor(streamStore, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedCommitEvents));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new AbortEventProcessor(streamStore, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedAbortEvents));

        // Wait until the commit event is processed and ensure that the txn state is COMMITTED.
        CommitEvent commitEvent = processedCommitEvents.take();
        assertEquals(tx1, commitEvent.getTxid());
        assertEquals(TxnStatus.COMMITTED, streamStore.transactionStatus(SCOPE, STREAM, tx1, null, executor).join());

        // Wait until the abort event is processed and ensure that the txn state is ABORTED.
        AbortEvent abortEvent = processedAbortEvents.take();
        assertEquals(tx2, abortEvent.getTxid());
        assertEquals(TxnStatus.ABORTED, streamStore.transactionStatus(SCOPE, STREAM, tx2, null, executor).join());

        // Ensure that commit (resp. abort) transaction tasks are idempotent
        // even after transaction is committed (resp. aborted)
        assertEquals(TxnStatus.COMMITTED, txnTasks.commitTxn(SCOPE, STREAM, tx1, null).join());
        assertEquals(TxnStatus.ABORTED, txnTasks.abortTxn(SCOPE, STREAM, tx2, null, null).join());
    }

    private <T extends ControllerEvent>
    void createEventProcessor(final String readerGroupName,
                              final String streamName,
                              final EventStreamReader<T> reader,
                              final EventStreamWriter<T> writer,
                              Supplier<EventProcessor<T>> factory) throws CheckpointStoreException {
        ClientFactory clientFactory = Mockito.mock(ClientFactory.class);
        Mockito.when(clientFactory.<T>createReader(anyString(), anyString(), any(), any())).thenReturn(reader);
        Mockito.when(clientFactory.<T>createEventWriter(anyString(), any(), any())).thenReturn(writer);

        ReaderGroup readerGroup = Mockito.mock(ReaderGroup.class);
        Mockito.when(readerGroup.getGroupName()).thenReturn(readerGroupName);

        ReaderGroupManager readerGroupManager = Mockito.mock(ReaderGroupManager.class);
        Mockito.when(readerGroupManager.createReaderGroup(anyString(), any(ReaderGroupConfig.class), any()))
                .then(invocation -> readerGroup);

        EventProcessorSystemImpl system = new EventProcessorSystemImpl("system", "host", SCOPE, clientFactory, readerGroupManager);

        EventProcessorGroupConfig eventProcessorConfig = EventProcessorGroupConfigImpl.builder()
                .eventProcessorCount(1)
                .readerGroupName(readerGroupName)
                .streamName(streamName)
                .checkpointConfig(CheckpointConfig.periodic(1, 1))
                .build();

        EventProcessorConfig<T> config = EventProcessorConfig.<T>builder()
                .config(eventProcessorConfig)
                .decider(ExceptionHandler.DEFAULT_EXCEPTION_HANDLER)
                .serializer(new JavaSerializer<>())
                .supplier(factory)
                .build();

        system.createEventProcessorGroup(config, CheckpointStoreFactory.createInMemoryStore());
    }
}
