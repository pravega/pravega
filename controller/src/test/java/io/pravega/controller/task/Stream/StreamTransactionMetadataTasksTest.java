/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.AbortEvent;
import io.pravega.controller.server.eventProcessor.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.CommitEvent;
import io.pravega.controller.server.eventProcessor.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.ConcurrentEventProcessor;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.AssertExtensions;
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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
        streamMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        executor.shutdown();
    }

    @SneakyThrows
    private List<CompletableFuture<Void>> getWriteResultSequence(int count) {
        List<CompletableFuture<Void>> ackFutures = new ArrayList<>();
        for (int i = 0; i < count; i++) {

            CompletableFuture<Void> spy = Mockito.spy(CompletableFuture.completedFuture(null));
            Mockito.when(spy.get()).thenThrow(InterruptedException.class);
            ackFutures.add(spy);
            ackFutures.add(FutureHelpers.failedFuture(new WriteFailedException()));
            ackFutures.add(CompletableFuture.completedFuture(null));
        }
        return ackFutures;
    }

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void commitAbortTests() {
        // Create mock writer objects.
        final List<CompletableFuture<Void>> commitWriterResponses = getWriteResultSequence(5);
        final List<CompletableFuture<Void>> abortWriterResponses = getWriteResultSequence(5);
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
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0).join());

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

    @Test
    public void failOverTests() throws CheckpointStoreException, InterruptedException {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();
        EventStreamReader<CommitEvent> commitReader = commitWriter.getReader();
        EventStreamReader<AbortEvent> abortReader = abortWriter.getReader();

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        // Create test scope and stream.
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis()).join());

        // Set up txn task for creating transactions from a failedHost.
        StreamTransactionMetadataTasks failedTxnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                segmentHelperMock, executor, "failedHost", connectionFactory);
        failedTxnTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(), "abortStream",
                new EventStreamWriterMock<>());

        // Create 3 transactions from failedHost.
        VersionedTransactionData tx1 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 10000, 10000, null).join().getKey();
        VersionedTransactionData tx2 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 10000, 10000, null).join().getKey();
        VersionedTransactionData tx3 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 10000, 10000, null).join().getKey();

        // Ping another txn from failedHost.
        UUID txnId = UUID.randomUUID();
        streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 30000, 30000, null, executor).join();
        PingTxnStatus pingStatus = failedTxnTasks.pingTxn(SCOPE, STREAM, txnId, 10000, null).join();
        VersionedTransactionData tx4 = streamStore.getTransactionData(SCOPE, STREAM, txnId, null, executor).join();

        // Validate versions of all txn
        Assert.assertEquals(0, tx1.getVersion());
        Assert.assertEquals(0, tx2.getVersion());
        Assert.assertEquals(0, tx3.getVersion());
        Assert.assertEquals(1, tx4.getVersion());
        Assert.assertEquals(PingTxnStatus.Status.OK, pingStatus.getStatus());

        // Validate the txn index.
        Assert.assertEquals(1, streamStore.listHostsOwningTxn().join().size());

        // Change state of one txn to COMMITTING.
        TxnStatus txnStatus2 = streamStore.sealTransaction(SCOPE, STREAM, tx2.getId(), true, Optional.empty(),
                null, executor).thenApply(AbstractMap.SimpleEntry::getKey).join();
        Assert.assertEquals(TxnStatus.COMMITTING, txnStatus2);

        // Change state of another txn to ABORTING.
        TxnStatus txnStatus3 = streamStore.sealTransaction(SCOPE, STREAM, tx3.getId(), false, Optional.empty(),
                null, executor).thenApply(AbstractMap.SimpleEntry::getKey).join();
        Assert.assertEquals(TxnStatus.ABORTING, txnStatus3);

        // Create transaction tasks for sweeping txns from failedHost.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, segmentHelperMock, executor, "host",
                connectionFactory);
        TxnSweeper txnSweeper = new TxnSweeper(streamStore, txnTasks, 100, executor);

        // Before initializing, txnSweeper.sweepFailedHosts would throw an error
        AssertExtensions.assertThrows("IllegalStateException before initialization",
                txnSweeper.sweepFailedProcesses(() -> Collections.singleton("host")),
                ex -> ex instanceof IllegalStateException);

        // Initialize stream writers.
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream", abortWriter);

        // Validate that txnTasks is ready.
        assertTrue(txnTasks.isReady());

        // Sweep txns that were being managed by failedHost.
        txnSweeper.sweepFailedProcesses(() -> Collections.singleton("host")).join();

        // Validate that sweeping completes correctly.
        Assert.assertEquals(0, streamStore.listHostsOwningTxn().join().size());
        Assert.assertEquals(TxnStatus.ABORTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx1.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.COMMITTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx2.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.ABORTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx3.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.ABORTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx4.getId(), null, executor).join());

        // Create commit and abort event processors.
        ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", commitReader, commitWriter,
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedAbortEvents), executor));

        // Wait until the commit event is processed and ensure that the txn state is COMMITTED.
        CommitEvent commitEvent = processedCommitEvents.take();
        assertEquals(tx2.getId(), commitEvent.getTxid());
        assertEquals(TxnStatus.COMMITTED, streamStore.transactionStatus(SCOPE, STREAM, tx2.getId(), null, executor).join());

        // Wait until 3 abort events are processed and ensure that the txn state is ABORTED.
        Predicate<AbortEvent> predicate = event -> event.getTxid().equals(tx1.getId()) ||
                event.getTxid().equals(tx3.getId()) || event.getTxid().equals(tx4.getId());

        AbortEvent abortEvent1 = processedAbortEvents.take();
        assertTrue(predicate.test(abortEvent1));
        AbortEvent abortEvent2 = processedAbortEvents.take();
        assertTrue(predicate.test(abortEvent2));
        AbortEvent abortEvent3 = processedAbortEvents.take();
        assertTrue(predicate.test(abortEvent3));

        assertEquals(TxnStatus.ABORTED, streamStore.transactionStatus(SCOPE, STREAM, tx1.getId(), null, executor).join());
        assertEquals(TxnStatus.ABORTED, streamStore.transactionStatus(SCOPE, STREAM, tx3.getId(), null, executor).join());
        assertEquals(TxnStatus.ABORTED, streamStore.transactionStatus(SCOPE, STREAM, tx4.getId(), null, executor).join());
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

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
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
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, hostStore, executor, segmentHelperMock,
                        connectionFactory, processedAbortEvents), executor));

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

    @Test(timeout = 10000)
    public void partialTxnCreationTest() {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, hostStore,
                SegmentHelperMock.getFailingSegmentHelperMock(), executor, "host", connectionFactory);
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        // Create ControllerService.
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder()
                .scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0).join());

        // Create partial transaction
        final long lease = 10000;
        final long maxExecutionTime = 10000;
        final long scaleGracePeriod = 10000;

        AssertExtensions.assertThrows("Transaction creation fails, although a new txn id gets added to the store",
                txnTasks.createTxn(SCOPE, STREAM, lease, maxExecutionTime, scaleGracePeriod, null),
                e -> e instanceof RuntimeException);

        // Ensure that exactly one transaction is active on the stream.
        Set<UUID> txns = streamStore.getActiveTxns(SCOPE, STREAM, null, executor).join().keySet();
        assertEquals(1, txns.size());

        // Ensure that transaction state is OPEN.
        UUID txn1 = txns.stream().findFirst().get();
        assertEquals(TxnStatus.OPEN, streamStore.transactionStatus(SCOPE, STREAM, txn1, null, executor).join());

        // Ensure that timeout service knows about the transaction.
        assertTrue(txnTasks.getTimeoutService().containsTxn(SCOPE, STREAM, txn1));
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
