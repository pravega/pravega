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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventSerializer;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.ConcurrentEventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for StreamTransactionMetadataTasks.
 */
@Slf4j
public class StreamTransactionMetadataTasksTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream1";

    boolean authEnabled = false;
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

    private RequestTracker requestTracker = new RequestTracker(true);

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
        connectionFactory = Mockito.mock(ConnectionFactory.class);
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock(hostStore, connectionFactory, AuthHelper.getDisabledAuthHelper());
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", requestTracker);
    }

    @After
    public void teardown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        txnTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @SneakyThrows
    private List<CompletableFuture<Void>> getWriteResultSequence(int count) {
        List<CompletableFuture<Void>> ackFutures = new ArrayList<>();
        for (int i = 0; i < count; i++) {

            CompletableFuture<Void> spy = spy(CompletableFuture.completedFuture(null));
            Mockito.when(spy.get()).thenThrow(InterruptedException.class);
            ackFutures.add(spy);
            ackFutures.add(Futures.failedFuture(new WriteFailedException()));
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
        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock,
                executor, "host");
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        // Create ControllerService.
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0).join());

        // Create 2 transactions
        final long lease = 5000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease, null).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease, null).join().getKey();

        // Commit the first one
        TxnStatus status = txnTasks.commitTxn(SCOPE, STREAM, txData1.getId(), null).join();
        Assert.assertEquals(TxnStatus.COMMITTING, status);

        // Abort the second one
        status = txnTasks.abortTxn(SCOPE, STREAM, txData2.getId(),
                txData2.getVersion(), null).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test(timeout = 60000)
    public void failOverTests() throws Exception {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();
        EventStreamReader<CommitEvent> commitReader = commitWriter.getReader();
        EventStreamReader<AbortEvent> abortReader = abortWriter.getReader();

        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host");

        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        // Create test scope and stream.
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis()).join());

        // Set up txn task for creating transactions from a failedHost.
        @Cleanup
        StreamTransactionMetadataTasks failedTxnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, 
                executor, "failedHost");
        failedTxnTasks.initializeStreamWriters("commitStream", new EventStreamWriterMock<>(), "abortStream",
                new EventStreamWriterMock<>());

        // Create 3 transactions from failedHost.
        VersionedTransactionData tx1 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, null).join().getKey();
        VersionedTransactionData tx2 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, null).join().getKey();
        VersionedTransactionData tx3 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, null).join().getKey();
        VersionedTransactionData tx4 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, null).join().getKey();

        // Ping another txn from failedHost.
        PingTxnStatus pingStatus = failedTxnTasks.pingTxn(SCOPE, STREAM, tx4.getId(), 10000, null).join();
        VersionedTransactionData tx4get = streamStore.getTransactionData(SCOPE, STREAM, tx4.getId(), null, executor).join();

        // Validate versions of all txn
        Assert.assertEquals(0, tx1.getVersion().asIntVersion().getIntValue());
        Assert.assertEquals(0, tx2.getVersion().asIntVersion().getIntValue());
        Assert.assertEquals(0, tx3.getVersion().asIntVersion().getIntValue());
        Assert.assertEquals(1, tx4get.getVersion().asIntVersion().getIntValue());
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
        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host");
        TxnSweeper txnSweeper = new TxnSweeper(streamStore, txnTasks, 100, executor);

        // Before initializing, txnSweeper.sweepFailedHosts would throw an error
        AssertExtensions.assertFutureThrows("IllegalStateException before initialization",
                txnSweeper.sweepFailedProcesses(() -> Collections.singleton("host")),
                ex -> ex instanceof IllegalStateException);

        // Initialize stream writers.
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream", abortWriter);

        // Validate that txnTasks is ready.
        assertTrue(txnTasks.isReady());

        // Sweep txns that were being managed by failedHost.
        txnSweeper.sweepFailedProcesses(() -> Collections.singleton("host")).join();

        // Validate that sweeping completes correctly.
        Set<String> listOfHosts = streamStore.listHostsOwningTxn().join();
        Assert.assertEquals(1, listOfHosts.size());
        Assert.assertTrue(listOfHosts.contains("host"));
        Assert.assertEquals(TxnStatus.OPEN,
                streamStore.transactionStatus(SCOPE, STREAM, tx1.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.COMMITTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx2.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.ABORTING,
                streamStore.transactionStatus(SCOPE, STREAM, tx3.getId(), null, executor).join());
        Assert.assertEquals(TxnStatus.OPEN,
                streamStore.transactionStatus(SCOPE, STREAM, tx4.getId(), null, executor).join());

        VersionedTransactionData txnData = streamStore.getTransactionData(SCOPE, STREAM, tx1.getId(), null, executor).join();
        Assert.assertEquals(1, txnData.getVersion().asIntVersion().getIntValue());
        txnData = streamStore.getTransactionData(SCOPE, STREAM, tx4.getId(), null, executor).join();
        Assert.assertEquals(2, txnData.getVersion().asIntVersion().getIntValue());

        // Create commit and abort event processors.
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", commitReader, commitWriter,
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, txnTasks, executor, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, executor, processedAbortEvents), executor));

        // Wait until the commit event is processed and ensure that the txn state is COMMITTED.
        CommitEvent commitEvent = processedCommitEvents.take();
        assertEquals(tx2.getEpoch(), commitEvent.getEpoch());
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
        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host");
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream", abortWriter);

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis()).join());

        // Create 2 transactions
        final long lease = 5000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease, null).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease, null).join().getKey();

        UUID tx1 = txData1.getId();
        UUID tx2 = txData2.getId();
        Version tx2Version = txData2.getVersion();

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
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", commitReader, commitWriter,
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, txnTasks, executor, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, executor, processedAbortEvents), executor));

        // Wait until the commit event is processed and ensure that the txn state is COMMITTED.
        CommitEvent commitEvent = processedCommitEvents.take();
        assertEquals(0, commitEvent.getEpoch());
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
        txnTasks = new StreamTransactionMetadataTasks(streamStore, 
                SegmentHelperMock.getFailingSegmentHelperMock(hostStore, connectionFactory, new AuthHelper(this.authEnabled, "secret")), executor, "host");
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        // Create ControllerService.
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0).join());

        // Create partial transaction
        final long lease = 10000;

        AssertExtensions.assertFutureThrows("Transaction creation fails, although a new txn id gets added to the store",
                txnTasks.createTxn(SCOPE, STREAM, lease, null),
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

    @Test(timeout = 10000)
    public void txnCreationTest() {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();

        StreamMetadataStore streamStoreMock = spy(StreamStoreFactory.createZKStore(zkClient, executor));

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, 
                SegmentHelperMock.getSegmentHelperMock(hostStore, connectionFactory,
                        new AuthHelper(this.authEnabled, "secret")), executor, "host");
        txnTasks.initializeStreamWriters("commitStream", commitWriter, "abortStream",
                abortWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        streamStoreMock.createScope(SCOPE).join();
        streamStoreMock.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis(), null, executor).join();
        streamStoreMock.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        // mock streamMetadataStore.generateTxnId should throw excecption first time.
        // Note: it should be retried.
        // now the id should have been generated
        doAnswer(new Answer<CompletableFuture<UUID>>() {
            AtomicInteger count = new AtomicInteger(0);
            @Override
            public CompletableFuture<UUID> answer(InvocationOnMock invocation) throws Throwable {

                // first time throw exception.
                if (count.getAndIncrement() == 0) {
                    return Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT, "write conflict on counter update"));
                }

                // subsequent times call origin method
                @SuppressWarnings("unchecked")
                CompletableFuture<UUID> future = (CompletableFuture<UUID>) invocation.callRealMethod();
                return future;
            }
        }).when(streamStoreMock).generateTransactionId(eq(SCOPE), eq(STREAM), any(), any());

        doAnswer(new Answer<CompletableFuture<VersionedTransactionData>>() {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public CompletableFuture<VersionedTransactionData> answer(InvocationOnMock invocation) throws Throwable {
                // first time throw exception.
                if (count.getAndIncrement() == 0) {
                    return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Epoch not found"));
                }

                // subsequent times call origin method
                @SuppressWarnings("unchecked")
                CompletableFuture<VersionedTransactionData> future = (CompletableFuture<VersionedTransactionData>) invocation.callRealMethod();
                return future;
            }
        }).when(streamStoreMock).createTransaction(any(), any(), any(), anyLong(), anyLong(), any(), any());
        Pair<VersionedTransactionData, List<Segment>> txn = txnTasks.createTxn(SCOPE, STREAM, 10000L, null).join();

        // verify that generate transaction id is called 3 times
        verify(streamStoreMock, times(3)).generateTransactionId(any(), any(), any(), any());
        // verify that create transaction is called 2 times
        verify(streamStoreMock, times(2)).createTransaction(any(), any(), any(), anyLong(), anyLong(), any(), any());

        // verify that the txn id that is generated is of type ""
        UUID txnId = txn.getKey().getId();
        assertEquals(0, (int) (txnId.getMostSignificantBits() >> 32));
        assertEquals(2, txnId.getLeastSignificantBits());
    }
    
    @Test(timeout = 10000)
    public void writerInitializationTest() {
        StreamMetadataStore streamStoreMock = StreamStoreFactory.createZKStore(zkClient, executor);

        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, hostStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", connectionFactory,
                new AuthHelper(this.authEnabled, "secret"));

        streamStore.createScope(SCOPE).join();
        streamStore.createStream(SCOPE, STREAM, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 1L, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        CompletableFuture<Pair<VersionedTransactionData, List<Segment>>> createFuture = txnTasks.createTxn(SCOPE, STREAM, 100L, null);

        // create and ping transactions should not wait for writer initialization and complete immediately.
        createFuture.join();
        assertTrue(Futures.await(createFuture));
        UUID txnId = createFuture.join().getKey().getId();
        CompletableFuture<PingTxnStatus> pingFuture = txnTasks.pingTxn(SCOPE, STREAM, txnId, 100L, null);
        assertTrue(Futures.await(pingFuture));

        CompletableFuture<TxnStatus> commitFuture = txnTasks.commitTxn(SCOPE, STREAM, txnId, null);
        assertFalse(commitFuture.isDone());

        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();

        txnTasks.initializeStreamWriters("", commitWriter, "", abortWriter);
        assertTrue(Futures.await(commitFuture));
        UUID txnId2 = txnTasks.createTxn(SCOPE, STREAM, 100L, null).join().getKey().getId();
        assertTrue(Futures.await(txnTasks.abortTxn(SCOPE, STREAM, txnId2, null, null)));

    }
    
    @Test(timeout = 10000)
    public void writerRoutingKeyTest() throws InterruptedException {
        StreamMetadataStore streamStoreMock = StreamStoreFactory.createZKStore(zkClient, executor);

        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, hostStore,
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", connectionFactory,
                new AuthHelper(this.authEnabled, "secret"));

        streamStore.createScope(SCOPE).join();
        streamStore.createStream(SCOPE, STREAM, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 1L, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        TestEventStreamWriter<CommitEvent> commitWriter = new TestEventStreamWriter<>();
        TestEventStreamWriter<AbortEvent> abortWriter = new TestEventStreamWriter<>();

        txnTasks.initializeStreamWriters("", commitWriter, "", abortWriter);

        UUID txnId = UUID.randomUUID();
        txnTasks.writeAbortEvent(SCOPE, STREAM, 0, txnId, TxnStatus.ABORTING).join();
        Pair<String, AbortEvent> request = abortWriter.requestsReceived.take();
        assertEquals(request.getKey(), request.getValue().getKey());
        txnTasks.writeAbortEvent(new AbortEvent(SCOPE, STREAM, 0, txnId)).join();
        Pair<String, AbortEvent> request2 = abortWriter.requestsReceived.take();
        assertEquals(request2.getKey(), request2.getValue().getKey());
        // verify that both use the same key
        assertEquals(request.getKey(), request2.getKey());

        txnTasks.writeCommitEvent(SCOPE, STREAM, 0, txnId, TxnStatus.COMMITTING).join();
        Pair<String, CommitEvent> request3 = commitWriter.requestsReceived.take();
        assertEquals(request3.getKey(), request3.getValue().getKey());
        txnTasks.writeCommitEvent(new CommitEvent(SCOPE, STREAM, 0)).join();
        Pair<String, CommitEvent> request4 = commitWriter.requestsReceived.take();
        assertEquals(request4.getKey(), request4.getValue().getKey());
        // verify that both use the same key
        assertEquals(request3.getKey(), request4.getKey());
    }

    private static class TestEventStreamWriter<T extends ControllerEvent> implements EventStreamWriter<T> {
        LinkedBlockingQueue<Pair<String, T>> requestsReceived = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Void> writeEvent(T event) {
            requestsReceived.offer(new ImmutablePair<>(null, event));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> writeEvent(String routingKey, T event) {
            requestsReceived.offer(new ImmutablePair<>(routingKey, event));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Transaction<T> beginTxn() {
            return null;
        }

        @Override
        public Transaction<T> getTxn(UUID transactionId) {
            return null;
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
    }

    private <T extends ControllerEvent> void createEventProcessor(final String readerGroupName,
                              final String streamName,
                              final EventStreamReader<T> reader,
                              final EventStreamWriter<T> writer,
                              Supplier<EventProcessor<T>> factory) throws CheckpointStoreException {
        EventStreamClientFactory clientFactory = Mockito.mock(EventStreamClientFactory.class);
        Mockito.when(clientFactory.<T>createReader(anyString(), anyString(), any(), any())).thenReturn(reader);
        Mockito.when(clientFactory.<T>createEventWriter(anyString(), any(), any())).thenReturn(writer);

        ReaderGroup readerGroup = Mockito.mock(ReaderGroup.class);
        Mockito.when(readerGroup.getGroupName()).thenReturn(readerGroupName);

        ReaderGroupManager readerGroupManager = Mockito.mock(ReaderGroupManager.class);

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
                .serializer(new EventSerializer<>())
                .supplier(factory)
                .build();

        system.createEventProcessorGroup(config, CheckpointStoreFactory.createInMemoryStore());
    }

    public static class RegularBookKeeperLogTests extends StreamTransactionMetadataTasksTest {
        @Override
        @Before
        public void setup() {
            this.authEnabled = true;
            super.setup();
        }
    }
}

