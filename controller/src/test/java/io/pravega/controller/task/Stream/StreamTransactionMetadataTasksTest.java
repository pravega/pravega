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
package io.pravega.controller.task.Stream;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
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
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.NotImplementedException;
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
import org.mockito.Mock;
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
import static org.mockito.Mockito.mock;
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
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    private ControllerService consumer;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataStore streamStore;
    private BucketStore bucketStore;
    private HostControllerStore hostStore;
    private SegmentHelper segmentHelperMock;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks txnTasks;
    private ConnectionFactory connectionFactory;
    @Mock
    private KVTableMetadataStore kvtStore;
    @Mock
    private TableMetadataTasks kvtMetadataTasks;

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

        bucketStore = StreamStoreFactory.createInMemoryBucketStore();

        connectionFactory = Mockito.mock(ConnectionFactory.class);
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", GrpcAuthHelper.getDisabledAuthHelper());
        StreamMetrics.initialize();
        TransactionMetrics.initialize();
    }

    @After
    public void teardown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        txnTasks.close();
        zkClient.close();
        zkServer.close();
        connectionFactory.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
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
                executor, "host", GrpcAuthHelper.getDisabledAuthHelper());
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        // Create ControllerService.
        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null, requestTracker);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE, 0L).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0, 0L).join());

        // Create 2 transactions
        final long lease = 5000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease, 0L, 1024 * 1024L).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease, 0L, 1024 * 1024L).join().getKey();

        // Commit the first one
        TxnStatus status = txnTasks.commitTxn(SCOPE, STREAM, txData1.getId(), 0L).join();
        Assert.assertEquals(TxnStatus.COMMITTING, status);

        // Abort the second one
        status = txnTasks.abortTxn(SCOPE, STREAM, txData2.getId(),
                txData2.getVersion(), 0L).join();
        Assert.assertEquals(TxnStatus.ABORTING, status);
    }

    @Test(timeout = 60000)
    public void failOverTests() throws Exception {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();
        EventStreamReader<CommitEvent> commitReader = commitWriter.getReader();
        EventStreamReader<AbortEvent> abortReader = abortWriter.getReader();

        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host",
                GrpcAuthHelper.getDisabledAuthHelper());

        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null, requestTracker);

        // Create test scope and stream.
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE, 0L).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis(), 0L).join());

        // Set up txn task for creating transactions from a failedHost.
        @Cleanup
        StreamTransactionMetadataTasks failedTxnTasks = new StreamTransactionMetadataTasks(streamStore, 
                segmentHelperMock, executor, "failedHost", GrpcAuthHelper.getDisabledAuthHelper());
        failedTxnTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        // Create 3 transactions from failedHost.
        VersionedTransactionData tx1 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 0L, 0L).join().getKey();
        VersionedTransactionData tx2 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 0L, 0L).join().getKey();
        VersionedTransactionData tx3 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 0L, 0L).join().getKey();
        VersionedTransactionData tx4 = failedTxnTasks.createTxn(SCOPE, STREAM, 10000, 0L, 0L).join().getKey();

        // Ping another txn from failedHost.
        PingTxnStatus pingStatus = failedTxnTasks.pingTxn(SCOPE, STREAM, tx4.getId(), 10000, 0L).join();
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
                "", Long.MIN_VALUE, null, executor).thenApply(AbstractMap.SimpleEntry::getKey).join();
        Assert.assertEquals(TxnStatus.COMMITTING, txnStatus2);

        // Change state of another txn to ABORTING.
        TxnStatus txnStatus3 = streamStore.sealTransaction(SCOPE, STREAM, tx3.getId(), false, Optional.empty(),
                "", Long.MIN_VALUE, null, executor).thenApply(AbstractMap.SimpleEntry::getKey).join();
        Assert.assertEquals(TxnStatus.ABORTING, txnStatus3);

        // Create transaction tasks for sweeping txns from failedHost.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host",
                GrpcAuthHelper.getDisabledAuthHelper());
        TxnSweeper txnSweeper = new TxnSweeper(streamStore, txnTasks, 100, executor);

        // Before initializing, txnSweeper.sweepFailedHosts would throw an error
        AssertExtensions.assertFutureThrows("IllegalStateException before initialization",
                txnSweeper.sweepFailedProcesses(() -> Collections.singleton("host")),
                ex -> ex instanceof IllegalStateException);

        // Initialize stream writers.
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

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
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, txnTasks, 
                        bucketStore, executor, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, executor, 
                        processedAbortEvents), executor));

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
        txnTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor, "host",
                GrpcAuthHelper.getDisabledAuthHelper());
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null, requestTracker);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE, 0L).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis(), 0L).join());

        // Create 2 transactions
        final long lease = 5000;

        VersionedTransactionData txData1 = txnTasks.createTxn(SCOPE, STREAM, lease, 0L, 1024 * 1024L).join().getKey();
        VersionedTransactionData txData2 = txnTasks.createTxn(SCOPE, STREAM, lease, 0L, 1024 * 1024L).join().getKey();

        UUID tx1 = txData1.getId();
        UUID tx2 = txData2.getId();
        Version tx2Version = txData2.getVersion();

        // Commit the first one
        Assert.assertEquals(TxnStatus.COMMITTING, txnTasks.commitTxn(SCOPE, STREAM, tx1, 0L).join());

        // Ensure that transaction state is COMMITTING.
        assertEquals(TxnStatus.COMMITTING, streamStore.transactionStatus(SCOPE, STREAM, tx1, null, executor).join());

        // Abort the second one
        Assert.assertEquals(TxnStatus.ABORTING, txnTasks.abortTxn(SCOPE, STREAM, tx2, tx2Version, 0L).join());

        // Ensure that transactions state is ABORTING.
        assertEquals(TxnStatus.ABORTING, streamStore.transactionStatus(SCOPE, STREAM, tx2, null, executor).join());

        // Ensure that commit (resp. abort) transaction tasks are idempotent
        // when transaction is in COMMITTING state (resp. ABORTING state).
        assertEquals(TxnStatus.COMMITTING, txnTasks.commitTxn(SCOPE, STREAM, tx1, 0L).join());
        assertEquals(TxnStatus.ABORTING, txnTasks.abortTxn(SCOPE, STREAM, tx2, null, 0L).join());

        // Create commit and abort event processors.
        BlockingQueue<CommitEvent> processedCommitEvents = new LinkedBlockingQueue<>();
        BlockingQueue<AbortEvent> processedAbortEvents = new LinkedBlockingQueue<>();
        createEventProcessor("commitRG", "commitStream", commitReader, commitWriter,
                () -> new ConcurrentEventProcessor<>(new CommitRequestHandler(streamStore, streamMetadataTasks, txnTasks, 
                        bucketStore, executor, processedCommitEvents), executor));
        createEventProcessor("abortRG", "abortStream", abortReader, abortWriter,
                () -> new ConcurrentEventProcessor<>(new AbortRequestHandler(streamStore, streamMetadataTasks, executor,
                        processedAbortEvents), executor));

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
        assertEquals(TxnStatus.COMMITTED, txnTasks.commitTxn(SCOPE, STREAM, tx1, 0L).join());
        assertEquals(TxnStatus.ABORTED, txnTasks.abortTxn(SCOPE, STREAM, tx2, null, 0L).join());
    }

    @Test(timeout = 10000)
    public void partialTxnCreationTest() {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStore, 
                SegmentHelperMock.getFailingSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 600));
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        // Create ControllerService.
        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, txnTasks,
                segmentHelperMock, executor, null, requestTracker);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        Assert.assertEquals(Controller.CreateScopeStatus.Status.SUCCESS, consumer.createScope(SCOPE, 0L).join().getStatus());
        Assert.assertEquals(Controller.CreateStreamStatus.Status.SUCCESS,
                streamMetadataTasks.createStream(SCOPE, STREAM, configuration1, 0, 0L).join());

        // Create partial transaction
        final long lease = 10000;

        AssertExtensions.assertFutureThrows("Transaction creation fails, although a new txn id gets added to the store",
                txnTasks.createTxn(SCOPE, STREAM, lease, 0L, 1024 * 1024L),
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
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 600));
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        streamStoreMock.createScope(SCOPE, null, executor).join();
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
                    return Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT, 
                            "write conflict on counter update"));
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
                    return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, 
                            "Epoch not found"));
                }

                // subsequent times call origin method
                @SuppressWarnings("unchecked")
                CompletableFuture<VersionedTransactionData> future = (CompletableFuture<VersionedTransactionData>)
                        invocation.callRealMethod();
                return future;
            }
        }).when(streamStoreMock).createTransaction(any(), any(), any(), anyLong(), anyLong(), any(), any());
        Pair<VersionedTransactionData, List<StreamSegmentRecord>> txn = txnTasks.createTxn(SCOPE, STREAM, 10000L, 
                0L, 1024 * 1024L).join();

        // verify that generate transaction id is called 3 times
        verify(streamStoreMock, times(3)).generateTransactionId(any(), any(), any(), any());
        // verify that create transaction is called 2 times
        verify(streamStoreMock, times(2)).createTransaction(any(), any(), any(), anyLong(), 
                anyLong(), any(), any());

        // verify that the txn id that is generated is of type ""
        UUID txnId = txn.getKey().getId();
        assertEquals(0, (int) (txnId.getMostSignificantBits() >> 32));
        assertEquals(2, txnId.getLeastSignificantBits());
    }

    @Test(timeout = 10000)
    public void txnPingTest() throws Exception {
        // Create mock writer objects.
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();

        StreamMetadataStore streamStoreMock = spy(StreamStoreFactory.createZKStore(zkClient, executor));

        // Create transaction tasks.
        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock,
                                                      SegmentHelperMock.getSegmentHelperMock(), executor, "host",
                                                      new GrpcAuthHelper(this.authEnabled, "secret", 
                                                              300));
        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // Create stream and scope
        streamStoreMock.createScope(SCOPE, null, executor).join();
        streamStoreMock.createStream(SCOPE, STREAM, configuration1, System.currentTimeMillis(), null, executor).join();
        streamStoreMock.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        // Verify Ping transaction on committing transaction.
        Pair<VersionedTransactionData, List<StreamSegmentRecord>> txn = txnTasks.createTxn(SCOPE, STREAM, 10000L, 
                0L, 0L).join();
        UUID txnId = txn.getKey().getId();
        txnTasks.commitTxn(SCOPE, STREAM, txnId, 0L).join();
        assertEquals(PingTxnStatus.Status.COMMITTED, txnTasks.pingTxn(SCOPE, STREAM, txnId, 10000L, 
                0L).join().getStatus());

        // complete commit of transaction. 
        streamStoreMock.startCommitTransactions(SCOPE, STREAM, 100, null, executor).join();
        val record = streamStoreMock.getVersionedCommittingTransactionsRecord(
                SCOPE, STREAM, null, executor).join();
        streamStoreMock.completeCommitTransactions(SCOPE, STREAM, record, null, executor, Collections.emptyMap()).join();

        // verify that transaction is removed from active txn
        AssertExtensions.assertFutureThrows("Fetching Active Txn record should throw DNF",
                streamStoreMock.getTransactionData(SCOPE, STREAM, txnId, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        assertEquals(PingTxnStatus.Status.COMMITTED, txnTasks.pingTxn(SCOPE, STREAM, txnId, 10000L, 
                0L).join().getStatus());

        // Verify Ping transaction on an aborting transaction.
        txn = txnTasks.createTxn(SCOPE, STREAM, 10000L, 0L, 1024 * 1024L).join();
        txnId = txn.getKey().getId();
        txnTasks.abortTxn(SCOPE, STREAM, txnId, null, 0L).join();
        assertEquals(PingTxnStatus.Status.ABORTED, txnTasks.pingTxn(SCOPE, STREAM, txnId, 10000L, 
                0L).join().getStatus());

        // now complete abort so that the transaction is removed from active txn and added to completed txn.
        streamStoreMock.abortTransaction(SCOPE, STREAM, txnId, null, executor).join();
        AssertExtensions.assertFutureThrows("Fetching Active Txn record should throw DNF", 
                streamStoreMock.getTransactionData(SCOPE, STREAM, txnId, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        assertEquals(PingTxnStatus.Status.ABORTED, txnTasks.pingTxn(SCOPE, STREAM, txnId, 10000L, 
                0L).join().getStatus());

        // try with a non existent transaction id 
        assertEquals(PingTxnStatus.Status.UNKNOWN, 
                txnTasks.pingTxn(SCOPE, STREAM, UUID.randomUUID(), 10000L, 0L).join().getStatus());

        // Verify max execution time.
        txnTasks.setMaxExecutionTime(1L);
        txn = txnTasks.createTxn(SCOPE, STREAM, 10000L, 0L, 1024 * 1024L).join();
        UUID tid = txn.getKey().getId();
        AssertExtensions.assertEventuallyEquals(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED, 
                () -> txnTasks.pingTxn(SCOPE, STREAM, tid, 10000L, 0L).join().getStatus(), 10000L);
        txnTasks.setMaxExecutionTime(Duration.ofDays(Config.MAX_TXN_EXECUTION_TIMEBOUND_DAYS).toMillis());
    }
    
    @Test(timeout = 10000)
    public void writerInitializationTest() throws Exception {
        EventStreamWriterMock<CommitEvent> commitWriter = new EventStreamWriterMock<>();
        EventStreamWriterMock<AbortEvent> abortWriter = new EventStreamWriterMock<>();
        StreamMetadataStore streamStoreMock = spy(StreamStoreFactory.createZKStore(zkClient, executor));
        final long leasePeriod = 5000;

        // region close before initialize
        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, 
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 300));
        CompletableFuture<Void> future = txnTasks.writeCommitEvent(new CommitEvent("scope", "stream", 0));
        assertFalse(future.isDone());

        txnTasks.close();
        AssertExtensions.assertFutureThrows("", future, e -> Exceptions.unwrap(e) instanceof CancellationException);
        // endregion

        // region test initialize writers with client factory
        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, 
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 300));

        future = txnTasks.writeCommitEvent(new CommitEvent("scope", "stream", 0));

        EventStreamClientFactory cfMock = mock(EventStreamClientFactory.class);
        ControllerEventProcessorConfig eventProcConfigMock = mock(ControllerEventProcessorConfig.class);
        String commitStream = "commitStream";
        doAnswer(x -> commitStream).when(eventProcConfigMock).getCommitStreamName();
        doAnswer(x -> commitWriter).when(cfMock).createEventWriter(eq(commitStream), any(), any());
        String abortStream = "abortStream";
        doAnswer(x -> abortStream).when(eventProcConfigMock).getAbortStreamName();
        doAnswer(x -> abortWriter).when(cfMock).createEventWriter(eq(abortStream), any(), any());

        // future should not have completed as we have not initialized the writers. 
        assertFalse(future.isDone());
        
        // initialize the writers. write future should have completed now. 
        txnTasks.initializeStreamWriters(cfMock, eventProcConfigMock);

        assertTrue(Futures.await(future));

        txnTasks.close();
        // endregion
        
        // region test method calls and initialize writers with direct writer set up method call
        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, 
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 300));

        streamStore.createScope(SCOPE, null, executor).join();
        streamStore.createStream(SCOPE, STREAM, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 1L, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        CompletableFuture<Pair<VersionedTransactionData, List<StreamSegmentRecord>>> createFuture = txnTasks.createTxn(
                SCOPE, STREAM, leasePeriod, 0L, 0L);

        // create and ping transactions should not wait for writer initialization and complete immediately.
        createFuture.join();
        assertTrue(Futures.await(createFuture));
        UUID txnId = createFuture.join().getKey().getId();
        CompletableFuture<PingTxnStatus> pingFuture = txnTasks.pingTxn(SCOPE, STREAM, txnId, leasePeriod, 0L);
        assertTrue(Futures.await(pingFuture));

        CompletableFuture<TxnStatus> commitFuture = txnTasks.commitTxn(SCOPE, STREAM, txnId, 0L);
        assertFalse(commitFuture.isDone());

        txnTasks.initializeStreamWriters(commitWriter, abortWriter);
        assertTrue(Futures.await(commitFuture));
        UUID txnId2 = txnTasks.createTxn(SCOPE, STREAM, leasePeriod, 0L, 1024 * 1024L).join().getKey().getId();
        assertTrue(Futures.await(txnTasks.abortTxn(SCOPE, STREAM, txnId2, null, 0L)));
    }
    
    @Test(timeout = 10000)
    public void writerRoutingKeyTest() throws InterruptedException {
        StreamMetadataStore streamStoreMock = StreamStoreFactory.createZKStore(zkClient, executor);

        txnTasks = new StreamTransactionMetadataTasks(streamStoreMock, 
                SegmentHelperMock.getSegmentHelperMock(), executor, "host", 
                new GrpcAuthHelper(this.authEnabled, "secret", 300));

        streamStore.createScope(SCOPE, null, executor).join();
        streamStore.createStream(SCOPE, STREAM, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 
                1L, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();

        TestEventStreamWriter<CommitEvent> commitWriter = new TestEventStreamWriter<>();
        TestEventStreamWriter<AbortEvent> abortWriter = new TestEventStreamWriter<>();

        txnTasks.initializeStreamWriters(commitWriter, abortWriter);

        UUID txnId = UUID.randomUUID();
        txnTasks.writeAbortEvent(SCOPE, STREAM, 0, txnId, TxnStatus.ABORTING, 0L).join();
        Pair<String, AbortEvent> request = abortWriter.requestsReceived.take();
        assertEquals(request.getKey(), request.getValue().getKey());
        txnTasks.writeAbortEvent(new AbortEvent(SCOPE, STREAM, 0, txnId, 10L)).join();
        Pair<String, AbortEvent> request2 = abortWriter.requestsReceived.take();
        assertEquals(request2.getKey(), request2.getValue().getKey());
        // verify that both use the same key
        assertEquals(request.getKey(), request2.getKey());

        txnTasks.writeCommitEvent(SCOPE, STREAM, 0, txnId, TxnStatus.COMMITTING, 0L).join();
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
        public CompletableFuture<Void> writeEvents(String routingKey, List<T> events) {
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

        EventProcessorSystemImpl system = new EventProcessorSystemImpl("system", "host", SCOPE,
                clientFactory, readerGroupManager);

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

        system.createEventProcessorGroup(config, CheckpointStoreFactory.createInMemoryStore(), executor);
    }

    public static class AuthEnabledTests extends StreamTransactionMetadataTasksTest {
        @Override
        @Before
        public void setup() {
            this.authEnabled = true;
            super.setup();
        }
    }
}

