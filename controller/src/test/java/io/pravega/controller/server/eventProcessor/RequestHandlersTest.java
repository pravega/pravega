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
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.controller.MetricsTestUtil;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class RequestHandlersTest {
    protected ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected CuratorFramework zkClient;

    private final String scope = "scope";
    private RequestTracker requestTracker = new RequestTracker(true);
    private PravegaZkCuratorResource pravegaZkCuratorResource = new PravegaZkCuratorResource();
    private StreamMetadataStore streamStore;
    private BucketStore bucketStore;
    private TaskMetadataStore taskMetadataStore;
    private StreamMetadataTasks streamMetadataTasks;
    private KVTableMetadataStore kvtStore;
    private TableMetadataTasks kvtTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;

    private EventStreamClientFactory clientFactory;
    private ConnectionFactory connectionFactory;
    private SegmentHelper segmentHelper;
    private StatsProvider statsProvider;
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
        segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        clientFactory = mock(EventStreamClientFactory.class);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());
        doAnswer(x -> new EventStreamWriterMock<>()).when(clientFactory).createEventWriter(anyString(), any(), any());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());
        streamMetadataTasks.initializeStreamWriters(clientFactory, Config.SCALE_STREAM_NAME);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, 
                segmentHelper, executor, hostId, GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());
        kvtStore = spy(getKvtStore());
        kvtTasks = mock(TableMetadataTasks.class);
        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope, null, executor).get();

        statsProvider = MetricsTestUtil.getInitializedStatsProvider();
        statsProvider.startWithoutExporting();

    }

    abstract StreamMetadataStore getStore();

    abstract KVTableMetadataStore getKvtStore();

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
    @Test(timeout = 300000)
    public void testConcurrentIdempotentCommitTxnRequest() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("startRollingTxn", 0);
        map.put("rollingTxnCreateDuplicateEpochs", 0);
        map.put("completeRollingTxn", 0);
        map.put("startCommitTransactions", 1);
        map.put("updateVersionedState", 0);
        map.put("completeCommitTransactions", 1);
        // first job should find no transactions and do nothing
        concurrentTxnCommit("commit1", "startCommitTransactions", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);
        map.put("updateVersionedState", 1);

        map.put("startCommitTransactions", 1);
        map.put("completeCommitTransactions", 1);
        // first job should fail to update committing transaction record with write conflict. 
        concurrentTxnCommit("commit2", "completeCommitTransactions", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 2);
    }

    private void concurrentTxnCommit(String stream, String func,
                                     boolean expectFailureOnFirstJob,
                                     Predicate<Throwable> firstExceptionPredicate,
                                     Map<String, Integer> invocationCount, int expectedVersion) throws Exception {
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        CommitRequestHandler requestHandler1 = new CommitRequestHandler(streamStore1Spied, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        CommitRequestHandler requestHandler2 = new CommitRequestHandler(streamStore2, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        
        // create txn on epoch 0 and set it to committing
        UUID txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch0 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch0.getVersion()), "", Long.MIN_VALUE, null, executor).join();

        // regular commit
        // start commit transactions
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        CommitEvent commitOnEpoch1 = new CommitEvent(scope, stream, 0);

        setMockCommitTxnLatch(streamStore1, streamStore1Spied, func, signal, wait);

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> requestHandler1.execute(commitOnEpoch1), executor);

        signal.join();
        // let this run to completion. this should succeed 
        requestHandler2.execute(commitOnEpoch1).join();

        wait.complete(null);

        if (expectFailureOnFirstJob) {
            AssertExtensions.assertSuppliedFutureThrows("first commit should fail", () -> future1, firstExceptionPredicate);
        } else {
            future1.join();
        }

        verify(streamStore1Spied, times(invocationCount.get("startCommitTransactions")))
                .startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("completeCommitTransactions")))
                .completeCommitTransactions(anyString(), anyString(), any(), any(), any(), any());
        verify(streamStore1Spied, times(invocationCount.get("updateVersionedState")))
                .updateVersionedState(anyString(), anyString(), any(), any(), any(), any());

        VersionedMetadata<CommittingTransactionsRecord> versioned = streamStore1.getVersionedCommittingTransactionsRecord(scope, stream, null, executor).join();
        assertEquals(CommittingTransactionsRecord.EMPTY, versioned.getObject());
        assertEquals(expectedVersion, getVersionNumber(versioned.getVersion()));
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        streamStore1.close();
        streamStore2.close();
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void updateSealedStream() throws Exception {
        String stream = "updateSealed";
        StreamMetadataStore streamStore = getStore();
        StreamMetadataStore streamStoreSpied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();

        streamStore.setState(scope, stream, State.ACTIVE, null, executor).join();
        streamStore.setState(scope, stream, State.SEALED, null, executor).join();

        UpdateStreamTask requestHandler = new UpdateStreamTask(streamMetadataTasks, streamStoreSpied, bucketStore, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        streamStore.startUpdateConfiguration(scope, stream, config, null, executor).join();

        UpdateStreamEvent event = new UpdateStreamEvent(scope, stream, System.currentTimeMillis());

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore.completeUpdateConfiguration(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStoreSpied).completeUpdateConfiguration(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null)
                .thenComposeAsync(v -> requestHandler.execute(event), executor);
        signal.join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("Updating sealed stream job should fail", () -> future,
                e -> Exceptions.unwrap(e) instanceof UnsupportedOperationException);

        // validate
        VersionedMetadata<StreamConfigurationRecord> versioned = streamStore.getConfigurationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.SEALED, streamStore.getState(scope, stream, true, null, executor).join());
        streamStore.close();
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void truncateSealedStream() throws Exception {
        String stream = "truncateSealed";
        StreamMetadataStore streamStore = getStore();
        StreamMetadataStore streamStoreSpied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();

        streamStore.setState(scope, stream, State.ACTIVE, null, executor).join();
        streamStore.setState(scope, stream, State.SEALED, null, executor).join();

        TruncateStreamTask requestHandler = new TruncateStreamTask(streamMetadataTasks, streamStoreSpied, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 100L);

        streamStore.startTruncation(scope, stream, map, null, executor).join();

        TruncateStreamEvent event = new TruncateStreamEvent(scope, stream, System.currentTimeMillis());

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore.completeTruncation(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStoreSpied).completeTruncation(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null)
                .thenComposeAsync(v -> requestHandler.execute(event), executor);
        signal.join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("Updating sealed stream job should fail", () -> future,
                e -> Exceptions.unwrap(e) instanceof UnsupportedOperationException);

        // validate
        VersionedMetadata<StreamTruncationRecord> versioned = streamStore.getTruncationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.SEALED, streamStore.getState(scope, stream, true, null, executor).join());
        streamStore.close();
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void testConcurrentIdempotentRollingTxnRequest() {
        Map<String, Integer> map = new HashMap<>();
        map.put("startRollingTxn", 0);
        map.put("rollingTxnCreateDuplicateEpochs", 0);
        map.put("completeRollingTxn", 0);
        map.put("startCommitTransactions", 1);
        map.put("completeCommitTransactions", 0);
        map.put("updateVersionedState", 1);
        // first job will wait at startCommitTransaction.
        // second job will complete transaction with rolling transaction. 
        // first job will complete having found no new transactions to commit
        concurrentRollingTxnCommit("stream1", "startCommitTransactions", false,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 4);

        map.put("startRollingTxn", 1);
        map.put("completeCommitTransactions", 0);
        // first job has created the committing transaction record. second job will mark it as rolling txn and complete
        // rolling transaction
        // first job will fail in its attempt to update CTR. 
        concurrentRollingTxnCommit("stream2", "startRollingTxn", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        map.put("rollingTxnCreateDuplicateEpochs", 1);
        map.put("completeRollingTxn", 1);
        map.put("completeCommitTransactions", 1);
        // first job has created rolling transcation's duplicate epochs. second job will complete rolling transaction
        // first job should complete rolling transaction's steps (idempotent) but will fail with write conflict 
        // in attempt to update CTR during completeCommitTransaction phase. 
        concurrentRollingTxnCommit("stream3", "rollingTxnCreateDuplicateEpochs", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);
        
        // same as above
        concurrentRollingTxnCommit("stream4", "completeRollingTxn", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);

        // same as above
        concurrentRollingTxnCommit("stream5", "completeCommitTransactions", true,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException, map, 3);
    }

    private void concurrentRollingTxnCommit(String stream, String func,
                                            boolean expectFailureOnFirstJob,
                                            Predicate<Throwable> firstExceptionPredicate,
                                            Map<String, Integer> invocationCount, int expectedVersion) {
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        CommitRequestHandler requestHandler1 = new CommitRequestHandler(streamStore1Spied, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        CommitRequestHandler requestHandler2 = new CommitRequestHandler(streamStore2, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        ScaleOperationTask scaleRequesthandler = new ScaleOperationTask(streamMetadataTasks, streamStore2, executor);

        // create txn on epoch 0 and set it to committing
        UUID txnId = streamStore1.generateTransactionId(scope, stream, null, executor).join();
        VersionedTransactionData txnEpoch0 = streamStore1.createTransaction(scope, stream, txnId, 1000L, 10000L, null, executor).join();
        streamStore1.sealTransaction(scope, stream, txnId, true, Optional.of(txnEpoch0.getVersion()), "", Long.MIN_VALUE, null, executor).join();
        // perform scale
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, Lists.newArrayList(0L),
                Lists.newArrayList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), false, System.currentTimeMillis(),
                System.currentTimeMillis());
        scaleRequesthandler.execute(event).join();
        
        // regular commit
        // start commit transactions
        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        // test rolling transaction --> since transaction on epoch 0 is committing, it will get committed first.
        CommitEvent commitOnEpoch0 = new CommitEvent(scope, stream, 0);

        setMockCommitTxnLatch(streamStore1, streamStore1Spied, func, signal, wait);

        // start rolling txn
        // stall rolling transaction in different stages
        CompletableFuture<Void> future1Rolling = CompletableFuture.completedFuture(null)
                                                                  .thenComposeAsync(v -> requestHandler1.execute(commitOnEpoch0), executor);
        signal.join();
        requestHandler2.execute(commitOnEpoch0).join();
        wait.complete(null);

        if (expectFailureOnFirstJob) {
            AssertExtensions.assertSuppliedFutureThrows("first commit should fail", () -> future1Rolling, firstExceptionPredicate);
            verify(streamStore1Spied, times(invocationCount.get("startCommitTransactions")))
                    .startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("startRollingTxn"))).startRollingTxn(anyString(), anyString(), anyInt(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("rollingTxnCreateDuplicateEpochs")))
                    .rollingTxnCreateDuplicateEpochs(anyString(), anyString(), any(), anyLong(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("completeRollingTxn")))
                    .completeRollingTxn(anyString(), anyString(), any(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("completeCommitTransactions")))
                    .completeCommitTransactions(anyString(), anyString(), any(), any(), any(), any());
            verify(streamStore1Spied, times(invocationCount.get("updateVersionedState")))
                    .updateVersionedState(anyString(), anyString(), any(), any(), any(), any());
        } else {
            future1Rolling.join();
        }
        // validate rolling txn done and first job has updated the CTR with new txn record
        VersionedMetadata<CommittingTransactionsRecord> versioned = streamStore1.getVersionedCommittingTransactionsRecord(scope, stream, null, executor).join();
        assertEquals(CommittingTransactionsRecord.EMPTY, versioned.getObject());
        assertEquals(expectedVersion, getVersionNumber(versioned.getVersion()));
        assertEquals(3, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
    }

    private void setMockCommitTxnLatch(StreamMetadataStore store, StreamMetadataStore spied,
                                       String func, CompletableFuture<Void> signal, CompletableFuture<Void> waitOn) {
        switch (func) {
            case "startCommitTransactions":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.startCommitTransactions(x.getArgument(0), x.getArgument(1), 
                            x.getArgument(2), x.getArgument(3), x.getArgument(4));
                }).when(spied).startCommitTransactions(anyString(), anyString(), anyInt(), any(), any());
                break;
            case "completeCommitTransactions":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.completeCommitTransactions(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), Collections.emptyMap());
                }).when(spied).completeCommitTransactions(anyString(), anyString(), any(), any(), any(), any());
                break;
            case "startRollingTxn":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.startRollingTxn(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
                }).when(spied).startRollingTxn(anyString(), anyString(), anyInt(), any(), any(), any());
                break;
            case "rollingTxnCreateDuplicateEpochs":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.rollingTxnCreateDuplicateEpochs(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5), x.getArgument(6));
                }).when(spied).rollingTxnCreateDuplicateEpochs(anyString(), anyString(), any(), anyLong(), any(), any(), any());
                break;
            case "completeRollingTxn":
                doAnswer(x -> {
                    signal.complete(null);
                    waitOn.join();
                    return store.completeRollingTxn(x.getArgument(0), x.getArgument(1),
                            x.getArgument(2), x.getArgument(3), x.getArgument(4),
                            x.getArgument(5));
                }).when(spied).completeRollingTxn(anyString(), anyString(), any(), any(), any(), any());
                break;
            case "updateVersionedState":
                doAnswer(x -> {
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

    // concurrent update stream
    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void concurrentUpdateStream() throws Exception {
        String stream = "update";
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        UpdateStreamTask requestHandler1 = new UpdateStreamTask(streamMetadataTasks, streamStore1Spied, bucketStore, executor);
        UpdateStreamTask requestHandler2 = new UpdateStreamTask(streamMetadataTasks, streamStore2, bucketStore, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        streamStore1.startUpdateConfiguration(scope, stream, config, null, executor).join();

        UpdateStreamEvent event = new UpdateStreamEvent(scope, stream, System.currentTimeMillis());

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.completeUpdateConfiguration(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStore1Spied).completeUpdateConfiguration(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> requestHandler1.execute(event), executor);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("first update job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // validate rolling txn done
        VersionedMetadata<StreamConfigurationRecord> versioned = streamStore1.getConfigurationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        streamStore1.close();
        streamStore2.close();
    }

    abstract int getVersionNumber(Version version);

    @Test(timeout = 300000)
    public void idempotentUpdatePartialScaleCompleted() throws Exception {
        String stream = "update2";
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        UpdateStreamTask requestHandler1 = new UpdateStreamTask(streamMetadataTasks, streamStore1Spied, bucketStore, executor);
        UpdateStreamTask requestHandler2 = new UpdateStreamTask(streamMetadataTasks, streamStore2, bucketStore, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(2)).build();

        streamStore1.startUpdateConfiguration(scope, stream, config, null, executor).join();

        UpdateStreamEvent event = new UpdateStreamEvent(scope, stream, System.currentTimeMillis());

        // make this wait at reset epoch transition. this has already changed the state to updating. both executions are 
        // performing the same update. 
        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.scaleSegmentsSealed(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4), x.getArgument(5));
        }).when(streamStore1Spied).scaleSegmentsSealed(anyString(), anyString(), any(), any(), any(), any());

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> requestHandler1.execute(event), executor);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("first update job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        VersionedMetadata<StreamConfigurationRecord> versioned = streamStore1.getConfigurationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        assertEquals(1, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());
        
        // repeat the above experiment with complete scale step also having been performed. 
        streamStore1.close();
        streamStore2.close();
    }

    @Test(timeout = 300000)
    public void idempotentUpdateCompletedScale() throws Exception {
        String stream = "update3";
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        UpdateStreamTask requestHandler1 = new UpdateStreamTask(streamMetadataTasks, streamStore1Spied, bucketStore, executor);
        UpdateStreamTask requestHandler2 = new UpdateStreamTask(streamMetadataTasks, streamStore2, bucketStore, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();
        config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.fixed(2)).build();

        streamStore1.startUpdateConfiguration(scope, stream, config, null, executor).join();

        UpdateStreamEvent event = new UpdateStreamEvent(scope, stream, System.currentTimeMillis());

        // make this wait at reset epoch transition. this has already changed the state to updating. both executions are 
        // performing the same update. 
        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.completeScale(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStore1Spied).completeScale(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                                                           .thenComposeAsync(v -> requestHandler1.execute(event), executor);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("first update job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        VersionedMetadata<StreamConfigurationRecord> versioned = streamStore1.getConfigurationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        assertEquals(1, streamStore1.getActiveEpoch(scope, stream, null, true, executor).join().getEpoch());

        // repeat the above experiment with complete scale step also having been performed. 
        streamStore1.close();
        streamStore2.close();
    }

    // concurrent truncate stream
    @SuppressWarnings("unchecked")
    @Test(timeout = 300000)
    public void concurrentTruncateStream() throws Exception {
        String stream = "update";
        StreamMetadataStore streamStore1 = getStore();
        StreamMetadataStore streamStore1Spied = spy(getStore());
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();
        streamStore1.createStream(scope, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore1.setState(scope, stream, State.ACTIVE, null, executor).join();

        StreamMetadataStore streamStore2 = getStore();

        TruncateStreamTask requestHandler1 = new TruncateStreamTask(streamMetadataTasks, streamStore1Spied, executor);
        TruncateStreamTask requestHandler2 = new TruncateStreamTask(streamMetadataTasks, streamStore2, executor);

        CompletableFuture<Void> wait = new CompletableFuture<>();
        CompletableFuture<Void> signal = new CompletableFuture<>();

        Map<Long, Long> map = new HashMap<>();
        map.put(0L, 100L);

        streamStore1.startTruncation(scope, stream, map, null, executor).join();

        TruncateStreamEvent event = new TruncateStreamEvent(scope, stream, System.currentTimeMillis());

        doAnswer(x -> {
            signal.complete(null);
            wait.join();
            return streamStore1.completeTruncation(x.getArgument(0), x.getArgument(1),
                    x.getArgument(2), x.getArgument(3), x.getArgument(4));
        }).when(streamStore1Spied).completeTruncation(anyString(), anyString(), any(), any(), any());

        CompletableFuture<Void> future1 = CompletableFuture.completedFuture(null)
                         .thenComposeAsync(v -> requestHandler1.execute(event), executor);
        signal.join();
        requestHandler2.execute(event).join();
        wait.complete(null);

        AssertExtensions.assertSuppliedFutureThrows("first truncate job should fail", () -> future1,
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        // validate rolling txn done
        VersionedMetadata<StreamTruncationRecord> versioned = streamStore1.getTruncationRecord(scope, stream, null, executor).join();
        assertFalse(versioned.getObject().isUpdating());
        assertEquals(2, getVersionNumber(versioned.getVersion()));
        assertEquals(State.ACTIVE, streamStore1.getState(scope, stream, true, null, executor).join());
        streamStore1.close();
        streamStore2.close();
    }

    @Test
    public void testDeleteAssociatedStream() {
        String stream = "deleteAssociated";
        createStreamInStore(stream, scope);
        String markStream = NameUtils.getMarkStreamForStream(stream);
        createStreamInStore(markStream, scope);

        SealStreamTask sealStreamTask = new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor);
        DeleteStreamTask deleteStreamTask = new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor);

        // create mark stream
        // seal stream. 
        SealStreamEvent sealEvent = new SealStreamEvent(scope, stream, 0L);
        // set state to sealing and send event to the processor
        streamStore.setState(scope, stream, State.SEALING, null, executor).join();
        sealStreamTask.execute(sealEvent).join();
        assertEquals(State.SEALED, streamStore.getState(scope, stream, true, null, executor).join());

        // mark stream should still be present and active
        assertTrue(streamStore.checkStreamExists(scope, markStream, null, executor).join());
        assertEquals(streamStore.getState(scope, markStream, true, null, executor).join(), State.ACTIVE);

        // delete the stream
        long creationTime = streamStore.getCreationTime(scope, stream, null, executor).join();
        DeleteStreamEvent firstDeleteEvent = new DeleteStreamEvent(scope, stream, 0L, creationTime);
        deleteStreamTask.execute(firstDeleteEvent).join();

        // verify that mark stream is also deleted
        assertFalse(streamStore.checkStreamExists(scope, markStream, null, executor).join());
    }

    @Test
    public void testDeleteScopeRecursive() {
        StreamMetadataStore streamStoreSpied = spy(getStore());
        KVTableMetadataStore kvtStoreSpied = spy(getKvtStore());
        OperationContext ctx = new OperationContext() {
            @Override
            public long getOperationStartTime() {
                return 0;
            }

            @Override
            public long getRequestId() {
                return 0;
            }
        };
        UUID scopeId = streamStoreSpied.getScopeId(scope, ctx, executor).join();
        doAnswer(x -> {
            CompletableFuture<UUID> cf = new CompletableFuture<>();
            cf.complete(scopeId);
            return cf;
        }).when(streamStoreSpied).getScopeId(eq(scope), eq(ctx), eq(executor));

        doAnswer(invocation -> {
            CompletableFuture<Boolean> cf = new CompletableFuture<>();
            cf.complete(false);
            return cf;
        }).when(streamStoreSpied).isScopeSealed(eq(scope), any(), any());

        DeleteScopeTask requestHandler = new DeleteScopeTask(streamMetadataTasks, streamStoreSpied, kvtStoreSpied, kvtTasks, executor);
        DeleteScopeEvent event = new DeleteScopeEvent(scope, System.currentTimeMillis(), scopeId);
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null)
                .thenComposeAsync(v -> requestHandler.execute(event), executor);
        future.join();
    }

    @Test
    public void scopeDeleteTest() {
        final String testScope = "testScope";
        final String testStream = "testStream";
        final String testRG = "_RGTestRG";
        final String testKVT = "testKVT";
        StreamMetadataStore streamStoreSpied = spy(getStore());
        KVTableMetadataStore kvtStoreSpied = spy(getKvtStore());
        StreamMetadataTasks streamMetadataTasks1 = mock(StreamMetadataTasks.class);
        TableMetadataTasks kvtTasksMocked = mock(TableMetadataTasks.class);
        streamStoreSpied.createScope(testScope, null, executor).join();
        OperationContext ctx = new OperationContext() {
            @Override
            public long getOperationStartTime() {
                return 0;
            }

            @Override
            public long getRequestId() {
                return 0;
            }
        };
        UUID scopeId = streamStoreSpied.getScopeId(testScope, ctx, executor).join();
        doAnswer(x -> {
            CompletableFuture<UUID> cf = new CompletableFuture<>();
            cf.complete(scopeId);
            return cf;
        }).when(streamStoreSpied).getScopeId(eq(testScope), eq(ctx), eq(executor));

        doAnswer(invocation -> {
            CompletableFuture<Boolean> cf = new CompletableFuture<>();
            cf.complete(true);
            return cf;
        }).when(streamStoreSpied).isScopeSealed(eq(testScope), any(), any());

        createStreamInStore(testStream, testScope);
        createStreamInStore(testRG, testScope);
        assertTrue(streamStore.checkStreamExists(testScope, testStream, ctx, executor).join());

        doAnswer(invocation -> {
            CompletableFuture<Controller.UpdateStreamStatus.Status> future = new CompletableFuture<>();
            future.complete(Controller.UpdateStreamStatus.Status.SUCCESS);
            return future;
        }).when(streamMetadataTasks1).sealStream(anyString(), anyString(), anyLong());

        doAnswer(invocation -> {
            CompletableFuture<Controller.DeleteStreamStatus.Status> future = new CompletableFuture<>();
            future.complete(Controller.DeleteStreamStatus.Status.SUCCESS);
            return future;
        }).when(streamMetadataTasks1).deleteStream(anyString(), anyString(), anyLong());

        // Create Reader Group
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(testScope, testStream))
                .build();
        final ReaderGroupConfig config = ReaderGroupConfig.cloneConfig(rgConfig, UUID.randomUUID(), 123L);

        Controller.ReaderGroupConfiguration expectedConfig = ModelHelper.decode(testScope, testRG, config);

        doAnswer(invocationOnMock -> {
            CompletableFuture<Controller.CreateReaderGroupResponse.Status> createRG = new CompletableFuture<>();
            createRG.complete(Controller.CreateReaderGroupResponse.Status.SUCCESS);
            return createRG;
        }).when(streamMetadataTasks1).createReaderGroup(anyString(), any(), any(), anyLong(), anyLong());

        doAnswer(invocation -> CompletableFuture.completedFuture(Controller.ReaderGroupConfigResponse.newBuilder()
                .setStatus(Controller.ReaderGroupConfigResponse.Status.SUCCESS)
                .setConfig(expectedConfig)
                .build()))
                .when(streamMetadataTasks1).getReaderGroupConfig(eq(testScope), anyString(), anyLong());

        doAnswer(invocationOnMock -> {
            CompletableFuture<Controller.DeleteReaderGroupStatus.Status> future = new CompletableFuture<>();
            future.complete(Controller.DeleteReaderGroupStatus.Status.SUCCESS);
            return future;
        }).when(streamMetadataTasks1).deleteReaderGroup(anyString(), anyString(), anyString(), anyLong());

        // Create KVT
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(1).primaryKeyLength(1).secondaryKeyLength(1).build();
        doAnswer(invocationOnMock -> {
            CompletableFuture<Controller.CreateKeyValueTableStatus.Status> fut = new CompletableFuture<>();
            fut.complete(Controller.CreateKeyValueTableStatus.Status.SUCCESS);
            return fut;
        }).when(kvtTasksMocked).createKeyValueTable(anyString(), anyString(), any(), anyLong(), anyLong());
        List<String> tableList = new ArrayList<>();
        tableList.add(testKVT);
        Pair<List<String>, String> listOfKVTables = new ImmutablePair<>(tableList, "");

        doAnswer(invocationOnMock -> CompletableFuture.completedFuture(listOfKVTables))
                .doAnswer(invocationOnMock ->
                        CompletableFuture.completedFuture(new ImmutablePair<>(Collections.emptyList(),
                                invocationOnMock.getArgument(0))))
                .when(kvtStoreSpied).listKeyValueTables(anyString(), any(), anyInt(), any(), any());

        doAnswer(invocationOnMock -> {
            CompletableFuture<Controller.DeleteKVTableStatus.Status> future = new CompletableFuture<>();
            future.complete(Controller.DeleteKVTableStatus.Status.SUCCESS);
            return future;
        }).when(kvtTasksMocked).deleteKeyValueTable(anyString(), anyString(), anyLong());

        Controller.CreateKeyValueTableStatus.Status status = kvtTasksMocked.createKeyValueTable(testScope,
                testKVT, kvtConfig, System.currentTimeMillis(), 123L).join();
        assertEquals(status, Controller.CreateKeyValueTableStatus.Status.SUCCESS);

        DeleteScopeTask requestHandler = new DeleteScopeTask(streamMetadataTasks1, streamStoreSpied, kvtStoreSpied, kvtTasksMocked, executor);
        DeleteScopeEvent event = new DeleteScopeEvent(testScope, 123L, scopeId);
        CompletableFuture<Void> future = requestHandler.execute(event);
        future.join();
        assertTrue(MetricsTestUtil.getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_SCOPE_LATENCY) > 0);
    }

    @Test
    public void testDeleteBucketReferences() {
        String stream = "deleteReferences";
        createStreamInStore(stream, scope);
        String scopedStreamName = NameUtils.getScopedStreamName(scope, stream);
        int watermarkingBuckets = bucketStore.getBucketCount(BucketStore.ServiceType.WatermarkingService);
        int retentionBuckets = bucketStore.getBucketCount(BucketStore.ServiceType.RetentionService);
        
        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor).join();
        bucketStore.addStreamToBucketStore(BucketStore.ServiceType.WatermarkingService, scope, stream, executor).join();
        
        // seal stream. 
        // set state to sealing and send event to the processor
        streamStore.setState(scope, stream, State.SEALING, null, executor).join();
        streamStore.setState(scope, stream, State.SEALED, null, executor).join();

        List<String> retentionStreams = IntStream.range(0, retentionBuckets).boxed().map(x ->
                bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, x, executor).join())
                                                 .flatMap(Collection::stream).collect(Collectors.toList());
        assertTrue(retentionStreams.contains(scopedStreamName));
        List<String> watermarkStreams = IntStream.range(0, watermarkingBuckets).boxed().map(x ->
                bucketStore.getStreamsForBucket(BucketStore.ServiceType.WatermarkingService, x, executor).join())
                                                 .flatMap(Collection::stream).collect(Collectors.toList());
        assertTrue(watermarkStreams.contains(scopedStreamName));

        // delete the stream
        DeleteStreamTask deleteStreamTask = new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor);

        long creationTime = streamStore.getCreationTime(scope, stream, null, executor).join();
        DeleteStreamEvent firstDeleteEvent = new DeleteStreamEvent(scope, stream, 0L, creationTime);
        deleteStreamTask.execute(firstDeleteEvent).join();

        watermarkStreams = IntStream.range(0, watermarkingBuckets).boxed().map(x ->
                bucketStore.getStreamsForBucket(BucketStore.ServiceType.WatermarkingService, x, executor).join())
                                                 .flatMap(Collection::stream).collect(Collectors.toList());
        assertFalse(watermarkStreams.contains(scopedStreamName));
        retentionStreams = IntStream.range(0, retentionBuckets).boxed().map(x ->
                bucketStore.getStreamsForBucket(BucketStore.ServiceType.RetentionService, x, executor).join())
                                                 .flatMap(Collection::stream).collect(Collectors.toList());
        assertFalse(retentionStreams.contains(scopedStreamName));
    }

    @Test
    public void testDeleteStreamReplay() {
        String stream = "delete";
        createStreamInStore(stream, scope);

        SealStreamTask sealStreamTask = new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor);
        DeleteStreamTask deleteStreamTask = new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor);

        // seal stream. 
        SealStreamEvent sealEvent = new SealStreamEvent(scope, stream, 0L);
        // set state to sealing and send event to the processor
        streamStore.setState(scope, stream, State.SEALING, null, executor).join();
        sealStreamTask.execute(sealEvent).join();
        assertEquals(State.SEALED, streamStore.getState(scope, stream, true, null, executor).join());

        // delete the stream
        long creationTime = streamStore.getCreationTime(scope, stream, null, executor).join();
        DeleteStreamEvent firstDeleteEvent = new DeleteStreamEvent(scope, stream, 0L, creationTime);
        deleteStreamTask.execute(firstDeleteEvent).join();

        // recreate stream with same name in the store
        createStreamInStore(stream, scope);

        long newCreationTime = streamStore.getCreationTime(scope, stream, null, executor).join();

        assertNotEquals(creationTime, newCreationTime);

        // seal stream. 
        sealEvent = new SealStreamEvent(scope, stream, 0L);
        // set state to sealing and send event to the processor
        streamStore.setState(scope, stream, State.SEALING, null, executor).join();
        sealStreamTask.execute(sealEvent).join();
        assertEquals(State.SEALED, streamStore.getState(scope, stream, true, null, executor).join());

        // replay old event. it should not seal the stream
        AssertExtensions.assertFutureThrows("Replaying older delete event should have no effect",
                deleteStreamTask.execute(firstDeleteEvent), e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);
        DeleteStreamEvent secondDeleteEvent = new DeleteStreamEvent(scope, stream, 0L, newCreationTime);
        // now delete should succeed
        deleteStreamTask.execute(secondDeleteEvent).join();
    }

    private void createStreamInStore(String stream, String scopeName) {
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(
                ScalingPolicy.byEventRate(1, 2, 1)).build();

        streamStore.createStream(scopeName, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore.setState(scopeName, stream, State.ACTIVE, null, executor).join();
    }

    @Test
    public void testScaleIgnoreFairness() {
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtTasks, executor),
                executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).sealSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());
        
        // 2. start scale --> this should fail with a retryable exception while talking to segment store!
        ScaleOpEvent scaleEvent = new ScaleOpEvent(fairness, fairness, Collections.singletonList(0L), 
                Collections.singletonList(new AbstractMap.SimpleEntry<>(0.0, 1.0)), 
                false, System.currentTimeMillis(), 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(scaleEvent, () -> false),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);
        // verify that scale was started
        assertEquals(State.SCALING, streamStore.getState(fairness, fairness, true, null, executor).join());

        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();
        
        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(true))
                .when(segmentHelper).sealSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());
        
        // 5. process again. it should succeed while ignoring waiting processor
        streamRequestHandler.process(scaleEvent, () -> false).join();
        EpochRecord activeEpoch = streamStore.getActiveEpoch(fairness, fairness, null, true, executor).join();
        assertEquals(1, activeEpoch.getEpoch());
        assertEquals(State.ACTIVE, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        // 6. run a new scale. it should fail because of waiting processor.
        ScaleOpEvent scaleEvent2 = new ScaleOpEvent(fairness, fairness, Collections.singletonList(NameUtils.computeSegmentId(1, 1)),
                Collections.singletonList(new AbstractMap.SimpleEntry<>(0.0, 1.0)),
                false, System.currentTimeMillis(), 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(scaleEvent2, () -> false),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        streamStore.deleteWaitingRequestConditionally(fairness, fairness, "myProcessor", null, executor).join();
    }
    
    @Test
    public void testUpdateIgnoreFairness() {
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtTasks, executor),
                executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).updatePolicy(anyString(), anyString(), any(), anyLong(), anyString(), anyLong());
        
        // 2. start process --> this should fail with a retryable exception while talking to segment store!
        streamStore.startUpdateConfiguration(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                null, executor).join();
        streamStore.setState(fairness, fairness, State.UPDATING, null, executor).join();
        assertEquals(State.UPDATING, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        UpdateStreamEvent event = new UpdateStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event, () -> false),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        verify(segmentHelper, atLeastOnce()).updatePolicy(anyString(), anyString(), any(), anyLong(), anyString(), anyLong());
        
        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();
        
        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(segmentHelper).updatePolicy(anyString(), anyString(), any(), anyLong(), anyString(), anyLong());
        
        // 5. process again. it should succeed while ignoring waiting processor
        streamRequestHandler.process(event, () -> false).join();
        assertEquals(State.ACTIVE, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        // 6. run a new update. it should fail because of waiting processor and our state does not allow us to ignore waiting processor
        UpdateStreamEvent event2 = new UpdateStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event2, () -> false),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        streamStore.deleteWaitingRequestConditionally(fairness, fairness, "myProcessor", null, executor).join();
    }

    @Test
    public void testTruncateIgnoreFairness() {
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtTasks, executor),
                executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).truncateSegment(anyString(), anyString(), anyLong(), anyLong(), anyString(), anyLong());
        
        // 2. start process --> this should fail with a retryable exception while talking to segment store!
        streamStore.startTruncation(fairness, fairness, Collections.singletonMap(0L, 0L), null, executor).join();
        streamStore.setState(fairness, fairness, State.TRUNCATING, null, executor).join();
        assertEquals(State.TRUNCATING, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        TruncateStreamEvent event = new TruncateStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event, () -> false),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        verify(segmentHelper, atLeastOnce()).truncateSegment(anyString(), anyString(), anyLong(), anyLong(), anyString(), anyLong());
        
        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();
        
        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(segmentHelper).truncateSegment(anyString(), anyString(), anyLong(), anyLong(), anyString(), anyLong());
        
        // 5. process again. it should succeed while ignoring waiting processor
        streamRequestHandler.process(event, () -> false).join();
        assertEquals(State.ACTIVE, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        // 6. run a new update. it should fail because of waiting processor.
        TruncateStreamEvent event2 = new TruncateStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event2, () -> false),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        streamStore.deleteWaitingRequestConditionally(fairness, fairness, "myProcessor", null, executor).join();
    }

    @Test(timeout = 10000)
    public void testCommitFailureOnNoSuchSegment() {
        CommitRequestHandler requestHandler = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        String noSuchSegment = "noSuchSegment";
        streamStore.createScope(noSuchSegment, null, executor).join();
        streamMetadataTasks.createStream(noSuchSegment, noSuchSegment, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        UUID txn = streamTransactionMetadataTasks.createTxn(noSuchSegment, noSuchSegment, 30000, 0L, 1024 * 1024L).join().getKey().getId();
        streamStore.sealTransaction(noSuchSegment, noSuchSegment, txn, true, Optional.empty(), "", Long.MIN_VALUE,
                null, executor).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new WireCommandFailedException(WireCommandType.NO_SUCH_SEGMENT, WireCommandFailedException.Reason.SegmentDoesNotExist)))
                .when(segmentHelper).mergeTxnSegments(anyString(), anyString(), anyLong(), anyLong(), any(),
                        anyString(), anyLong());

        streamStore.startCommitTransactions(noSuchSegment, noSuchSegment, 100, null, executor).join();
        streamStore.setState(noSuchSegment, noSuchSegment, State.COMMITTING_TXN, null, executor).join();

        assertEquals(State.COMMITTING_TXN, streamStore.getState(noSuchSegment, noSuchSegment, true, null, executor).join());

        CommitEvent event = new CommitEvent(noSuchSegment, noSuchSegment, 0);
        AssertExtensions.assertFutureThrows("", requestHandler.process(event, () -> false),
                e -> Exceptions.unwrap(e) instanceof IllegalStateException);

        verify(segmentHelper, atLeastOnce()).mergeTxnSegments(anyString(), anyString(), anyLong(), anyLong(), any(),
                anyString(), anyLong());
    }

    @Test
    public void testCommitTxnIgnoreFairness() {
        CommitRequestHandler requestHandler = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        UUID txn = streamTransactionMetadataTasks.createTxn(fairness, fairness, 30000, 0L, 1024 * 1024L).join().getKey().getId();
        streamStore.sealTransaction(fairness, fairness, txn, true, Optional.empty(), "", Long.MIN_VALUE, 
                null, executor).join();
        
        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).mergeTxnSegments(anyString(), anyString(), anyLong(), anyLong(), any(),
                anyString(), anyLong());
        
        streamStore.startCommitTransactions(fairness, fairness, 100, null, executor).join();
        
        // 2. start process --> this should fail with a retryable exception while talking to segment store!
        streamStore.setState(fairness, fairness, State.COMMITTING_TXN, null, executor).join();

        assertEquals(State.COMMITTING_TXN, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        CommitEvent event = new CommitEvent(fairness, fairness, 0);
        AssertExtensions.assertFutureThrows("", requestHandler.process(event, () -> false), e -> Exceptions.unwrap(e) instanceof RuntimeException);

        verify(segmentHelper, atLeastOnce()).mergeTxnSegments(anyString(), anyString(), anyLong(), anyLong(), any(),
                anyString(), anyLong());
        
        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();
        
        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(0L))
                .when(segmentHelper).mergeTxnSegments(anyString(), anyString(), anyLong(), anyLong(), any(),
                anyString(), anyLong());
        
        // 5. process again. it should succeed while ignoring waiting processor
        requestHandler.process(event, () -> false).join();
        assertEquals(State.ACTIVE, streamStore.getState(fairness, fairness, true, null, executor).join());
        
        // 6. run a new update. it should fail because of waiting processor.
        CommitEvent event2 = new CommitEvent(fairness, fairness, 0);
        AssertExtensions.assertFutureThrows("", requestHandler.process(event2, () -> false),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        streamStore.deleteWaitingRequestConditionally(fairness, fairness, "myProcessor", null, executor).join();
    }

    @Test
    public void testSealIgnoreFairness() {
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtTasks, executor),
                executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                System.currentTimeMillis(), 0L).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).sealSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 2. start process --> this should fail with a retryable exception while talking to segment store!
        streamStore.setState(fairness, fairness, State.SEALING, null, executor).join();
        assertEquals(State.SEALING, streamStore.getState(fairness, fairness, true, null, executor).join());

        SealStreamEvent event = new SealStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event, () -> false),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        verify(segmentHelper, atLeastOnce())
                .sealSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();

        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(segmentHelper).sealSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 5. process again. it should succeed while ignoring waiting processor
        streamRequestHandler.process(event, () -> false).join();
        assertEquals(State.SEALED, streamStore.getState(fairness, fairness, true, null, executor).join());

        // 6. run a new update. it should fail because of waiting processor.
        SealStreamEvent event2 = new SealStreamEvent(fairness, fairness, 0L);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event2, () -> false),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        streamStore.deleteWaitingRequestConditionally(fairness, fairness, "myProcessor", null, executor).join();
    }

    @Test
    public void testDeleteIgnoreFairness() {
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, executor),
                new TruncateStreamTask(streamMetadataTasks, streamStore, executor),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, executor),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, executor),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtTasks, executor),
                executor);
        String fairness = "fairness";
        streamStore.createScope(fairness, null, executor).join();
        long createTimestamp = System.currentTimeMillis();
        streamMetadataTasks.createStream(fairness, fairness, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(),
                createTimestamp, 0L).join();

        // 1. set segment helper mock to throw exception
        doAnswer(x -> Futures.failedFuture(new RuntimeException()))
                .when(segmentHelper).deleteSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 2. start process --> this should fail with a retryable exception while talking to segment store!
        streamStore.setState(fairness, fairness, State.SEALED, null, executor).join();
        assertEquals(State.SEALED, streamStore.getState(fairness, fairness, true, null, executor).join());

        DeleteStreamEvent event = new DeleteStreamEvent(fairness, fairness, 0L, createTimestamp);
        AssertExtensions.assertFutureThrows("", streamRequestHandler.process(event, () -> false),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        verify(segmentHelper, atLeastOnce())
                .deleteSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 3. set waiting processor to "random name"
        streamStore.createWaitingRequestIfAbsent(fairness, fairness, "myProcessor", null, executor).join();

        // 4. reset segment helper to return success
        doAnswer(x -> CompletableFuture.completedFuture(null))
                .when(segmentHelper).deleteSegment(anyString(), anyString(), anyLong(), anyString(), anyLong());

        // 5. process again. it should succeed while ignoring waiting processor
        streamRequestHandler.process(event, () -> false).join();
        AssertExtensions.assertFutureThrows("", streamStore.getState(fairness, fairness, true, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }
}
