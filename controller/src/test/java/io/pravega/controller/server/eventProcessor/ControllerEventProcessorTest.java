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

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Controller Event ProcessorTests.
 */
public abstract class ControllerEventProcessorTest {
    private static final RetryPolicy RETRY_POLICY = new RetryOneTime(2000);
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource(RETRY_POLICY);
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    protected CuratorFramework zkClient;
    protected ScheduledExecutorService executor;
    protected StreamMetadataStore streamStore;
    protected BucketStore bucketStore;
    protected StreamMetadataTasks streamMetadataTasks;
    protected StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private EventHelper eventHelperMock;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        zkClient.start();

        streamStore = createStore();
        bucketStore = StreamStoreFactory.createZKBucketStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        eventHelperMock = EventHelperMock.getEventHelperMock(executor, "1", ((AbstractStreamMetadataStore) this.streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, TaskStoreFactory.createInMemoryStore(executor),
                segmentHelperMock, executor, "1", GrpcAuthHelper.getDisabledAuthHelper(), eventHelperMock);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock,
                executor, "host", GrpcAuthHelper.getDisabledAuthHelper());
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        // region createStream
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        streamStore.createScope(SCOPE, null, executor).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, STREAM, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // endregion
    }

    abstract StreamMetadataStore createStore();

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 10000)
    public void testCommitEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, txnData.getEpoch())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
    }

    @Test(timeout = 60000)
    public void testCommitEventForSealingStream() {
        ScaleOperationTask scaleTask = new ScaleOperationTask(streamMetadataTasks, streamStore, executor);
        SealStreamTask sealStreamTask = new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, executor);

        String stream = "commitWithSeal";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        streamStore.createStream(SCOPE, stream, config, System.currentTimeMillis(), null, executor).join();
        streamStore.setState(SCOPE, stream, State.ACTIVE, null, executor).join();
        
        UUID txnOnEpoch0 = streamStore.generateTransactionId(SCOPE, stream, null, executor).join();
        VersionedTransactionData txnData0 = streamStore.createTransaction(SCOPE, stream, txnOnEpoch0, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData0);
        checkTransactionState(SCOPE, stream, txnOnEpoch0, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, stream, txnData0.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();
        checkTransactionState(SCOPE, stream, txnData0.getId(), TxnStatus.COMMITTING);

        // scale stream
        List<Map.Entry<Double, Double>> newRange = new LinkedList<>();
        newRange.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        scaleTask.execute(new ScaleOpEvent(SCOPE, stream, Collections.singletonList(0L), newRange, false, System.currentTimeMillis(), 0L)).join();

        UUID txnOnEpoch1 = streamStore.generateTransactionId(SCOPE, stream, null, executor).join();
        VersionedTransactionData txnData1 = streamStore.createTransaction(SCOPE, stream, txnOnEpoch1, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData1);
        checkTransactionState(SCOPE, stream, txnOnEpoch1, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, stream, txnData1.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();
        checkTransactionState(SCOPE, stream, txnData1.getId(), TxnStatus.COMMITTING);

        // set the stream to SEALING
        streamStore.setState(SCOPE, stream, State.SEALING, null, executor).join();

        // attempt to seal the stream. This should fail with postponement. 
        AssertExtensions.assertFutureThrows("Seal stream should fail with operation not allowed as their are outstanding transactions", 
                sealStreamTask.execute(new SealStreamEvent(SCOPE, stream, 0L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        
        // now attempt to commit the transaction on epoch 1. epoch in commit event is ignored and transactions on lowest epoch 
        // should be committed first. 
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, stream, txnData1.getEpoch())).join();
        checkTransactionState(SCOPE, stream, txnData0.getId(), TxnStatus.COMMITTED);
        checkTransactionState(SCOPE, stream, txnData1.getId(), TxnStatus.COMMITTING);

        EpochRecord activeEpoch = streamStore.getActiveEpoch(SCOPE, stream, null, true, executor).join();
        assertEquals(3, activeEpoch.getEpoch());
        assertEquals(1, activeEpoch.getReferenceEpoch());

        // attempt to seal the stream. This should still fail with postponement. 
        AssertExtensions.assertFutureThrows("Seal stream should fail with operation not allowed as their are outstanding transactions",
                sealStreamTask.execute(new SealStreamEvent(SCOPE, stream, 0L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        // now attempt to commit the transaction on epoch 1. 
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, stream, txnData1.getEpoch())).join();
        checkTransactionState(SCOPE, stream, txnData1.getId(), TxnStatus.COMMITTED);

        // verify transaction has rolled over
        activeEpoch = streamStore.getActiveEpoch(SCOPE, stream, null, true, executor).join();
        assertEquals(3, activeEpoch.getEpoch());
        assertEquals(1, activeEpoch.getReferenceEpoch());
        
        // now attempt to seal the stream. it should complete. 
        sealStreamTask.execute(new SealStreamEvent(SCOPE, stream, 0L)).join();
    }
    
    @Test(timeout = 10000)
    public void testMultipleTransactionsSuccess() {
        // 1. commit request for an older epoch
        // this should be ignored
        // 2. multiple transactions in committing state
        // first event should commit all of them
        // subsequent events should be no op
        // 3. commit request for future epoch
        // this should be ignored as there is nothing in the epoch to commit
        // scale stream
        List<VersionedTransactionData> txnDataList = createAndCommitTransactions(3);
        int epoch = txnDataList.get(0).getEpoch();
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch - 1)).join();
        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch - 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch + 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch))));
    }

    @Test(timeout = 10000)
    public void testTransactionOutstandingCommit() {
        // keep a committxnlist in the store
        // same epoch --> commit txn list should be cleared first
        //     subsequent events should complete remainder txns that are in committing state
        // lower epoch --> rolling transaction
        // higher epoch --> no transactions can exist on higher epoch. nothing to do. ignore.
        List<VersionedTransactionData> txnDataList1 = createAndCommitTransactions(3);
        int epoch = txnDataList1.get(0).getEpoch();
        streamStore.startCommitTransactions(SCOPE, STREAM, 100, null, executor).join();

        List<VersionedTransactionData> txnDataList2 = createAndCommitTransactions(3);

        streamMetadataTasks.setRequestEventWriter(new EventStreamWriterMock<>());
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);

        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList1) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);
        }

        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }
    }

    @Test(timeout = 30000)
    public void testCommitAndStreamProcessorFairness() {
        List<VersionedTransactionData> txnDataList1 = createAndCommitTransactions(3);
        int epoch = txnDataList1.get(0).getEpoch();
        streamStore.startCommitTransactions(SCOPE, STREAM, 100, null, executor).join();

        EventStreamWriterMock<ControllerEvent> requestEventWriter = new EventStreamWriterMock<>();
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);
        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, executor),
                new ScaleOperationTask(streamMetadataTasks, streamStore, executor),
                null, null, null, null, null, null, null, streamStore, null, executor);

        // set some processor name so that the processing gets postponed
        streamStore.createWaitingRequestIfAbsent(SCOPE, STREAM, "test", null, executor).join();
        AssertExtensions.assertFutureThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        streamStore.deleteWaitingRequestConditionally(SCOPE, STREAM, "test1", null, executor).join();
        assertEquals("test", streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        // now remove the barrier but change the state so that processing can not happen.
        streamStore.deleteWaitingRequestConditionally(SCOPE, STREAM, "test", null, executor).join();
        assertNull(streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());
        streamStore.setState(SCOPE, STREAM, State.SCALING, null, executor).join();

        AssertExtensions.assertFutureThrows("Operation should be disallowed", commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        assertEquals(commitEventProcessor.getProcessorName(), streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // verify that we are able to process if the waiting processor name is same as ours.
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, epoch)).join();

        // verify that waiting processor is cleaned up.
        assertNull(streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());

        // now set the state to COMMITTING_TXN and try the same with scaling
        streamStore.setState(SCOPE, STREAM, State.COMMITTING_TXN, null, executor).join();

        // verify that event that does not try to use `processor.withCompletion` runs without contention
        assertTrue(Futures.await(streamRequestHandler.processEvent(new AutoScaleEvent(SCOPE, STREAM, 0L,
                AutoScaleEvent.UP, 0L, 2, true, 0L))));

        // now same event's processing in face of a barrier should get postponed
        streamStore.createWaitingRequestIfAbsent(SCOPE, STREAM, commitEventProcessor.getProcessorName(), null, executor).join();
        assertTrue(Futures.await(streamRequestHandler.processEvent(new AutoScaleEvent(SCOPE, STREAM, 0L,
                AutoScaleEvent.UP, 0L, 2, true, 0L))));

        AssertExtensions.assertFutureThrows("Operation should be disallowed", streamRequestHandler.processEvent(
                new ScaleOpEvent(SCOPE, STREAM, Collections.emptyList(), Collections.emptyList(), false, 0L, 0L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        assertEquals(commitEventProcessor.getProcessorName(), streamStore.getWaitingRequestProcessor(SCOPE, STREAM, null, executor).join());
    }

    protected List<VersionedTransactionData> createAndCommitTransactions(int count) {
        List<VersionedTransactionData> retVal = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
            VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                    null, executor).join();
            Assert.assertNotNull(txnData);
            checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

            streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

            retVal.add(txnData);
        }
        return retVal;
    }

    @Test(timeout = 10000)
    public void testAbortEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), false, Optional.empty(), "", Long.MIN_VALUE, null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTING);

        AbortRequestHandler abortRequestHandler = new AbortRequestHandler(streamStore, streamMetadataTasks, executor);
        abortRequestHandler.processEvent(new AbortEvent(SCOPE, STREAM, txnData.getEpoch(), txnData.getId(), 11L)).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTED);
    }

    protected void checkTransactionState(String scope, String stream, UUID txnId, TxnStatus expectedStatus) {
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnId, null, executor).join();
        assertEquals(expectedStatus, txnStatus);
    }

    @Test(timeout = 10000)
    public void testMarkOnCommit() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        String writer1 = "writer1";
        long timestamp = 1L;
        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), writer1, timestamp, null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

        CommitRequestHandler commitEventProcessor = new CommitRequestHandler(streamStore, streamMetadataTasks, streamTransactionMetadataTasks, bucketStore, executor);
        commitEventProcessor.processEvent(new CommitEvent(SCOPE, STREAM, txnData.getEpoch())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);

        WriterMark mark = streamStore.getWriterMark(SCOPE, STREAM, writer1, null, executor).join();
        assertEquals(mark.getTimestamp(), timestamp);
    }
}
