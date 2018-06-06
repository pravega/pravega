/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AbortRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.CommitTransactionTask;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.TxnStatus;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import io.pravega.shared.controller.event.CommitEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

/**
 * Controller Event ProcessorTests.
 */
public class ControllerEventProcessorTest {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    private ScheduledExecutorService executor;
    private StreamMetadataStore streamStore;
    private StreamMetadataTasks streamMetadataTasks;
    private HostControllerStore hostStore;
    private TestingServer zkServer;
    private SegmentHelper segmentHelperMock;
    private CuratorFramework zkClient;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newScheduledThreadPool(10);

        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);
        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, TaskStoreFactory.createInMemoryStore(executor),
                segmentHelperMock, executor, "1", mock(ConnectionFactory.class), false, "");
        // region createStream
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(STREAM).scalingPolicy(policy1).build();
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, STREAM, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, STREAM, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 10000)
    public void testCommitEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

        CommitTransactionTask commitEventProcessor = new CommitTransactionTask(streamStore, streamMetadataTasks, executor);
        commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, txnData.getEpoch())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
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
        List<VersionedTransactionData> txnDataList = createAndCommitTransactions(3);
        int epoch = txnDataList.get(0).getEpoch();
        CommitTransactionTask commitEventProcessor = new CommitTransactionTask(streamStore, streamMetadataTasks, executor);
        commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        Assert.assertTrue(Futures.await(commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch - 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch + 1))));
        Assert.assertTrue(Futures.await(commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch))));
    }

    @Test(timeout = 10000)
    public void testTransactionOutstandingCommit() {
        // keep a committxnlist in the store
        // same epoch --> commit txn list should be cleared first
        //     subsequent events should complete remainder txns that are in committing state
        // lower epoch --> rolling transaction
        // higher epoch --> no transactions can exist on higher epoch. nothing to do. ignore.
        List<VersionedTransactionData> txnDataList1 = createAndCommitTransactions(3);
        List<VersionedTransactionData> txnDataList2 = createAndCommitTransactions(3);
        int epoch = txnDataList1.get(0).getEpoch();
        streamStore.createCommittingTransactionsRecord(SCOPE, STREAM, epoch, txnDataList1.stream().map(VersionedTransactionData::getId).collect(Collectors.toList()), null, executor).join();

        CommitTransactionTask commitEventProcessor = new CommitTransactionTask(streamStore, streamMetadataTasks, executor);

        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch - 1)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);
        AssertExtensions.assertThrows("Operation should be disallowed", commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch + 1)),
                e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException);

        commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList1) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }

        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);
        }

        commitEventProcessor.execute(new CommitEvent(SCOPE, STREAM, epoch)).join();
        for (VersionedTransactionData txnData : txnDataList2) {
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTED);
        }
    }

    private List<VersionedTransactionData> createAndCommitTransactions(int count) {
        List<VersionedTransactionData> retVal = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
            VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000, 10000,
                    null, executor).join();
            Assert.assertNotNull(txnData);
            checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

            streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), true, Optional.empty(), null, executor).join();
            checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.COMMITTING);

            retVal.add(txnData);
        }
        return retVal;
    }

    @Test(timeout = 10000)
    public void testAbortEventProcessor() {
        UUID txnId = streamStore.generateTransactionId(SCOPE, STREAM, null, executor).join();
        VersionedTransactionData txnData = streamStore.createTransaction(SCOPE, STREAM, txnId, 10000, 10000, 10000,
                null, executor).join();
        Assert.assertNotNull(txnData);
        checkTransactionState(SCOPE, STREAM, txnId, TxnStatus.OPEN);

        streamStore.sealTransaction(SCOPE, STREAM, txnData.getId(), false, Optional.empty(), null, executor).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTING);

        AbortRequestHandler abortRequestHandler = new AbortRequestHandler(streamStore, streamMetadataTasks, executor);
        abortRequestHandler.processEvent(new AbortEvent(SCOPE, STREAM, txnData.getEpoch(), txnData.getId())).join();
        checkTransactionState(SCOPE, STREAM, txnData.getId(), TxnStatus.ABORTED);
    }

    private void checkTransactionState(String scope, String stream, UUID txnId, TxnStatus expectedStatus) {
        TxnStatus txnStatus = streamStore.transactionStatus(scope, stream, txnId, null, executor).join();
        Assert.assertEquals(expectedStatus, txnStatus);
    }
}
