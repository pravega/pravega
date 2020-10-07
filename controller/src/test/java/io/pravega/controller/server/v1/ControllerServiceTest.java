/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.v1;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

/**
 * Controller service implementation test.
 */
public class ControllerServiceTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore streamStore = spy(StreamStoreFactory.createInMemoryStore(executor));
    private final KVTableMetadataStore kvtStore = spy(KVTableStoreFactory.createInMemoryStore(streamStore, executor));

    private StreamMetadataTasks streamMetadataTasks;
    private TableMetadataTasks kvtMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private ConnectionPool connectionPool;
    private ControllerService consumer;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private long startTs;
    private long scaleTs;

    private RequestTracker requestTracker = new RequestTracker(true);
    
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), new SocketConnectionFactoryImpl(ClientConfig.builder().build()));

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore,
                segmentHelper, executor, "host", GrpcAuthHelper.getDisabledAuthHelper(), requestTracker);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                segmentHelper, executor, "host", GrpcAuthHelper.getDisabledAuthHelper());

        kvtMetadataTasks = new TableMetadataTasks(kvtStore, segmentHelper,  executor,  executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), requestTracker);
        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, streamTransactionMetadataTasks,
                new SegmentHelper(connectionPool, hostStore, executor), executor, null);
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(policy2).build();

        // createScope
        streamStore.createScope(SCOPE).get();

        // region createStream
        startTs = System.currentTimeMillis();
        OperationContext context = streamStore.createContext(SCOPE, stream1);
        streamStore.createStream(SCOPE, stream1, configuration1, startTs, context, executor).get();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, context, executor);

        OperationContext context2 = streamStore.createContext(SCOPE, stream2);
        streamStore.createStream(SCOPE, stream2, configuration2, startTs, context2, executor).get();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, context2, executor);

        // endregion

        // region scaleSegments

        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);
        scaleTs = System.currentTimeMillis();
        VersionedMetadata<EpochTransitionRecord> record = streamStore.submitScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), startTs,
                null, null, executor).get();
        VersionedMetadata<State> state = streamStore.getVersionedState(SCOPE, stream1, null, executor).get();
        state = streamStore.updateVersionedState(SCOPE, stream1, State.SCALING, state, null, executor).get();
        record = streamStore.startScale(SCOPE, stream1, false, record, state, null, executor).get();
        streamStore.scaleCreateNewEpochs(SCOPE, stream1, record, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream1, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), record,
                null, executor).get();
        streamStore.completeScale(SCOPE, stream1, record, null, executor).get();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        sealedSegments = Arrays.asList(0L, 1L, 2L);
        record = streamStore.submitScale(SCOPE, stream2, sealedSegments, Arrays.asList(segment3, segment4, segment5),
                scaleTs, null, null, executor).get();
        state = streamStore.getVersionedState(SCOPE, stream2, null, executor).get();
        state = streamStore.updateVersionedState(SCOPE, stream2, State.SCALING, state, null, executor).get();
        record = streamStore.startScale(SCOPE, stream2, false, record, state, null, executor).get();
        streamStore.scaleCreateNewEpochs(SCOPE, stream2, record, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream2, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), record,
                null, executor).get();
        streamStore.completeScale(SCOPE, stream2, record, null, executor).get();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, null, executor).get();

        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamTransactionMetadataTasks.close();
        streamMetadataTasks.close();
        connectionPool.close();
        streamStore.close();
        zkClient.close();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 10000L)
    public void testMethods() throws InterruptedException, ExecutionException {
        Map<SegmentId, Long> segments;

        segments = consumer.getSegmentsAtHead(SCOPE, stream1).get();
        assertEquals(2, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 1)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream1).get();
        assertEquals(2, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 1)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream2).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 1)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 2)));

        segments = consumer.getSegmentsAtHead(SCOPE, stream2).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 1)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 2)));
    }

    @Test(timeout = 10000L)
    public void testTransactions() {
        TransactionMetrics.initialize();
        UUID txnId = consumer.createTransaction(SCOPE, stream1, 10000L).join().getKey();
        doThrow(StoreException.create(StoreException.Type.WRITE_CONFLICT, "Write conflict"))
                .when(streamStore).sealTransaction(eq(SCOPE), eq(stream1), eq(txnId), anyBoolean(), any(), anyString(), anyLong(),
                any(), any());

        AssertExtensions.assertFutureThrows("Write conflict should have been thrown",
                consumer.commitTransaction(SCOPE, stream1, txnId, "", 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        AssertExtensions.assertFutureThrows("Write conflict should have been thrown",
                consumer.abortTransaction(SCOPE, stream1, txnId),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        doThrow(StoreException.create(StoreException.Type.CONNECTION_ERROR, "Connection failed"))
                .when(streamStore).sealTransaction(eq(SCOPE), eq(stream1), eq(txnId), anyBoolean(), any(), anyString(), anyLong(),
                any(), any());

        AssertExtensions.assertFutureThrows("Store connection exception should have been thrown",
                consumer.commitTransaction(SCOPE, stream1, txnId, "", 0L),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        AssertExtensions.assertFutureThrows("Store connection exception should have been thrown",
                consumer.abortTransaction(SCOPE, stream1, txnId),
                e -> Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException);

        doThrow(StoreException.create(StoreException.Type.UNKNOWN, "Connection failed"))
                .when(streamStore).sealTransaction(eq(SCOPE), eq(stream1), eq(txnId), anyBoolean(), any(), anyString(), anyLong(),
                any(), any());

        Controller.TxnStatus status = consumer.commitTransaction(SCOPE, stream1, txnId, "", 0L).join();
        assertEquals(status.getStatus(), Controller.TxnStatus.Status.FAILURE);

        status = consumer.abortTransaction(SCOPE, stream1, txnId).join();
        assertEquals(status.getStatus(), Controller.TxnStatus.Status.FAILURE);
        reset(streamStore);
    }
    
    @Test(timeout = 10000L)
    public void testWriterMark() {
        String scope = "mark";
        String stream = "mark";
        String writerId = "writer";
        // partially create stream
        doAnswer(x -> CompletableFuture.completedFuture(null)).when(streamStore).createStream(eq(scope), eq(stream), any(), anyLong(), any(), any());

        doAnswer(x -> Futures.failedFuture(StoreException.create(StoreException.Type.WRITE_CONFLICT, "write conflict")))
                .when(streamStore).noteWriterMark(eq(scope), eq(stream), eq(writerId), anyLong(), any(), any(), any());
        
        AssertExtensions.assertFutureThrows("Exception should be thrown to the caller", 
                consumer.noteTimestampFromWriter(scope, stream, writerId, 100L, Collections.singletonMap(1L, 1L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        doAnswer(x -> Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "data not found")))
                .when(streamStore).noteWriterMark(eq(scope), eq(stream), eq(writerId), anyLong(), any(), any(), any());

        AssertExtensions.assertFutureThrows("Exception should be thrown to the caller", 
                consumer.noteTimestampFromWriter(scope, stream, writerId, 100L, Collections.singletonMap(1L, 1L)),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
        
    }
}
