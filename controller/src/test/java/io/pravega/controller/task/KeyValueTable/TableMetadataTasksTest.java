/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.KeyValueTable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.task.EventHelper;
import io.pravega.shared.controller.event.ControllerEvent;

import lombok.Data;
import lombok.Getter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public abstract class TableMetadataTasksTest {

    protected static final String SCOPE = "taskscope";
    protected StreamMetadataStore streamStore;
    protected KVTableMetadataStore kvtStore;
    protected TableMetadataTasks kvtMetadataTasks;
    protected SegmentHelper segmentHelperMock;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final String kvtable1 = "kvtable1";
    private boolean isScopeCreated;
    private RequestTracker requestTracker = new RequestTracker(true);
    private EventStreamWriter<ControllerEvent> requestEventWriter = new WriterMock();
    private TableRequestHandler tableRequestHandler;

    @Before
    public void setup() throws Exception {
        StreamMetrics.initialize();
        setupStores();
        CreateScopeStatus scopeCreationStatus = this.streamStore.createScope(SCOPE).get();
        if (scopeCreationStatus.getStatus().equals(CreateScopeStatus.Status.SCOPE_EXISTS)
                || scopeCreationStatus.getStatus().equals(CreateScopeStatus.Status.SUCCESS)) {
            this.isScopeCreated = true;
        }

        segmentHelperMock = getSegmentHelper();
        EventHelper helper = new EventHelper(executor, "host", ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        helper.setRequestEventWriter(requestEventWriter);
        kvtMetadataTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                 "host", GrpcAuthHelper.getDisabledAuthHelper(),
                requestTracker, helper));
        this.tableRequestHandler = new TableRequestHandler(new CreateTableTask(this.kvtStore, this.kvtMetadataTasks, executor), this.kvtStore, executor);
    }

    public abstract void setupStores() throws Exception;

    public abstract void cleanupStores() throws Exception;

    abstract SegmentHelper getSegmentHelper();

    @After
    public void tearDown() throws Exception {
        cleanupStores();
        StreamMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testCreateKeyValueTable() throws ExecutionException, InterruptedException {
        Assert.assertTrue(isScopeCreated);
        long creationTime = System.currentTimeMillis();
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).build();
        CompletableFuture<Controller.CreateKeyValueTableStatus.Status> createOperationFuture
                = kvtMetadataTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime);

        assertTrue(Futures.await(processEvent((TableMetadataTasksTest.WriterMock) requestEventWriter)));
        assertEquals(CreateKeyValueTableStatus.Status.SUCCESS, createOperationFuture.join());
        List<KVTSegmentRecord> segmentsList = kvtStore.getActiveSegments(SCOPE, kvtable1, null, executor).get();
        assertEquals(segmentsList.size(), kvtConfig.getPartitionCount());

        long storedCreationTime = kvtStore.getCreationTime(SCOPE, kvtable1, null, executor).get();
        assertEquals(storedCreationTime, creationTime);

        KeyValueTableConfiguration storedConfig = kvtStore.getConfiguration(SCOPE, kvtable1, null, executor).get();
        assertEquals(storedConfig.getPartitionCount(), kvtConfig.getPartitionCount());

        // check retry failures...
        EventHelper mockHelper = EventHelperMock.getFailingEventHelperMock();
        TableMetadataTasks kvtFailingMetaTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(),
                requestTracker, mockHelper));
        CreateKeyValueTableStatus.Status status = kvtFailingMetaTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime).get();
        assertEquals(CreateKeyValueTableStatus.Status.FAILURE, status);
    }

    private CompletableFuture<Void> processEvent(TableMetadataTasksTest.WriterMock requestEventWriter) throws InterruptedException {
        ControllerEvent event;
        try {
            event = requestEventWriter.getEventQueue().take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return tableRequestHandler.processEvent(event)
                .exceptionally(e -> {
                    requestEventWriter.getEventQueue().add(event);
                    throw new CompletionException(e);
                });
    }

    @Test(timeout = 30000)
    public void testWorkflowCompletionTimeout() {
        /*
        EventHelper helper = EventHelperMock.getEventHelperMock(executor, "host", ((AbstractStreamMetadataStore) kvtStorePartialMock).getHostTaskIndex());

        StreamMetadataTasks streamMetadataTask = new StreamMetadataTasks(kvtStorePartialMock, bucketStore,
                TaskStoreFactory.createZKStore(zkClient, executor),
                SegmentHelperMock.getSegmentHelperMock(), executor, "host",
                new GrpcAuthHelper(authEnabled, "key", 300), requestTracker, helper);
        streamMetadataTask.setCompletionTimeoutMillis(500L);
        StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();

        String completion = "completion";
        kvtStorePartialMock.createStream(SCOPE, completion, configuration, System.currentTimeMillis(), null, executor).join();
        kvtStorePartialMock.setState(SCOPE, completion, State.ACTIVE, null, executor).join();

        WriterMock requestEventWriter = new WriterMock(streamMetadataTask, executor);
        streamMetadataTask.setRequestEventWriter(requestEventWriter);
        
        StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(3)).build();

        AssertExtensions.assertFutureThrows("update timedout", 
                streamMetadataTask.updateStream(SCOPE, completion, configuration2, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        ControllerEvent event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof UpdateStreamEvent);
        VersionedMetadata<StreamConfigurationRecord> configurationRecord = kvtStorePartialMock
                .getConfigurationRecord(SCOPE, completion, null, executor).join();
        assertTrue(configurationRecord.getObject().isUpdating());

        Map<Long, Long> streamCut = Collections.singletonMap(0L, 0L);
        AssertExtensions.assertFutureThrows("truncate timedout",
                streamMetadataTask.truncateStream(SCOPE, completion, streamCut, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof TruncateStreamEvent);
        
        VersionedMetadata<StreamTruncationRecord> truncationRecord = kvtStorePartialMock
                .getTruncationRecord(SCOPE, completion, null, executor).join();
        assertTrue(truncationRecord.getObject().isUpdating());

        AssertExtensions.assertFutureThrows("seal timedout",
                streamMetadataTask.sealStream(SCOPE, completion, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof SealStreamEvent);
        
        VersionedMetadata<State> state = kvtStorePartialMock
                .getVersionedState(SCOPE, completion, null, executor).join();
        assertEquals(state.getObject(), State.SEALING);

        kvtStorePartialMock.setState(SCOPE, completion, State.SEALED, null, executor).join();

        AssertExtensions.assertFutureThrows("delete timedout",
                streamMetadataTask.deleteStream(SCOPE, completion, null),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        event = requestEventWriter.eventQueue.poll();
        assertTrue(event instanceof DeleteStreamEvent);
         */
    }

    @Data
    public class WriterMock implements EventStreamWriter<ControllerEvent> {
        @Getter
        private LinkedBlockingQueue<ControllerEvent> eventQueue = new LinkedBlockingQueue<>();

        @Override
        public CompletableFuture<Void> writeEvent(ControllerEvent event) {
            this.eventQueue.add(event);

            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> writeEvent(String routingKey, ControllerEvent event) {
            return writeEvent(event);
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
}