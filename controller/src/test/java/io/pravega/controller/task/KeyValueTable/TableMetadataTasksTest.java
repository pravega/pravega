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
package io.pravega.controller.task.KeyValueTable;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.CreateTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.DeleteTableTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.kvtable.TableRequestHandler;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.MetricsTestUtil;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.AssertExtensions;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public abstract class TableMetadataTasksTest {

    protected static final String SCOPE = "taskscope";
    protected StreamMetadataStore streamStore;
    protected KVTableMetadataStore kvtStore;
    protected TableMetadataTasks kvtMetadataTasks;
    protected SegmentHelper segmentHelperMock;
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    private final String kvtable1 = "kvtable1";
    private boolean isScopeCreated;
    private RequestTracker requestTracker = new RequestTracker(true);
    private EventStreamWriter<ControllerEvent> requestEventWriter = new WriterMock();
    private TableRequestHandler tableRequestHandler;

    private StatsProvider statsProvider = null;

    @Before
    public void setup() throws Exception {
        StreamMetrics.initialize();
        setupStores();
        CreateScopeStatus scopeCreationStatus = this.streamStore.createScope(SCOPE, null, executor).get();
        if (scopeCreationStatus.getStatus().equals(CreateScopeStatus.Status.SCOPE_EXISTS)
                || scopeCreationStatus.getStatus().equals(CreateScopeStatus.Status.SUCCESS)) {
            this.isScopeCreated = true;
        }

        segmentHelperMock = getSegmentHelper();
        EventHelper helper = new EventHelper(executor, "host", ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        helper.setRequestEventWriter(requestEventWriter);
        kvtMetadataTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                 "host", GrpcAuthHelper.getDisabledAuthHelper(), helper));
        this.tableRequestHandler = new TableRequestHandler(new CreateTableTask(this.kvtStore, this.kvtMetadataTasks, executor),
                                                            new DeleteTableTask(this.kvtStore, this.kvtMetadataTasks, executor),
                                                            this.kvtStore, executor);

        statsProvider = MetricsTestUtil.getInitializedStatsProvider();
        statsProvider.startWithoutExporting();
    }

    public abstract void setupStores() throws Exception;

    public abstract void cleanupStores() throws Exception;

    abstract SegmentHelper getSegmentHelper();

    @After
    public void tearDown() throws Exception {
        cleanupStores();
        StreamMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
        if (this.statsProvider != null) {
            statsProvider.close();
            statsProvider = null;
        }
    }

    @Test(timeout = 30000)
    public void testCreateKeyValueTable() throws ExecutionException, InterruptedException {
        Assert.assertTrue(isScopeCreated);
        long creationTime = System.currentTimeMillis();
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        CompletableFuture<Controller.CreateKeyValueTableStatus.Status> createOperationFuture
                = kvtMetadataTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime, 0L);

        assertTrue(Futures.await(processEvent((TableMetadataTasksTest.WriterMock) requestEventWriter)));
        assertEquals(CreateKeyValueTableStatus.Status.SUCCESS, createOperationFuture.join());
        assertTrue(MetricsTestUtil.getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_CREATE_TABLE_LATENCY) > 0);
        List<KVTSegmentRecord> segmentsList = kvtStore.getActiveSegments(SCOPE, kvtable1, null, executor).get();
        assertEquals(segmentsList.size(), kvtConfig.getPartitionCount());

        long storedCreationTime = kvtStore.getCreationTime(SCOPE, kvtable1, null, executor).get();
        assertEquals(storedCreationTime, creationTime);

        KeyValueTableConfiguration storedConfig = kvtStore.getConfiguration(SCOPE, kvtable1, null, executor).get();
        assertEquals(storedConfig.getPartitionCount(), kvtConfig.getPartitionCount());

        // check retry failures...
        EventHelper mockHelper = EventHelperMock.getFailingEventHelperMock();
        TableMetadataTasks kvtFailingMetaTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), mockHelper));

        AssertExtensions.assertFutureThrows("addIndexAndSubmitTask throws exception",
                kvtFailingMetaTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime, 0L),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);
    }

    @Test(timeout = 30000)
    public void testDeleteKeyValueTable() throws ExecutionException, InterruptedException {
        Assert.assertTrue(isScopeCreated);
        long creationTime = System.currentTimeMillis();
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        CompletableFuture<Controller.CreateKeyValueTableStatus.Status> createOperationFuture
                = kvtMetadataTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime, 0L);

        assertTrue(Futures.await(processEvent((TableMetadataTasksTest.WriterMock) requestEventWriter)));
        assertEquals(CreateKeyValueTableStatus.Status.SUCCESS, createOperationFuture.join());

        // delete KVTable
        CompletableFuture<DeleteKVTableStatus.Status> future = kvtMetadataTasks.deleteKeyValueTable(SCOPE, kvtable1, 0L);
        assertTrue(Futures.await(processEvent((TableMetadataTasksTest.WriterMock) requestEventWriter)));

        assertEquals(Controller.DeleteKVTableStatus.Status.SUCCESS, future.get());
        assertTrue(kvtMetadataTasks.isDeleted(SCOPE, kvtable1, null).join());
        assertFalse(kvtStore.checkTableExists(SCOPE, kvtable1, null, executor).join());
        assertFalse(kvtStore.isScopeSealed("testScope", null, executor).join());
        assertTrue(MetricsTestUtil.getTimerMillis(MetricsNames.CONTROLLER_EVENT_PROCESSOR_DELETE_TABLE_LATENCY) > 0);
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
    public void testWorkflowCompletionTimeout() throws Exception {
        // Create a new KVTable
        String tableName = "kvtable2";
        long creationTime = System.currentTimeMillis();
        KeyValueTableConfiguration kvtConfig = KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).secondaryKeyLength(4).build();
        CompletableFuture<Controller.CreateKeyValueTableStatus.Status> createOperationFuture
                = kvtMetadataTasks.createKeyValueTable(SCOPE, tableName, kvtConfig, creationTime, 0L);
        assertTrue(Futures.await(processEvent((TableMetadataTasksTest.WriterMock) requestEventWriter)));
        assertEquals(CreateKeyValueTableStatus.Status.SUCCESS, createOperationFuture.join());

        //Create KVTable times out
        EventHelper helper = new EventHelper(executor, "host", ((AbstractKVTableMetadataStore) kvtStore).getHostTaskIndex());
        helper.setCompletionTimeoutMillis(50L);
        EventStreamWriter<ControllerEvent> eventWriter = new WriterMock();
        helper.setRequestEventWriter(eventWriter);
        TableMetadataTasks kvtTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), helper));

        AssertExtensions.assertFutureThrows("create timedout",
                kvtTasks.createKeyValueTable(SCOPE, kvtable1, kvtConfig, creationTime, 0L),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);

        //Delete KVTable times out
        AssertExtensions.assertFutureThrows("delete timedout",
                kvtTasks.deleteKeyValueTable(SCOPE, tableName, 0L),
                e -> Exceptions.unwrap(e) instanceof TimeoutException);
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
        public CompletableFuture<Void> writeEvents(String routingKey, List<ControllerEvent> events) {
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
}