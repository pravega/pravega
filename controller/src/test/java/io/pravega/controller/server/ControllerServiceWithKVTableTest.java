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
package io.pravega.controller.server;

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.mockito.Mockito.spy;

/**
 * Tests for KeyValueTables API in ControllerService
 */
@Slf4j
public abstract class ControllerServiceWithKVTableTest {
    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource();
    private static final String SCOPE = "scope";

    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected SegmentHelper segmentHelperMock;

    private ControllerService consumer;
    private StreamMetadataTasks streamMetadataTasks;

    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private ConnectionFactory connectionFactory;
    private StreamMetadataStore streamStore;
    private RequestTracker requestTracker = new RequestTracker(true);

    private KVTableMetadataStore kvtStore;
    private TableMetadataTasks kvtMetadataTasks;

    @Before
    public void setup() {

        segmentHelperMock = SegmentHelperMock.getSegmentHelperMockForTables(executor);
        streamStore = spy(getStore());
        kvtStore = spy(getKVTStore());
        BucketStore bucketStore = StreamStoreFactory.createZKBucketStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                  .controllerURI(URI.create("tcp://localhost"))
                                                                  .build());
        GrpcAuthHelper disabledAuthHelper = GrpcAuthHelper.getDisabledAuthHelper();
        StreamMetrics.initialize();
        TransactionMetrics.initialize();
        EventHelper helperMock = EventHelperMock.getEventHelperMock(executor, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", disabledAuthHelper, helperMock);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelperMock, executor,
                "host", disabledAuthHelper);
        kvtMetadataTasks = spy(new TableMetadataTasks(kvtStore, segmentHelperMock, executor, executor,
                "host", GrpcAuthHelper.getDisabledAuthHelper(), helperMock));
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
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtMetadataTasks, executor),
                executor);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, executor));

        consumer = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks, 
                streamTransactionMetadataTasks, segmentHelperMock, executor, null, requestTracker);
    }

    abstract StreamMetadataStore getStore();

    abstract KVTableMetadataStore getKVTStore();

    @After
    public void teardown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        kvtMetadataTasks.close();
        kvtStore.close();
        connectionFactory.close();
        StreamMetrics.reset();
        TransactionMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 5000)
    public void createKeyValueTableTest() {
        assertThrows(IllegalArgumentException.class, () -> consumer.createKeyValueTable(SCOPE, "kvtzero", 
                KeyValueTableConfiguration.builder().partitionCount(0).build(), System.currentTimeMillis(), 0L).join());
    }


}
