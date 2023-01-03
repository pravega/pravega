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

import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.util.Retry;
import io.pravega.controller.PravegaZkCuratorResource;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.test.common.SerializedClassRunner;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(SerializedClassRunner.class)
public class IntermittentCnxnFailureTest {

    @ClassRule
    public static final PravegaZkCuratorResource PRAVEGA_ZK_CURATOR_RESOURCE = new PravegaZkCuratorResource();
    private static final String SCOPE = "scope";

    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");

    private ControllerService controllerService;

    private StreamMetadataStore streamStore;
    private BucketStore bucketStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private SegmentHelper segmentHelperMock;
    private RequestTracker requestTracker = new RequestTracker(true);
    private ConnectionPool connectionPool;
    @Mock
    private KVTableMetadataStore kvtStore;
    @Mock
    private TableMetadataTasks kvtMetadataTasks;
    private StatsProvider statsProvider = null;

    @Before
    public void setup() throws Exception {
        MetricsConfig metricsConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(60));

        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();

        streamStore = spy(StreamStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor));
        bucketStore = StreamStoreFactory.createZKBucketStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(PRAVEGA_ZK_CURATOR_RESOURCE.client, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), new SocketConnectionFactoryImpl(ClientConfig.builder().build()));

        segmentHelperMock = spy(new SegmentHelper(connectionPool, hostStore, executor));

        doReturn(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(Config.SERVICE_PORT).build()).when(segmentHelperMock).getSegmentUri(
                anyString(), anyString(), anyInt());

        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelperMock,
                executor, "host", GrpcAuthHelper.getDisabledAuthHelper());

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, segmentHelperMock, executor, "host", GrpcAuthHelper.getDisabledAuthHelper());

        controllerService = new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelperMock, executor, null, requestTracker);
        StreamMetrics.initialize();
        controllerService.createScope(SCOPE, 0L).get();
    }

    @After
    public void tearDown() throws Exception {
        statsProvider.close();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        streamStore.close();
        connectionPool.close();
        StreamMetrics.reset();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void failedScopeOperationsTest() throws ExecutionException, InterruptedException {
        final String testScope = "testScope2";

        // Simulate a stream store failure when creating a scope and verify that the failed metrics are updated.
        final Controller.CreateScopeStatus createScopeStatus = Controller.CreateScopeStatus.newBuilder()
                .setStatus(Controller.CreateScopeStatus.Status.FAILURE).build();
        doAnswer(x -> CompletableFuture.completedFuture(createScopeStatus)).when(streamStore).createScope(anyString(), any(), any());
        assertEquals(createScopeStatus, controllerService.createScope(testScope, 0L).get());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.CREATE_SCOPE_FAILED).count());

        // Simulate a stream store failure when deleting a scope and verify that the failed metrics are updated.
        final Controller.DeleteScopeStatus deleteScopeStatus = Controller.DeleteScopeStatus.newBuilder()
                .setStatus(Controller.DeleteScopeStatus.Status.FAILURE).build();
        doAnswer(x -> CompletableFuture.completedFuture(deleteScopeStatus)).when(streamStore).deleteScope(anyString(), any(), any());

        assertEquals(deleteScopeStatus, controllerService.deleteScope(testScope, 0L).get());
        assertEquals(1, (long) MetricRegistryUtils.getCounter(MetricsNames.DELETE_SCOPE_FAILED).count());
    }

    @Test(timeout = 30000)
    public void createStreamTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // start stream creation in background/asynchronously.
        // the connection to server will fail and should be retried
        controllerService.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis(), 0L);

        // Stream should not have been created and while trying to access any stream metadata
        // we should get illegalStateException
        try {
            Retry.withExpBackoff(10, 10, 4)
                    .retryingOn(StoreException.DataNotFoundException.class)
                    .throwingOn(IllegalStateException.class)
                    .run(() -> {
                        Futures.getAndHandleExceptions(streamStore.getConfiguration(SCOPE, stream1, null, executor),
                                CompletionException::new);
                        return null;
                    });
        } catch (CompletionException ex) {
            Assert.assertEquals(Exceptions.unwrap(ex).getMessage(), "stream state unknown");
            assertEquals(Exceptions.unwrap(ex).getClass(), IllegalStateException.class);
        }

        // Mock createSegment to return success.
        doReturn(CompletableFuture.completedFuture(true)).when(segmentHelperMock).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), anyLong(), anyLong());

        AtomicBoolean result = new AtomicBoolean(false);
        Retry.withExpBackoff(10, 10, 4)
                .retryingOn(IllegalStateException.class)
                .throwingOn(RuntimeException.class)
                .run(() -> {
                    Futures.getAndHandleExceptions(
                            streamStore.getConfiguration(SCOPE, stream1, null, executor)
                                    .thenAccept(configuration -> result.set(configuration.equals(configuration1))),
                            CompletionException::new);
                    return null;
                });

        assertTrue(result.get());
    }
}
