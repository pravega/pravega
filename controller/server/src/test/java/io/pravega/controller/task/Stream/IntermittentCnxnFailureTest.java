/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.DataNotFoundException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;
import io.pravega.controller.util.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class IntermittentCnxnFailureTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService controllerService;

    private CuratorFramework zkClient;
    private TestingServer zkServer;

    private StreamMetadataStore streamStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private TimeoutService timeoutService;

    private SegmentHelper segmentHelperMock;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        segmentHelperMock = spy(new SegmentHelper());

        doReturn(Controller.NodeUri.newBuilder().setEndpoint("localhost").setPort(Config.SERVICE_PORT).build()).when(segmentHelperMock).getSegmentUri(
                anyString(), anyString(), anyInt(), any());

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelperMock,
                executor, "host", connectionFactory);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStore, hostStore, taskMetadataStore, segmentHelperMock, executor, "host", connectionFactory);
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, timeoutService, segmentHelperMock, executor, null);

        controllerService.createScope(SCOPE).get();
    }

    @After
    public void tearDown() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        executor.shutdown();
    }

    @Test
    public void createStreamTest() throws Exception {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();

        // start stream creation in background/asynchronously.
        // the connection to server will fail and should be retried
        controllerService.createStream(configuration1, System.currentTimeMillis());

        // Stream should not have been created and while trying to access any stream metadata
        // we should get illegalStateException
        try {
            Retry.withExpBackoff(10, 10, 4)
                    .retryingOn(DataNotFoundException.class)
                    .throwingOn(IllegalStateException.class)
                    .run(() -> {
                        FutureHelpers.getAndHandleExceptions(streamStore.getConfiguration(SCOPE, stream1, null, executor),
                                CompletionException::new);
                        return null;
                    });
        } catch (CompletionException ex) {
            Assert.assertEquals(ExceptionHelpers.getRealException(ex).getMessage(), "stream state unknown");
            assertEquals(ExceptionHelpers.getRealException(ex).getClass(), IllegalStateException.class);
        }

        // Mock createSegment to return success.
        doReturn(CompletableFuture.completedFuture(true)).when(segmentHelperMock).createSegment(
                anyString(), anyString(), anyInt(), any(), any(), any());

        AtomicBoolean result = new AtomicBoolean(false);
        Retry.withExpBackoff(10, 10, 4)
                .retryingOn(IllegalStateException.class)
                .throwingOn(RuntimeException.class)
                .run(() -> {
                    FutureHelpers.getAndHandleExceptions(
                            streamStore.getConfiguration(SCOPE, stream1, null, executor)
                                    .thenAccept(configuration -> result.set(configuration.equals(configuration1))),
                            CompletionException::new);
                    return null;
                });

        assertTrue(result.get());
    }
}
