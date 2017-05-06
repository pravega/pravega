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
package io.pravega.controller.server.eventProcessor;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.shared.controller.event.ScaleEvent;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;

public class ScaleRequestHandlerTest {
    private final String scope = "scope";
    private final String stream = "stream";
    StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
            ScalingPolicy.byEventRate(0, 2, 3)).build();

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;

    private CuratorFramework zkClient;

    @Before
    public void setup() throws Exception {
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

        streamStore = StreamStoreFactory.createZKStore(zkClient, executor);

        taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);

        hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executor, hostId, connectionFactory);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore,
                segmentHelper, executor, hostId, connectionFactory);

        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope);
        streamMetadataTasks.createStream(scope, stream, config, createTimestamp).get();
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamTransactionMetadataTasks.close();
        zkClient.close();
        zkServer.close();
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        ScaleRequestHandler requestHandler = new ScaleRequestHandler(streamMetadataTasks, streamStore, executor);
        ScaleEvent request = new ScaleEvent(scope, stream, 2, ScaleEvent.UP, System.currentTimeMillis(), 2, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        List<Segment> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 2));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new ScaleEvent(scope, stream, 4, ScaleEvent.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new ScaleEvent(scope, stream, 3, ScaleEvent.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);
    }
}
