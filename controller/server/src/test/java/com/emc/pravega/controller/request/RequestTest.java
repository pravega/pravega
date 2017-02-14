/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.request;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.requesthandler.ScaleRequestHandler;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelperMock;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.ZKHostStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class RequestTest {
    private final String scope = "scope";
    private final String stream = "stream";

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(100);
    private StreamMetadataStore streamStore;
    private TaskMetadataStore taskMetadataStore;
    private HostControllerStore hostStore;
    private StreamMetadataTasks streamMetadataTasks;

    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    private TestingServer zkServer;

    private CuratorFramework zkClient;

    @Before
    public void createStream() throws Exception {
        SegmentHelperMock.init();

        zkServer = new TestingServer();
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

        streamStore = new ZKStreamMetadataStore(zkClient, executor);

        taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(zkClient), executor);

        hostStore = spy(new ZKHostStore(zkClient, "test"));
        doReturn(new Host("", 0)).when(hostStore).getHostForSegment(anyString(), anyString(), anyInt());

        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createStream(scope, stream, new StreamConfigurationImpl(scope, stream, new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, 0, 2, 3)), System.currentTimeMillis(), null, executor).get();
    }

    @Test
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        ScaleRequestHandler requestHandler = new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor);
        ScaleRequest request = new ScaleRequest(scope, stream, 2, ScaleRequest.UP, System.currentTimeMillis(), 2, false);

        assert FutureHelpers.await(requestHandler.process(request));
        List<Segment> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assert activeSegments.stream().noneMatch(z -> z.getNumber() == 2);
        assert activeSegments.stream().anyMatch(z -> z.getNumber() == 3);
        assert activeSegments.stream().anyMatch(z -> z.getNumber() == 4);
        assert activeSegments.size() == 4;

        request = new ScaleRequest(scope, stream, 4, ScaleRequest.DOWN, System.currentTimeMillis(), 0, false);

        assert FutureHelpers.await(requestHandler.process(request));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assert activeSegments.stream().anyMatch(z -> z.getNumber() == 4);
        assert activeSegments.size() == 4;

        request = new ScaleRequest(scope, stream, 3, ScaleRequest.DOWN, System.currentTimeMillis(), 0, false);

        assert FutureHelpers.await(requestHandler.process(request));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assert activeSegments.stream().noneMatch(z -> z.getNumber() == 3);
        assert activeSegments.stream().noneMatch(z -> z.getNumber() == 4);
        assert activeSegments.stream().anyMatch(z -> z.getNumber() == 5);
        assert activeSegments.size() == 3;
    }
}
