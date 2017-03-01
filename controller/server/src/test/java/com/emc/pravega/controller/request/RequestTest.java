/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.request;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.requesthandler.ScaleRequestHandler;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
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

import static org.junit.Assert.assertTrue;

public class RequestTest {
    private final String scope = "scope";
    private final String stream = "stream";
    StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC, 0, 2, 3)).build();

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

        hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, segmentHelper,
                executor, hostId);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore,
                segmentHelper, executor, hostId);

        long createTimestamp = System.currentTimeMillis();

        // add a host in zk
        // mock pravega
        // create a stream
        streamStore.createScope(scope);
        streamMetadataTasks.createStream(scope, stream, config, createTimestamp).get();
    }

    @Test(timeout = 10000)
    public void testScaleRequest() throws ExecutionException, InterruptedException {
        ScaleRequestHandler requestHandler = new ScaleRequestHandler(streamMetadataTasks, streamStore, streamTransactionMetadataTasks, executor);
        ScaleRequest request = new ScaleRequest(scope, stream, 2, ScaleRequest.UP, System.currentTimeMillis(), 2, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        List<Segment> activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 2));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new ScaleRequest(scope, stream, 4, ScaleRequest.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.size() == 4);

        request = new ScaleRequest(scope, stream, 3, ScaleRequest.DOWN, System.currentTimeMillis(), 0, false);

        assertTrue(FutureHelpers.await(requestHandler.process(request)));
        activeSegments = streamStore.getActiveSegments(scope, stream, null, executor).get();

        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 3));
        assertTrue(activeSegments.stream().noneMatch(z -> z.getNumber() == 4));
        assertTrue(activeSegments.stream().anyMatch(z -> z.getNumber() == 5));
        assertTrue(activeSegments.size() == 3);
    }
}
