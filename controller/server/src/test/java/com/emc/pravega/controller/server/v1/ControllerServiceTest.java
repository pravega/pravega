/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.mocks.SegmentHelperMock;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Controller service implementation test.
 */
public class ControllerServiceTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor);

    private final ControllerService consumer;

    private final TestingServer zkServer;

    public ControllerServiceTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(zkClient), executor);
        final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelper, executor, "host");
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, segmentHelper, executor, "host");
        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                                         new SegmentHelper(), executor);
    }

    @Before
    public void prepareStreamStore() throws ExecutionException, InterruptedException {

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(SCOPE).streamName(stream2).scalingPolicy(policy2).build();

        // createScope
        streamStore.createScope(SCOPE).get();

        // region createStream
        streamStore.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis(), null, executor).get();
        streamStore.createStream(SCOPE, stream2, configuration2, System.currentTimeMillis(), null, executor).get();
        // endregion

        // region scaleSegments

        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        streamStore.scale(SCOPE, stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20, null, executor).get();

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        streamStore.scale(SCOPE, stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20, null, executor).get();
        // endregion
    }

    @After
    public void stopZKServer() throws IOException {
        zkServer.close();
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        List<Position> positions;

        positions = consumer.getPositions(SCOPE, stream1, 10, 3).get();
        assertEquals(2, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream1, 10, 1).get();
        assertEquals(1, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream2, 10, 3).get();
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());
        assertEquals(1, positions.get(2).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream2, 10, 2).get();
        assertEquals(2, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream1, 25, 3).get();
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());
        assertEquals(1, positions.get(2).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream1, 25, 1).get();
        assertEquals(1, positions.size());
        assertEquals(3, positions.get(0).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream2, 25, 3).get();
        assertEquals(3, positions.size());
        assertEquals(1, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());
        assertEquals(1, positions.get(2).getOwnedSegmentsCount());

        positions = consumer.getPositions(SCOPE, stream2, 25, 2).get();
        assertEquals(2, positions.size());
        assertEquals(2, positions.get(0).getOwnedSegmentsCount());
        assertEquals(1, positions.get(1).getOwnedSegmentsCount());
    }
}
