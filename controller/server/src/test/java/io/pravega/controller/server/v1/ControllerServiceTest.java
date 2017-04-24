/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.v1;

import io.pravega.testcommon.TestingServerStarter;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.timeout.TimeoutService;
import io.pravega.controller.timeout.TimeoutServiceConfig;
import io.pravega.controller.timeout.TimerWheelTimeoutService;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.ModelHelper;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private final StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(executor);

    private final TimeoutService timeoutService;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final ConnectionFactoryImpl connectionFactory;
    private final ControllerService consumer;

    private final CuratorFramework zkClient;
    private final TestingServer zkServer;

    private long startTs;

    public ControllerServiceTest() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createZKStore(zkClient, executor);
        final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        connectionFactory = new ConnectionFactoryImpl(false);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore,
                taskMetadataStore, segmentHelper, executor, "host", connectionFactory);
        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, segmentHelper, executor, "host", connectionFactory);
        timeoutService = new TimerWheelTimeoutService(streamTransactionMetadataTasks,
                TimeoutServiceConfig.defaultConfig());

        consumer = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks,
                timeoutService, new SegmentHelper(), executor, null);
    }

    @Before
    public void setup() throws ExecutionException, InterruptedException {

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(SCOPE).streamName(stream2).scalingPolicy(policy2).build();

        // createScope
        streamStore.createScope(SCOPE).get();

        // region createStream
        startTs = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, startTs, null, executor).get();
        streamStore.createStream(SCOPE, stream2, configuration2, startTs, null, executor).get();
        // endregion

        // region scaleSegments

        SimpleEntry<Double, Double> segment1 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment2 = new SimpleEntry<>(0.75, 1.0);
        List<Integer> sealedSegments = Collections.singletonList(1);

        List<Segment> segmentCreated = streamStore.startScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), startTs + 20, null, executor).get();
        streamStore.scaleNewSegmentsCreated(SCOPE, stream1, sealedSegments, segmentCreated, startTs + 20, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream1, sealedSegments, segmentCreated, startTs + 20, null, executor).get();

        SimpleEntry<Double, Double> segment3 = new SimpleEntry<>(0.0, 0.5);
        SimpleEntry<Double, Double> segment4 = new SimpleEntry<>(0.5, 0.75);
        SimpleEntry<Double, Double> segment5 = new SimpleEntry<>(0.75, 1.0);
        sealedSegments = Arrays.asList(0, 1, 2);
        segmentCreated = streamStore.startScale(SCOPE, stream2, sealedSegments, Arrays.asList(segment3, segment4, segment5), startTs + 20, null, executor).get();
        streamStore.scaleNewSegmentsCreated(SCOPE, stream2, sealedSegments, segmentCreated, startTs + 20, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream2, sealedSegments, segmentCreated, startTs + 20, null, executor).get();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        timeoutService.stopAsync();
        timeoutService.awaitTerminated();
        streamTransactionMetadataTasks.close();
        streamMetadataTasks.close();
        connectionFactory.close();
        zkClient.close();
        zkServer.close();
        executor.shutdown();
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        Map<SegmentId, Long> segments;

        segments = consumer.getSegmentsAtTime(SCOPE, stream1, startTs + 10).get();
        assertEquals(2, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 1)));

        segments = consumer.getSegmentsAtTime(SCOPE, stream2, startTs + 10).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 1)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 2)));

        segments = consumer.getSegmentsAtTime(SCOPE, stream1, startTs + 25).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 0)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 2)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream1, 3)));

        segments = consumer.getSegmentsAtTime(SCOPE, stream2, startTs + 25).get();
        assertEquals(3, segments.size());
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 3)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 4)));
        assertEquals(Long.valueOf(0), segments.get(ModelHelper.createSegmentId(SCOPE, stream2, 5)));
    }
}
