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
package io.pravega.controller.task;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaggedResource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.TestTasks;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Task test cases.
 */
@Slf4j
public abstract class TaskTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";

    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
    protected CuratorFramework cli;

    private final String stream1 = "stream1";
    private final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
    private final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

    private StreamMetadataStore streamStore;

    private final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

    private TaskMetadataStore taskMetadataStore;

    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;
    private SegmentHelper segmentHelperMock;

    abstract StreamMetadataStore getStream();

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        streamStore = getStream();
        taskMetadataStore = TaskStoreFactory.createZKStore(cli, executor);

        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(), 
                taskMetadataStore, segmentHelperMock,
                executor, HOSTNAME, GrpcAuthHelper.getDisabledAuthHelper());

        final String stream2 = "stream2";
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scalingPolicy(policy2).build();

        // region createStream
        streamStore.createScope(SCOPE, null, executor).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).join();
        streamStore.createStream(SCOPE, stream2, configuration2, start, null, executor).join();
        streamStore.setState(SCOPE, stream2, State.ACTIVE, null, executor).join();
        // endregion

        // region scaleSegments

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);
        VersionedMetadata<EpochTransitionRecord> versioned = streamStore.submitScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), start + 20, null, null, executor).get();
        EpochTransitionRecord response = versioned.getObject();
        Map<Long, Map.Entry<Double, Double>> segmentsCreated = response.getNewSegmentsWithRange();
        VersionedMetadata<State> state = streamStore.getVersionedState(SCOPE, stream1, null, executor).join();
        state = streamStore.updateVersionedState(SCOPE, stream1, State.SCALING, state, null, executor).get();
        versioned = streamStore.startScale(SCOPE, stream1, false, versioned, state, null, executor).join();
        streamStore.scaleCreateNewEpochs(SCOPE, stream1, versioned, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream1, sealedSegments.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned, null, executor).get();
        streamStore.completeScale(SCOPE, stream1, versioned, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();

        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments1 = Arrays.asList(0L, 1L, 2L);
        versioned = streamStore.submitScale(SCOPE, stream2, sealedSegments1, Arrays.asList(segment3, segment4, segment5), start + 20, null, null, executor).get();
        response = versioned.getObject();
        segmentsCreated = response.getNewSegmentsWithRange();
        state = streamStore.getVersionedState(SCOPE, stream2, null, executor).join();
        state = streamStore.updateVersionedState(SCOPE, stream2, State.SCALING, state, null, executor).get();
        versioned = streamStore.startScale(SCOPE, stream2, false, versioned, state, null, executor).join();
        streamStore.scaleCreateNewEpochs(SCOPE, stream2, versioned, null, executor).get();
        streamStore.scaleSegmentsSealed(SCOPE, stream2, sealedSegments1.stream().collect(Collectors.toMap(x -> x, x -> 0L)), versioned,
                null, executor).get();
        streamStore.completeScale(SCOPE, stream2, versioned, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).get();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        cli.close();
        zkServer.stop();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testMethods() throws InterruptedException, ExecutionException {
        CreateStreamStatus.Status status = streamMetadataTasks.createStream(SCOPE, stream1, configuration1, 
                System.currentTimeMillis(), 0L).join();
        assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, status);
        streamStore.createScope(SCOPE, null, executor);
        CreateStreamStatus.Status result = streamMetadataTasks.createStream(SCOPE, "dummy", configuration1,
                System.currentTimeMillis(), 0L).join();
        assertEquals(result, CreateStreamStatus.Status.SUCCESS);
    }

    @Test(timeout = 30000)
    public void testTaskSweeper() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";
        final String deadThreadId = UUID.randomUUID().toString();
        final String scope = SCOPE;
        final String stream = "streamSweeper";
        final StreamConfiguration configuration = StreamConfiguration.builder().scalingPolicy(policy1).build();

        final Resource resource = new Resource(scope, stream);
        final long timestamp = System.currentTimeMillis();
        final TaskData taskData = new TaskData("createStream", "1.0",
                new Serializable[]{scope, stream, configuration, timestamp, 0L});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource);
            taskMetadataStore.putChild(deadHost, taggedResource).join();
        }
        final TaggedResource taggedResource = new TaggedResource(deadThreadId, resource);
        taskMetadataStore.putChild(deadHost, taggedResource).join();

        taskMetadataStore.lock(resource, taskData, deadHost, deadThreadId, null, null).join();

        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, streamMetadataTasks);
        taskSweeper.handleFailedProcess(deadHost).get();

        Optional<TaskData> data = taskMetadataStore.getTask(resource, deadHost, deadThreadId).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());

        // ensure that the stream streamSweeper is created
        StreamConfiguration config = streamStore.getConfiguration(SCOPE, stream, null, executor).get();
        assertTrue(config.getScalingPolicy().equals(configuration.getScalingPolicy()));
    }

    @Test(timeout = 10000)
    public void testZkLock() {
        final String oldHost = "oldHost";
        final String newHost = "newHost";
        final String newHost1 = "newHost1";
        final String oldThreadId = UUID.randomUUID().toString();
        final String newThreadId = UUID.randomUUID().toString();
        final String scope = SCOPE;
        final String stream = "testStream";
        final StreamConfiguration config1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final Resource resource1 = new Resource(scope, stream1);
        final long timestamp1 = System.currentTimeMillis();
        final TaskData taskData1 = new TaskData("createStream", "1.0",
                new Serializable[]{scope, stream, config1, timestamp1, 0L});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource1);
            taskMetadataStore.putChild(oldHost, taggedResource).join();
        }
        taskMetadataStore.lock(resource1, taskData1, oldHost, oldThreadId, null, null).join();
        taskMetadataStore.lock(resource1, taskData1, newHost, newThreadId, newHost1, oldThreadId).join();
        Optional<TaskData> testData = Optional.of(new TaskData("createStream", "1.0",
                new Serializable[]{scope, stream, config1, timestamp1, 0L}));
        Assert.assertEquals(testData, taskMetadataStore.getTask(resource1, newHost1, oldThreadId).join());
    }

    @Test(timeout = 10000)
    public void testStreamTaskSweeping() throws Exception {
        final String stream = "testPartialCreationStream";
        final String deadHost = "deadHost";
        final int initialSegments = 2;
        final ScalingPolicy policy1 = ScalingPolicy.fixed(initialSegments);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final ArrayList<Long> sealSegments = new ArrayList<>();
        sealSegments.add(0L);
        final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.25, 0.5));

        // Create objects.
        @Cleanup
        StreamMetadataTasks mockStreamTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(),
                taskMetadataStore, segmentHelperMock,
                executor, deadHost, GrpcAuthHelper.getDisabledAuthHelper());
        mockStreamTasks.setCreateIndexOnlyMode();
        TaskSweeper sweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, executor, streamMetadataTasks);

        // Create stream test.
        // this will create 2 new streams -> stream + mark stream
        completePartialTask(mockStreamTasks.createStream(SCOPE, stream, configuration1, System.currentTimeMillis(), 0L),
                deadHost, sweeper);
        Assert.assertEquals(initialSegments, streamStore.getActiveSegments(SCOPE, stream, null, executor).join().size());

        Map<String, StreamConfiguration> streams = streamStore.listStreamsInScope(SCOPE, null, executor).join();
        assertEquals(4, streams.size());
        assertEquals(configuration1, streams.get(stream));
    }

    private <T> void completePartialTask(CompletableFuture<T> task, String hostId, TaskSweeper sweeper) {
        AssertExtensions.assertFutureThrows("IllegalStateException expected", task, e -> e instanceof IllegalStateException);
        sweeper.handleFailedProcess(hostId).join();
        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(hostId).join();
        assertFalse(child.isPresent());
    }

    @Test(timeout = 30000)
    public void parallelTaskSweeperTest() throws InterruptedException, ExecutionException {
        final String deadHost = "deadHost";
        final String deadThreadId1 = UUID.randomUUID().toString();
        final String deadThreadId2 = UUID.randomUUID().toString();

        final String scope = SCOPE;
        final String stream1 = "parallelSweeper1";
        final String stream2 = "parallelSweeper2";

        final StreamConfiguration config1 = StreamConfiguration.builder().scalingPolicy(policy1).build();
        final StreamConfiguration config2 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        final Resource resource1 = new Resource(scope, stream1);
        final long timestamp1 = System.currentTimeMillis();
        final TaskData taskData1 = new TaskData("createStream", "1.0", 
                new Serializable[]{scope, stream1, config1, timestamp1, 0L});

        final Resource resource2 = new Resource(scope, stream2);
        final long timestamp2 = System.currentTimeMillis();
        final TaskData taskData2 = new TaskData("createStream", "1.0", 
                new Serializable[]{scope, stream2, config2, timestamp2, 0L});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource1);
            taskMetadataStore.putChild(deadHost, taggedResource).join();
        }
        final TaggedResource taggedResource1 = new TaggedResource(deadThreadId1, resource1);
        taskMetadataStore.putChild(deadHost, taggedResource1).join();

        final TaggedResource taggedResource2 = new TaggedResource(deadThreadId2, resource2);
        taskMetadataStore.putChild(deadHost, taggedResource2).join();

        taskMetadataStore.lock(resource1, taskData1, deadHost, deadThreadId1, null, null).join();
        taskMetadataStore.lock(resource2, taskData2, deadHost, deadThreadId2, null, null).join();

        final SweeperThread sweeperThread1 = new SweeperThread(HOSTNAME, executor, taskMetadataStore, streamMetadataTasks,
                deadHost);
        final SweeperThread sweeperThread2 = new SweeperThread(HOSTNAME, executor, taskMetadataStore, streamMetadataTasks,
                deadHost);

        sweeperThread1.start();
        sweeperThread2.start();

        sweeperThread1.getResult().join();
        sweeperThread2.getResult().join();

        Optional<TaskData> data = taskMetadataStore.getTask(resource1, deadHost, deadThreadId1).get();
        assertFalse(data.isPresent());

        data = taskMetadataStore.getTask(resource2, deadHost, deadThreadId2).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());

        // ensure that the stream streamSweeper is created
        StreamConfiguration config = streamStore.getConfiguration(SCOPE, stream1, null, executor).get();
        assertEquals(config1, config);

        config = streamStore.getConfiguration(SCOPE, stream2, null, executor).get();
        assertEquals(config2, config);
    }

    @Test(timeout = 30000)
    public void testLocking() throws Exception {
        @Cleanup
        TestTasks testTasks = new TestTasks(taskMetadataStore, executor, HOSTNAME);

        CompletableFuture<Void> first = testTasks.testStreamLock(SCOPE, stream1);
        CompletableFuture<Void> second = testTasks.testStreamLock(SCOPE, stream1);
        try {
            first.getNow(null);
            second.getNow(null);
        } catch (CompletionException ce) {
            assertTrue(ce.getCause() instanceof LockFailedException);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    static class SweeperThread extends Thread {

        private final CompletableFuture<Void> result;
        private final String deadHostId;
        private final TaskSweeper taskSweeper;
        private final String hostId;

        public SweeperThread(String hostId, ScheduledExecutorService executor, TaskMetadataStore taskMetadataStore,
                             StreamMetadataTasks streamMetadataTasks, String deadHostId) {
            this.result = new CompletableFuture<>();
            this.taskSweeper = new TaskSweeper(taskMetadataStore, hostId, executor, streamMetadataTasks);
            this.deadHostId = deadHostId;
            this.hostId = hostId;
        }

        @Override
        public void run() {
            taskSweeper.sweepFailedProcesses(() -> Collections.singleton(hostId))
                    .whenComplete((value, e) -> {
                        if (e != null) {
                            result.completeExceptionally(e);
                        } else {
                            result.complete(value);
                        }
                    });
        }
    }
}

