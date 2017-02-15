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
package com.emc.pravega.controller.task;

import com.emc.pravega.controller.server.rpc.v1.SegmentHelperMock;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.LockFailedException;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaggedResource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.TestTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Task test cases.
 */
@Slf4j
public class TaskTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
    private final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private final StreamMetadataStore streamStore =

            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor);

    private final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

    private final TaskMetadataStore taskMetadataStore;

    private final TestingServer zkServer;

    private final StreamMetadataTasks streamMetadataTasks;

    public TaskTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();

        SegmentHelperMock.init();

        CuratorFramework cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(cli), executor);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, HOSTNAME);
    }

    @Before
    public void prepareStreamStore() {

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(SCOPE, stream2, policy2);

        // region createStream
        streamStore.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis(), null, executor);
        streamStore.createStream(SCOPE, stream2, configuration2, System.currentTimeMillis(), null, executor);
        // endregion

        // region scaleSegments

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStore.scale(SCOPE, stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20, null, executor);

        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStore.scale(SCOPE, stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20, null, executor);
        // endregion
    }

    @After
    public void stopZKServer() throws IOException {
        zkServer.stop();
        zkServer.close();
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        try {
            streamMetadataTasks.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis(), null).join();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof StreamAlreadyExistsException);
        }

        CreateStreamStatus result = streamMetadataTasks.createStream(SCOPE, "dummy", configuration1,
                System.currentTimeMillis(), null).join();
        assertEquals(result, CreateStreamStatus.SUCCESS);
    }

    @Test
    public void testTaskSweeper() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";
        final String deadThreadId = UUID.randomUUID().toString();
        final String scope = SCOPE;
        final String stream = "streamSweeper";
        final StreamConfiguration configuration = new StreamConfigurationImpl(SCOPE, stream1, policy1);

        final Resource resource = new Resource(scope, stream);
        final long timestamp = System.currentTimeMillis();
        final TaskData taskData = new TaskData("createStream", "1.0", new Serializable[]{scope, stream, configuration, timestamp, null});

        for (int i = 0; i < 5; i++) {
            final TaggedResource taggedResource = new TaggedResource(UUID.randomUUID().toString(), resource);
            taskMetadataStore.putChild(deadHost, taggedResource).join();
        }
        final TaggedResource taggedResource = new TaggedResource(deadThreadId, resource);
        taskMetadataStore.putChild(deadHost, taggedResource).join();

        taskMetadataStore.lock(resource, taskData, deadHost, deadThreadId, null, null).join();

        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, HOSTNAME, streamMetadataTasks);
        taskSweeper.sweepOrphanedTasks(deadHost).get();

        Optional<TaskData> data = taskMetadataStore.getTask(resource, deadHost, deadThreadId).get();
        assertFalse(data.isPresent());

        Optional<TaggedResource> child = taskMetadataStore.getRandomChild(deadHost).get();
        assertFalse(child.isPresent());

        // ensure that the stream streamSweeper is created
        StreamConfiguration config = streamStore.getConfiguration(SCOPE, stream, null, executor).get();
        assertTrue(config.getName().equals(configuration.getName()));
        assertTrue(config.getScope().equals(configuration.getScope()));
        assertTrue(config.getScalingPolicy().equals(configuration.getScalingPolicy()));
    }

    @Test
    public void parallelTaskSweeperTest() throws InterruptedException, ExecutionException {
        final String deadHost = "deadHost";
        final String deadThreadId1 = UUID.randomUUID().toString();
        final String deadThreadId2 = UUID.randomUUID().toString();

        final String scope = SCOPE;
        final String stream1 = "parallelSweeper1";
        final String stream2 = "parallelSweeper2";

        final StreamConfiguration config1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration config2 = new StreamConfigurationImpl(SCOPE, stream2, policy1);

        final Resource resource1 = new Resource(scope, stream1);
        final long timestamp1 = System.currentTimeMillis();
        final TaskData taskData1 = new TaskData("createStream", "1.0", new Serializable[]{scope, stream1, config1, timestamp1, null});

        final Resource resource2 = new Resource(scope, stream2);
        final long timestamp2 = System.currentTimeMillis();
        final TaskData taskData2 = new TaskData("createStream", "1.0", new Serializable[]{scope, stream2, config2, timestamp2, null});

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

        final SweeperThread sweeperThread1 = new SweeperThread(HOSTNAME, taskMetadataStore, streamMetadataTasks,
                deadHost);
        final SweeperThread sweeperThread2 = new SweeperThread(HOSTNAME, taskMetadataStore, streamMetadataTasks,
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
        assertTrue(config.getName().equals(stream1));

        config = streamStore.getConfiguration(SCOPE, stream2, null, executor).get();
        assertTrue(config.getName().equals(stream2));

    }

    @Test
    public void testLocking() {
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

        public SweeperThread(String hostId, TaskMetadataStore taskMetadataStore,
                             StreamMetadataTasks streamMetadataTasks, String deadHostId) {
            this.result = new CompletableFuture<>();
            this.taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks);
            this.deadHostId = deadHostId;
        }

        @Override
        public void run() {
            taskSweeper.sweepOrphanedTasks(this.deadHostId)
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

