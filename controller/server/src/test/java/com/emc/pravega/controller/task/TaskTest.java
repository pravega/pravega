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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.InMemoryHostControllerStoreConfig;
import com.emc.pravega.controller.store.stream.StoreConfiguration;
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
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;

/**
 * Task test cases
 */
@Slf4j
public class TaskTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
    private final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);

    private final StreamMetadataStore streamStore =
            StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null);

    private final Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();

    private final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory,
            new InMemoryHostControllerStoreConfig(hostContainerMap));

    private final TaskMetadataStore taskMetadataStore;

    private final TestingServer zkServer;

    private final StreamMetadataTasks streamMetadataTasks;

    public TaskTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        StoreConfiguration config = new StoreConfiguration(zkServer.getConnectString());
        taskMetadataStore = TaskStoreFactory.createStore(TaskStoreFactory.StoreType.Zookeeper, config);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, HOSTNAME);
    }

    @Before
    public void prepareStreamStore() {

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(SCOPE, stream2, policy2);

        // region createStream
        streamStore.createStream(stream1, configuration1);
        streamStore.createStream(stream2, configuration2);
        // endregion

        // region scaleSegments

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStore.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);

        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.5);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStore.scale(stream2, Arrays.asList(0, 1, 2), Arrays.asList(segment3, segment4, segment5), 20);
        // endregion
    }

    @Before
    public void prepareHostStore() {
        Host host = new Host("localhost", 9090);
        hostContainerMap.put(host, new HashSet<>(Collections.singletonList(0)));
    }

    @After
    public void stopZKServer() throws IOException {
        zkServer.stop();
        zkServer.close();
    }

    @Test
    public void testMethods() throws InterruptedException, ExecutionException {
        try {
            streamMetadataTasks.createStream(SCOPE, stream1, configuration1, System.currentTimeMillis()).join();
        } catch (CompletionException e) {
            assertTrue(e.getCause() instanceof StreamAlreadyExistsException);
        }

        CreateStreamStatus result = streamMetadataTasks.createStream(SCOPE, "dummy", configuration1, System.currentTimeMillis()).join();
        assertEquals(result, CreateStreamStatus.SUCCESS);
    }

    @Test
    public void testTaskSweeper() throws ExecutionException, InterruptedException {
        final String deadHost = "deadHost";
        final String deadThreadId = UUID.randomUUID().toString();
        final String scope = SCOPE;
        final String stream = "streamSweeper";
        final TaskData taskData = new TaskData();
        final Resource resource = new Resource(scope, stream);
        final long timestamp = System.currentTimeMillis();

        taskData.setMethodName("createStream");
        taskData.setMethodVersion("1.0");
        taskData.setParameters(new Serializable[]{scope, stream, configuration1, timestamp});

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
        StreamConfiguration config = streamStore.getConfiguration(stream).get();
        assertTrue(config.getName().equals(configuration1.getName()));
        assertTrue(config.getScope().equals(configuration1.getScope()));
        assertTrue(config.getScalingingPolicy().equals(configuration1.getScalingingPolicy()));
    }

    @Test
    public void testLocking() {

        TestTasks testTasks = new TestTasks(taskMetadataStore, HOSTNAME);

        LockingTask first = new LockingTask(testTasks, SCOPE, stream1);
        LockingTask second = new LockingTask(testTasks, SCOPE, stream1);

        first.start();
        second.start();

        try {
            first.result.join();
            second.result.join();
        } catch (CompletionException ce) {
            assertTrue(ce.getCause() instanceof LockFailedException);
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    class LockingTask extends Thread {

        private final TestTasks testTasks;
        private final String scope;
        private final String stream;
        private CompletableFuture<Void> result = new CompletableFuture<>();

        LockingTask(TestTasks testTasks, String scope, String stream) {
            this.testTasks = testTasks;
            this.scope = scope;
            this.stream = stream;
        }

        @Override
        public void run() {
            testTasks.testStreamLock(scope, stream)
            .whenComplete((value, ex) -> {
                if (ex != null) {
                    this.result.completeExceptionally(ex);
                } else {
                    this.result.complete(value);
                }
            });
        }
    }
}
