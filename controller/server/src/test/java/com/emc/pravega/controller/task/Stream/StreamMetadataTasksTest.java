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
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class StreamMetadataTasksTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private ControllerService consumer;

    private TestingServer zkServer;

    private StreamMetadataStore streamStorePartialMock;
    private StreamMetadataTasks streamMetadataTasksPartialMock;

    @Before
    public void initialize() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        StreamMetadataStore streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType
                .InMemory, executor);
        streamStorePartialMock = spy(streamStore); //create a partial mock.
        doReturn(CompletableFuture.completedFuture(false)).when(streamStorePartialMock).isTransactionOngoing(
                anyString(), anyString()); //mock only isTransactionOngoing call.

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(new ZKStoreClient(zkClient), executor);
        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStorePartialMock, hostStore,
                taskMetadataStore,
                executor, "host");
        streamMetadataTasksPartialMock = spy(streamMetadataTasks); //create a partial mock
        doReturn(CompletableFuture.completedFuture(true)).when(streamMetadataTasksPartialMock).notifySealedSegment(
                anyString(), anyString(), anyInt()); //mock only the actual calls to Pravega.

        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(
                streamStorePartialMock, hostStore, taskMetadataStore, executor, "host");
        consumer = new ControllerService(streamStorePartialMock, hostStore, streamMetadataTasksPartialMock,
                streamTransactionMetadataTasks);

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);

        streamStorePartialMock.createStream(stream1, configuration1, System.currentTimeMillis());

        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        streamStorePartialMock.scale(stream1, Collections.singletonList(1), Arrays.asList(segment1, segment2), 20);
    }

    @After
    public void tearDown() throws IOException {
        zkServer.close();
    }

    @Test
    public void sealStreamTest() throws Exception {
        assertNotEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());

        //seal a stream.
        UpdateStreamStatus sealOperationResult = streamMetadataTasksPartialMock.sealStreamBody(SCOPE, stream1).get();
        assertEquals(UpdateStreamStatus.SUCCESS, sealOperationResult);

        //a sealed stream should have zero active/current segments
        assertEquals(0, consumer.getCurrentSegments(SCOPE, stream1).get().size());
        assertTrue(streamStorePartialMock.isSealed(stream1).get());

        //scale operation on the sealed stream.
        AbstractMap.SimpleEntry<Double, Double> segment3 = new AbstractMap.SimpleEntry<>(0.0, 0.2);
        AbstractMap.SimpleEntry<Double, Double> segment4 = new AbstractMap.SimpleEntry<>(0.3, 0.4);
        AbstractMap.SimpleEntry<Double, Double> segment5 = new AbstractMap.SimpleEntry<>(0.4, 0.5);

        ScaleResponse scaleOpResult = streamMetadataTasksPartialMock.scaleBody(SCOPE, stream1, Collections
                        .singletonList(0),
                Arrays.asList(segment3, segment4, segment5), 30).get();

        //scaling operation fails once a stream is sealed.
        assertEquals(ScaleStreamStatus.PRECONDITION_FAILED, scaleOpResult.getStatus());
    }
}
