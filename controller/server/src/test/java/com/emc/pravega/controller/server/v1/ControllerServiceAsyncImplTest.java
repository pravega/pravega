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
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.host.InMemoryHostControllerStoreConfig;
import com.emc.pravega.controller.store.stream.StoreConfiguration;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.model.ModelHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

/**
 * Async Controller Service Impl tests.
 */
public class ControllerServiceAsyncImplTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ControllerServiceAsyncImpl controllerService;

    private final TestingServer zkServer;


    public ControllerServiceAsyncImplTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();

        StoreConfiguration config = new StoreConfiguration(zkServer.getConnectString());

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        StoreClient storeClient = StoreClientFactory.createStoreClient(StoreClientFactory.StoreType.Zookeeper, config);

        final StreamMetadataStore streamStore =
                StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, null, executor);

        final TaskMetadataStore taskMetadataStore =
                TaskStoreFactory.createStore(storeClient, executor);

        final Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
        hostContainerMap.put(new Host("localhost", 12345), Collections.singleton(0));

        final HostControllerStore hostStore =
                HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory,
                        new InMemoryHostControllerStoreConfig(hostContainerMap, 1));

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, "host");
        StreamTransactionMetadataTasks streamTransactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, "host");

        this.controllerService =
                new ControllerServiceAsyncImpl(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }

    @Test
    public void createStreamTests() throws TException, ExecutionException, InterruptedException {
        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(SCOPE, stream2, policy2);
        CreateStreamStatus status;

        // region checkStream
        ThriftAsyncCallback<CreateStreamStatus> result1 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result1);
        status = result1.getResult().get();
        assertEquals(status, CreateStreamStatus.SUCCESS);

        ThriftAsyncCallback<CreateStreamStatus> result2 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration2), result2);
        status = result2.getResult().get();
        assertEquals(status, CreateStreamStatus.SUCCESS);
        // endregion

        // region duplicate create stream
        ThriftAsyncCallback<CreateStreamStatus> result3 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result3);
        status = result3.getResult().get();
        assertEquals(status, CreateStreamStatus.STREAM_EXISTS);
        // endregion
    }
}
