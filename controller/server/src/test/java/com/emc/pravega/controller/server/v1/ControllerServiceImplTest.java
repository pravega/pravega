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

import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Async Controller Service Impl tests.
 */
public class ControllerServiceImplTest {
    private static final String SCOPE = "scope";

    //Ensure each test completes within 5 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(5, TimeUnit.SECONDS);

    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ControllerServiceImpl controllerService;

    private final TestingServer zkServer;

    public ControllerServiceImplTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        StoreClient storeClient = new ZKStoreClient(zkClient);

        final StreamMetadataStore streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory,
                executor);

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, "host");
        StreamTransactionMetadataTasks streamTransactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, "host");

        this.controllerService = new ControllerServiceImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks));
    }

    @Test
    public void createStreamTests() throws ExecutionException, InterruptedException {
        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 = new StreamConfigurationImpl(SCOPE, stream1, policy1);
        final StreamConfiguration configuration2 = new StreamConfigurationImpl(SCOPE, stream2, policy2);

        CreateStreamStatus status;

        // region checkStream
        ResultObserver<CreateStreamStatus> result1 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result1);
        synchronized (result1) {
            if (result1.getResult() == null) {
                result1.wait();
            }
        }
        status = result1.getResult();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.SUCCESS);

        ResultObserver<CreateStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration2), result2);
        synchronized (result2) {
            if (result2.getResult() == null) {
                result2.wait();
            }
        }
        status = result2.getResult();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.SUCCESS);
        // endregion

        // region duplicate create stream
        ResultObserver<CreateStreamStatus> result3 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result3);
        synchronized (result3) {
            if (result3.getResult() == null) {
                result3.wait();
            }
        }
        status = result3.getResult();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.STREAM_EXISTS);
        // endregion
    }

    private static class ResultObserver<T> implements StreamObserver<T> {
        @Getter
        private T result = null;

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                this.notifyAll();
            }
        }
    }
}
