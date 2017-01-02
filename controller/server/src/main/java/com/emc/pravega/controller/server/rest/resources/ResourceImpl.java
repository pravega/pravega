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

package com.emc.pravega.controller.server.rest.resources;

import com.emc.pravega.controller.server.rest.v1.ApiV1;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.StoreClientFactory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;

import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.emc.pravega.controller.util.Config.*;

/*
Implementation of Resource
 */
@Slf4j
public class ResourceImpl implements com.emc.pravega.controller.server.rest.v1.ApiV1.Controller {

    /*private final ControllerService controllerService;

    public ResourceImpl(final StreamMetadataStore streamStore,
                        final HostControllerStore hostStore,
                        final StreamMetadataTasks streamMetadataTasks,
                        final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }*/

    @Override
    public Response createStream(StreamConfigurationImpl streamConfig) throws Exception {

        System.out.println("streamConfig NAME "+streamConfig.getName());
        System.out.println("streamConfig SCOPE "+streamConfig.getScope());
        System.out.println("streamConfig scaling policy "+streamConfig.getScalingPolicy());
        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        log.info("Creating store client");
        StoreClient storeClient = StoreClientFactory.createStoreClient(
                StoreClientFactory.StoreType.valueOf(STORE_TYPE));

        log.info("Creating the stream store");
        StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.valueOf(STREAM_STORE_TYPE), executor);

        log.info("Creating zk based task store");
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        log.info("Creating the host store");
        HostControllerStore hostStore = HostStoreFactory.createStore(
                HostStoreFactory.StoreType.valueOf(HOST_STORE_TYPE));
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);
        ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);

        CompletableFuture<CreateStreamStatus> createStreamStatus = controllerService.createStream(streamConfig, System.currentTimeMillis());
        if (createStreamStatus.isDone()) {
            log.info("rest api controller service called -- DONE");
        } else {
            log.info("rest api controller service called -- NOT DONE");
        }

        log.info("rest api controller service called");
        log.info("rest api controllerService = {}", controllerService.toString());
        System.out.println(createStreamStatus.get());

        return Response.serverError().status(500).build();
    }

    @Override
    public Response updateStreamConfig(StreamConfiguration streamConfiguration) {
        return Response.serverError().status(500).build();
    }

    @Override
    public Response getStreamConfig() {
        return Response.serverError().status(500).build();
    }

    @Override
    public Response listStreamsInScope() {
        return Response.serverError().status(500).build();
    }

    @Override
    public Response listScopes() {
        return Response.serverError().status(500).build();
    }

    @Override
    public Response deleteStream(StreamConfiguration streamConfiguration) {
        return Response.serverError().status(500).build();
    }

    /*
    @Override
    public CreateStreamStatus createStream(final StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createStream(ModelHelper.encode(streamConfig),
                System.currentTimeMillis()), RuntimeException::new);
    }
    */
}
