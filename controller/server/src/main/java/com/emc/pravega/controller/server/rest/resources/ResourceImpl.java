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
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
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

    private final ControllerService controllerService;
    private final StreamMetadataStore streamStore;

    public ResourceImpl(final StreamMetadataStore streamStore,
                        final HostControllerStore hostStore,
                        final StreamMetadataTasks streamMetadataTasks,
                        final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {

        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
        this.streamStore = streamStore;
    }

    @Override
    public Response createStream(StreamConfigurationImpl streamConfig) throws Exception {

        log.info("createStream called for stream name: {}, scope name: {}, scaling policy: {} ",streamConfig.getName(),
                streamConfig.getScope(), streamConfig.getScalingPolicy());

        CompletableFuture<CreateStreamStatus> createStreamStatus = controllerService.createStream(streamConfig, System.currentTimeMillis());

        if(createStreamStatus.get() == CreateStreamStatus.SUCCESS )
            return Response.ok(streamConfig).status(201).build();
        else if(createStreamStatus.get() == CreateStreamStatus.STREAM_EXISTS)
            return Response.status(409).build();
        else
            return Response.status(500).build();
    }

    @Override
    public Response updateStreamConfig(StreamConfiguration streamConfig) throws Exception {
        log.info("updateStream  called for stream name: {}, scope name: {}, scaling policy: {} ",streamConfig.getName(),
                streamConfig.getScope(), streamConfig.getScalingPolicy());

        CompletableFuture<UpdateStreamStatus> updateStreamStatus = controllerService.alterStream(streamConfig);

        if(updateStreamStatus.get() == UpdateStreamStatus.SUCCESS) {
            return Response.ok(streamConfig).status(201).build();
        } else if(updateStreamStatus.get() == UpdateStreamStatus.STREAM_NOT_FOUND)
            return Response.status(404).build();
        else
            return Response.status(500).build();

    }

    @Override
    public Response getStreamConfig(String scope, String stream) throws Exception {
        CompletableFuture<StreamConfiguration>  streamConfig = streamStore.getConfiguration(stream);
        if(streamConfig.get() != null)
            return Response.ok(streamConfig.get()).status(200).build();
        else
            return Response.status(404).build();
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
