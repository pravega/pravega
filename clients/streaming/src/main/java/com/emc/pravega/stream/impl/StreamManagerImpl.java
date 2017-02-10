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
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.ScalingPolicy.Type;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;

import static com.emc.pravega.common.concurrent.FutureHelpers.allOfWithResults;
import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class StreamManagerImpl implements StreamManager {

    private final String scope;
    private final ClientFactory clientFactory;
    private final ControllerImpl controller;

    public StreamManagerImpl(String scope, URI controllerUri, ClientFactory clientFactory) {
        this.scope = scope;
        this.clientFactory = clientFactory;
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    @Override
    public void createStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller.createStream(new StreamConfigurationImpl(scope, streamName,
                        config.getScalingPolicy())),
                RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }

    @Override
    public void sealStream(String streamName) {
        FutureHelpers.getAndHandleExceptions(controller.sealStream(scope, streamName), RuntimeException::new);
    }

    @Override
    public void close() throws Exception {

    }
    
    @Override
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, List<String> streams) {
        createStreamHelper(groupName,
                           new StreamConfigurationImpl(scope,
                                   groupName,
                                   new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, 1)));
        SynchronizerConfig synchronizerConfig = new SynchronizerConfig(null, null);
        ReaderGroupImpl result = new ReaderGroupImpl(scope,
                groupName,
                streams,
                config,
                synchronizerConfig,
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                clientFactory);
        List<CompletableFuture<Map<Segment, Long>>> futures = new ArrayList<>(streams.size());
        for (String stream : streams) {
            CompletableFuture<List<PositionInternal>> future = controller.getPositions(new StreamImpl(scope, stream), 0, 1);
            futures.add(future.thenApply(list -> list.stream()
                                         .flatMap(pos -> pos.getOwnedSegmentsWithOffsets().entrySet().stream())
                                         .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))));
        }
        Map<Segment, Long> segments = getAndHandleExceptions(allOfWithResults(futures).thenApply(listOfMaps -> {
            return listOfMaps.stream()
                             .flatMap(map -> map.entrySet().stream())
                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }), RuntimeException::new);
        result.initializeGroup(segments);
        return result;
    }

    @Override
    public ReaderGroup updateReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames) {
        throw new NotImplementedException();
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteReaderGroup(ReaderGroup group) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteStream(String toDelete) {
        throw new NotImplementedException();
    }

}
