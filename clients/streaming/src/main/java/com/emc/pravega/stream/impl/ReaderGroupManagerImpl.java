/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
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
public class ReaderGroupManagerImpl implements ReaderGroupManager {

    private final String scope;
    private final ClientFactory clientFactory;
    private final ControllerImpl controller;

    public ReaderGroupManagerImpl(String scope, URI controllerUri) {
        this.scope = scope;
        this.clientFactory = ClientFactory.withScope(scope, controllerUri);
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                                                                                        .scope(scope)
                                                                                        .streamName(streamName)
                                                                                        .scalingPolicy(config.getScalingPolicy())
                                                                                        .build()),
                RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }
    
    @Override
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, List<String> streams) {
        createStreamHelper(groupName,
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(groupName)
                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                              .build());
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
    public ReaderGroup alterReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames) {
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
    public void close() {
        clientFactory.close();
    }

}
