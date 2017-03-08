/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.InvalidStreamException;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private final Controller controller;

    public ReaderGroupManagerImpl(String scope, URI controllerUri) {
        this.scope = scope;
        this.clientFactory = ClientFactory.withScope(scope, controllerUri);
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    public ReaderGroupManagerImpl(String scope, Controller controller) {
        this.scope = scope;
        this.clientFactory = ClientFactory.withScope(scope, controller);
        this.controller = controller;
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
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, Set<String> streams) {
        createStreamHelper(groupName,
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(groupName)
                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                              .build());
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
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
            futures.add(controller.getSegmentsAtTime(new StreamImpl(scope, stream), 0L));
        }
        Map<Segment, Long> segments = getAndHandleExceptions(allOfWithResults(futures).thenApply(listOfMaps -> {
            return listOfMaps.stream()
                             .flatMap(map -> map.entrySet().stream())
                             .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        }), InvalidStreamException::new);
        result.initializeGroup(segments);
        return result;
    }
    
    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(groupName,
                                                                                         new JavaSerializer<>(),
                                                                                         new JavaSerializer<>(),
                                                                                         synchronizerConfig);
        Set<String> streamNames = sync.getState().getStreamNames();
        ReaderGroupConfig config = sync.getState().getConfig();
        return new ReaderGroupImpl(scope,
                groupName,
                streamNames,
                config,
                synchronizerConfig,
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                clientFactory);
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
