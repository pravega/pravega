/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.shared.NameUtils;
import java.util.Arrays;
import java.util.Map;
import lombok.Cleanup;
import lombok.Lombok;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.shared.NameUtils.getStreamForReaderGroup;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class ReaderGroupManagerImpl implements ReaderGroupManager {

    private final String scope;
    private final ClientFactory clientFactory;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;

    public ReaderGroupManagerImpl(String scope, ClientConfig config, ConnectionFactory connectionFactory) {
        this.scope = scope;
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                connectionFactory.getInternalExecutor());

        this.connectionFactory = connectionFactory;
        this.clientFactory = new ClientFactoryImpl(scope, this.controller, connectionFactory);
    }

    public ReaderGroupManagerImpl(String scope, Controller controller, ClientFactory clientFactory, ConnectionFactory connectionFactory) {
        this.scope = scope;
        this.clientFactory = clientFactory;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                                                                          .scope(scope)
                                                                          .streamName(streamName)
                                                                          .scalingPolicy(config.getScalingPolicy())
                                                                          .build()),
                               RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }

    @Override
    public void createReaderGroup(String groupName, ReaderGroupConfig config) {
        log.info("Creating reader group: {} for streams: {} with configuration: {}", groupName,
                Arrays.toString(config.getStartingStreamCuts().keySet().toArray()), config);
        NameUtils.validateReaderGroupName(groupName);
        createStreamHelper(getStreamForReaderGroup(groupName), StreamConfiguration.builder()
                                                                                  .scope(scope)
                                                                                  .streamName(getStreamForReaderGroup(groupName))
                                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                                  .build());
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                              new JavaSerializer<>(), new JavaSerializer<>(), SynchronizerConfig.builder().build());
        Map<Segment, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        synchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(config, segments, getEndSegmentsForStreams(config)));
    }

    @Override
    public void deleteReaderGroup(String groupName) {
        getAndHandleExceptions(controller.sealStream(scope, getStreamForReaderGroup(groupName))
                                         .thenCompose(b -> controller.deleteStream(scope,
                                                                                   getStreamForReaderGroup(groupName)))
                                         .exceptionally(e -> {
                                             if (e instanceof InvalidStreamException) {
                                                 return null;
                                             } else {
                                                 log.warn("Failed to delete stream", e);
                                             }
                                             throw Lombok.sneakyThrow(e);
                                         }),
                               RuntimeException::new);
        
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        return new ReaderGroupImpl(scope, groupName, synchronizerConfig, new JavaSerializer<>(), new JavaSerializer<>(),
                                   clientFactory, controller, connectionFactory);
    }

    @Override
    public void close() {
        clientFactory.close();
    }

}