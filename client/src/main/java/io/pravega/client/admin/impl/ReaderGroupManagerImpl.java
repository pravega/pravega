/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.admin.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.NameUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import lombok.Cleanup;
import lombok.SneakyThrows;
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
    private final AbstractClientFactoryImpl clientFactory;
    private final Controller controller;

    public ReaderGroupManagerImpl(String scope, ClientConfig config, ConnectionFactory connectionFactory) {
        this.scope = scope;
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                connectionFactory.getInternalExecutor());

        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(config, connectionFactory);
        this.clientFactory = new ClientFactoryImpl(scope, this.controller, connectionPool);
    }

    public ReaderGroupManagerImpl(String scope, Controller controller, AbstractClientFactoryImpl clientFactory) {
        this.scope = scope;
        this.clientFactory = clientFactory;
        this.controller = controller;
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        getAndHandleExceptions(controller.createStream(scope, streamName, StreamConfiguration.builder()
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
                                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                                  .build());
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                              new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        synchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(config, segments, getEndSegmentsForStreams(config)));

        if (config.isSubscriber()) {
            Set<Stream> streams = config.getStartingStreamCuts().keySet();
            streams.forEach(s -> {
                CompletableFuture<Boolean> result = controller.addSubscriber(scope, s.getStreamName(), groupName);
                try {
                    result.join();
                } catch (Exception e) {
                    log.warn("Failed to add subscriber reader group: ", e);
                    throw e;
                }
            });
        }
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
                                             throw Exceptions.sneakyThrow(e);
                                         }),
                               RuntimeException::new);

        AsyncIterator<Stream> streamIter = controller.listStreams(scope);
        while (streamIter.getNext() != null) {
            String streamName = streamIter.getNext().join().getStreamName();
            controller.listSubscribers(scope, streamName)
                    .thenApply(ls -> ls.contains(groupName))
                    .thenApply(b -> {
                        if (b) {
                            return controller.deleteSubscriber(scope, streamName, groupName);
                        }
                        return null;
                    });
        }
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        return new ReaderGroupImpl(scope, groupName, synchronizerConfig, new ReaderGroupStateInitSerializer(), new ReaderGroupStateUpdatesSerializer(),
                                   clientFactory, controller, clientFactory.getConnectionPool());
    }

    @Override
    public void close() {
        clientFactory.close();
    }
    
    @VisibleForTesting
    public static class ReaderGroupStateInitSerializer implements Serializer<InitialUpdate<ReaderGroupState>> {
        private final ReaderGroupState.ReaderGroupInitSerializer serializer = new ReaderGroupState.ReaderGroupInitSerializer();

        @Override
        @SneakyThrows(IOException.class)
        public ByteBuffer serialize(InitialUpdate<ReaderGroupState> value) {
             ByteArraySegment serialized = serializer.serialize(value);
             return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
        }

        @Override
        @SneakyThrows(IOException.class)
        public InitialUpdate<ReaderGroupState> deserialize(ByteBuffer serializedValue) {
            return serializer.deserialize(new ByteArraySegment(serializedValue));
        }
    }
    
    @VisibleForTesting
    public static class ReaderGroupStateUpdatesSerializer implements Serializer<Update<ReaderGroupState>> {
        private final ReaderGroupState.ReaderGroupUpdateSerializer serializer = new ReaderGroupState.ReaderGroupUpdateSerializer();

        @Override
        @SneakyThrows(IOException.class)
        public ByteBuffer serialize(Update<ReaderGroupState> value) {
             ByteArraySegment serialized = serializer.serialize(value);
             return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
        }

        @Override
        @SneakyThrows(IOException.class)
        public Update<ReaderGroupState> deserialize(ByteBuffer serializedValue) {
            return serializer.deserialize(new ByteArraySegment(serializedValue));
        }
    }

}