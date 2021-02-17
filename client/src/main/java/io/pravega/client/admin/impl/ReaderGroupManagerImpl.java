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
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.NameUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.common.concurrent.Futures.getThrowingException;

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

    @Override
    public void createReaderGroup(String groupName, ReaderGroupConfig config) {
        log.info("Creating reader group: {} for streams: {} with configuration: {}", groupName,
                Arrays.toString(config.getStartingStreamCuts().keySet().toArray()), config);
        NameUtils.validateReaderGroupName(groupName);
        if (config.getReaderGroupId() == ReaderGroupConfig.DEFAULT_UUID) {
            // make sure we never attempt to create a ReaderGroup with default ReadrGroupId which is 0-0-0
            config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        }
        ReaderGroupConfig controllerConfig = getThrowingException(controller.createReaderGroup(scope, groupName, config));
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                              new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, controllerConfig);
        synchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(controllerConfig, segments, getEndSegmentsForStreams(controllerConfig), false));
        ReaderGroupConfig syncConfig = synchronizer.getState().getConfig();
        if (ReaderGroupConfig.DEFAULT_UUID.equals(syncConfig.getReaderGroupId())
        && ReaderGroupConfig.DEFAULT_GENERATION == syncConfig.getGeneration()) {
            // migrate RG from version < 0.9 to 0.9+
            synchronizer.updateState((s, updates) -> {
              updates.add(new ReaderGroupState.ReaderGroupStateInit(controllerConfig, segments, getEndSegmentsForStreams(controllerConfig), false));
          });
        }
    }

    @Override
    public void deleteReaderGroup(String groupName) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        synchronizer.fetchUpdates();
        ReaderGroupConfig syncConfig = synchronizer.getState().getConfig();
        if (ReaderGroupConfig.DEFAULT_UUID.equals(syncConfig.getReaderGroupId())
            && ReaderGroupConfig.DEFAULT_GENERATION == syncConfig.getGeneration()) {
            // migrate RG case
            getAndHandleExceptions(controller.createReaderGroup(scope, groupName,
                    ReaderGroupConfig.cloneConfig(syncConfig, UUID.randomUUID(), 0L))
                    .thenCompose(conf -> controller.deleteReaderGroup(scope, groupName, conf.getReaderGroupId(),
                            conf.getGeneration())), RuntimeException::new);
        } else {
            // normal delete
            getAndHandleExceptions(controller.deleteReaderGroup(scope, groupName, syncConfig.getReaderGroupId(),
                    syncConfig.getGeneration()),
                    RuntimeException::new);
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