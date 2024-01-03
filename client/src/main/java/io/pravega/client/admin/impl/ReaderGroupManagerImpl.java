/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import io.pravega.client.stream.ConfigMismatchException;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.NameUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.common.concurrent.Futures.getThrowingException;
import static io.pravega.shared.NameUtils.getStreamForReaderGroup;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class ReaderGroupManagerImpl implements ReaderGroupManager {

    private final String scope;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
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
    public boolean createReaderGroup(String groupName, ReaderGroupConfig config) throws ConfigMismatchException, ObjectClosedException {
        isExecutorAvailable();
        log.info("Creating reader group: {} for streams: {} with configuration: {}", groupName,
                Arrays.toString(config.getStartingStreamCuts().keySet().toArray()), config);
        NameUtils.validateReaderGroupName(groupName);
        if (config.getReaderGroupId() == ReaderGroupConfig.DEFAULT_UUID) {
            // make sure we never attempt to create a ReaderGroup with default ReadrGroupId which is 0-0-0
            config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        }
        ReaderGroupConfig controllerConfig = getThrowingException(controller.createReaderGroup(scope, groupName, config));
        if (!controllerConfig.equals(config)) {
            log.warn("ReaderGroup {} already exists with pre-existing configuration {}", groupName, controllerConfig);
            throw new ConfigMismatchException(groupName, controllerConfig);
        } else if (controllerConfig.getGeneration() > 0 ) {
            log.info("ReaderGroup {} already exists", groupName);
            return false;
        } else {
            @Cleanup
            StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                                                                                     new ReaderGroupStateUpdatesSerializer(), 
                                                                                                     new ReaderGroupStateInitSerializer(), 
                                                                                                     SynchronizerConfig.builder().build());
            Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, controllerConfig);
            synchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(controllerConfig, segments, getEndSegmentsForStreams(controllerConfig), false));
            return true;
        }
    }

    @Override
    public void deleteReaderGroup(String groupName) throws ObjectClosedException {
        isExecutorAvailable();
        UUID readerGroupId = null;
        ReaderGroupConfig syncConfig = null;
        try {
            @Cleanup
            StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
            synchronizer.fetchUpdates();
            syncConfig = synchronizer.getState().getConfig();
            readerGroupId = syncConfig.getReaderGroupId();
            if (ReaderGroupConfig.DEFAULT_UUID.equals(syncConfig.getReaderGroupId())
                    && ReaderGroupConfig.DEFAULT_GENERATION == syncConfig.getGeneration()) {
                // migrate RG case
                try {
                    controller.getReaderGroupConfig(scope, groupName)
                            .thenCompose(conf -> controller.deleteReaderGroup(scope, groupName,
                                    conf.getReaderGroupId())).join();
                } catch (ReaderGroupNotFoundException ex) {
                    controller.sealStream(scope, getStreamForReaderGroup(groupName))
                            .thenCompose(b -> controller.deleteStream(scope, getStreamForReaderGroup(groupName)))
                            .exceptionally(e -> {
                                log.warn("Failed to delete ReaderGroup Stream {}", getStreamForReaderGroup(groupName), e);
                                throw Exceptions.sneakyThrow(e);
                            }).join();
                }
                return;
            }
        } catch (InvalidStreamException ex) {
            log.warn("State-Synchronizer Stream for ReaderGroup {} was not found.", NameUtils.getScopedReaderGroupName(scope, groupName));
            // if the State Synchronizer Stream was deleted, but the RG still exists, get Id from Controller
            readerGroupId = getAndHandleExceptions(controller.getReaderGroupConfig(scope, groupName),
                    RuntimeException::new).getReaderGroupId();
        }
        // normal delete
        getAndHandleExceptions(controller.deleteReaderGroup(scope, groupName, readerGroupId),
                RuntimeException::new);
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) throws ReaderGroupNotFoundException, ObjectClosedException {
        isExecutorAvailable();
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();

        try {
            return new ReaderGroupImpl(scope, groupName, synchronizerConfig, new ReaderGroupStateInitSerializer(), new ReaderGroupStateUpdatesSerializer(),
                                       clientFactory, controller, clientFactory.getConnectionPool());
        } catch (InvalidStreamException e) {
            throw new ReaderGroupNotFoundException(groupName, e);
        }
    }

    @Override
    public void close() {
        clientFactory.close();
    }

    private void isExecutorAvailable() {
        if (this.clientFactory.getConnectionPool().getInternalExecutor().isShutdown() || this.clientFactory.getConnectionPool().getInternalExecutor().isTerminated()) {
            log.error("Executor is closed, can not process further calls");
            throw new ObjectClosedException(this.clientFactory.getConnectionPool().getInternalExecutor());
        }
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