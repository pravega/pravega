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
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.EventSegmentReaderUtility;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.shared.NameUtils.READER_GROUP_STREAM_PREFIX;

public class MockStreamManager implements StreamManager, ReaderGroupManager {
    @Getter
    private final String scope;
    @Getter
    private final ConnectionPool connectionPool;
    @Getter
    private final MockController controller;
    @Getter
    private final MockClientFactory clientFactory;
    @Getter
    private final EventSegmentReaderUtility eventSegmentReaderUtility;
    @Getter
    private final MockSegmentStreamFactory inFactory;

    public MockStreamManager(String scope, String endpoint, int port) {
        this.scope = scope;
        ClientConfig config = ClientConfig.builder().controllerURI(URI.create("tcp://localhost")).build();
        this.connectionPool = new ConnectionPoolImpl(config, new SocketConnectionFactoryImpl(config));
        this.controller = new MockController(endpoint, port, connectionPool, true);
        this.clientFactory = new MockClientFactory(scope, controller, connectionPool);
        this.inFactory = new MockSegmentStreamFactory();
        this.eventSegmentReaderUtility = new EventSegmentReaderUtility(inFactory);
    }

    @Override
    public boolean createScope(String scopeName) {
        return Futures.getAndHandleExceptions(controller.createScope(scope),
                RuntimeException::new);
    }

    @Override
    public boolean checkScopeExists(String scopeName) {
        return Futures.getAndHandleExceptions(controller.checkScopeExists(scopeName),
                RuntimeException::new);
    }

    @Override
    public Iterator<Stream> listStreams(String scopeName) {
        AsyncIterator<Stream> asyncIterator = controller.listStreams(scopeName);
        return asyncIterator.asIterator();
    }

    @Override
    public Iterator<Stream> listStreams(String scopeName, String tagName) {
        AsyncIterator<Stream> asyncIterator = controller.listStreamsForTag(scopeName, tagName);
        return asyncIterator.asIterator();
    }

    @Override
    public Collection<String> getStreamTags(String scopeName, String streamName) {
        return Futures.getAndHandleExceptions(controller.getStreamConfiguration(scopeName, streamName),
                                              RuntimeException::new).getTags();
    }

    @Override
    public boolean checkStreamExists(String scopeName, String streamName) {
        return Futures.getAndHandleExceptions(controller.checkStreamExists(scopeName, streamName),
                RuntimeException::new);
    }

    @Override
    public boolean deleteScope(String scopeName) {
        return Futures.getAndHandleExceptions(controller.deleteScope(scope),
                RuntimeException::new);
    }

    /**
     * A new API is created hence this is going to be deprecated.
     *
     * @deprecated As of Pravega release 0.11, replaced by {@link #deleteScopeRecursive(String)}.
     */
    @Override
    @Deprecated
    public boolean deleteScope(String scopeName, boolean forceDelete) {
        if (forceDelete) {
            List<String> readerGroupList = new ArrayList<>();
            Iterator<Stream> iterator = controller.listStreams(scopeName).asIterator();
            while (iterator.hasNext()) {
                Stream stream = iterator.next();
                if (stream.getStreamName().startsWith(READER_GROUP_STREAM_PREFIX)) {
                    readerGroupList.add(stream.getStreamName().substring(
                            READER_GROUP_STREAM_PREFIX.length()));
                }
                Futures.getAndHandleExceptions(controller.sealStream(scope, stream.getStreamName()), RuntimeException::new);
                Futures.getAndHandleExceptions(controller.deleteStream(scope, stream.getStreamName()), RuntimeException::new);
            }

            Iterator<KeyValueTableInfo> kvtIterator = controller.listKeyValueTables(scopeName).asIterator();
            while (iterator.hasNext()) {
                KeyValueTableInfo kvt = kvtIterator.next();
                Futures.getAndHandleExceptions(controller.deleteKeyValueTable(scopeName, kvt.getKeyValueTableName()), RuntimeException::new);
            }

            for (String rg: readerGroupList) {
                ReaderGroupConfig rgc = getAndHandleExceptions(controller.getReaderGroupConfig(scopeName, rg), RuntimeException::new);
                Futures.getAndHandleExceptions(controller.deleteReaderGroup(scopeName, rg, rgc.getReaderGroupId()), RuntimeException::new);
            }
        }
        return Futures.getAndHandleExceptions(controller.deleteScope(scope),
                RuntimeException::new);
    }

    @Override
    public boolean deleteScopeRecursive(String scopeName) {
        return Boolean.TRUE.equals(Futures.getAndHandleExceptions(controller.deleteScopeRecursive(scope),
                RuntimeException::new));
    }

    @Override
    public List<TransactionInfo> listCompletedTransactions(Stream stream) {
        return Futures.getAndHandleExceptions(controller.listCompletedTransactions(stream),
                RuntimeException::new);
    }

    @Override
    public CompletableFuture<StreamInfo> fetchStreamInfo(String scopeName, String streamName) {
        throw new NotImplementedException("fetchStreamInfo");
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        if (config == null) {
            config = StreamConfiguration.builder()
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build();
        }
        return Futures.getAndHandleExceptions(controller.createStream(scopeName, streamName, config), RuntimeException::new);
    }

    @Override
    public boolean updateStream(String scopeName, String streamName, StreamConfiguration config) {
        if (config == null) {
            config = StreamConfiguration.builder()
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build();
        }

        return Futures.getAndHandleExceptions(controller.updateStream(scopeName, streamName, config), RuntimeException::new);
    }

    @Override
    public boolean truncateStream(String scopeName, String streamName, StreamCut streamCut) {
        Preconditions.checkNotNull(streamCut);

        return Futures.getAndHandleExceptions(controller.truncateStream(scopeName, streamName, streamCut),
                RuntimeException::new);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        Futures.getAndHandleExceptions(controller.createStream(scope, streamName, config),
                RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }

    @Override
    public boolean sealStream(String scopeName, String streamName) {
        return Futures.getAndHandleExceptions(controller.sealStream(scopeName, streamName), RuntimeException::new);
    }

    @Override
    public void close() {
        clientFactory.close();
        connectionPool.close();
    }

    @Override
    public CompletableFuture<Long> getDistanceBetweenTwoStreamCuts(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut) {
        return null;
    }

    @Override
    public boolean createReaderGroup(String groupName, ReaderGroupConfig config) {
        NameUtils.validateReaderGroupName(groupName);
        createStreamHelper(NameUtils.getStreamForReaderGroup(groupName),
                StreamConfiguration.builder()
                                   .scalingPolicy(ScalingPolicy.fixed(1)).build());
        if (ReaderGroupConfig.DEFAULT_UUID.equals(config.getReaderGroupId())) {
            config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        }
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                                              new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        Futures.getThrowingException(controller.createReaderGroup(scope, groupName, config));
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);

        synchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(config, segments, getEndSegmentsForStreams(config), false));
        return true;
    }

    public Position getInitialPosition(String stream) {
        return new PositionImpl(controller.getSegmentsWithRanges(new StreamImpl(scope, stream))
                                          .stream()
                                          .collect(Collectors.toMap(segment -> segment, segment -> 0L)));
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        return new ReaderGroupImpl(scope, groupName, synchronizerConfig, new ReaderGroupStateInitSerializer(),
                                   new ReaderGroupStateUpdatesSerializer(), clientFactory, controller,
                                   connectionPool);
    }

    @Override
    public boolean deleteStream(String scopeName, String toDelete) {
        return Futures.getAndHandleExceptions(controller.deleteStream(scopeName, toDelete), RuntimeException::new);
    }

    @Override
    public Iterator<String> listScopes() {
        AsyncIterator<String> asyncIterator = controller.listScopes();
        return asyncIterator.asIterator();
    }

    @Override
    public <T> CompletableFuture<T> fetchEvent(EventPointer pointer, Serializer<T> serializer) {
        Preconditions.checkNotNull(pointer);
        Preconditions.checkNotNull(serializer);
        CompletableFuture<T> completableFuture = CompletableFuture.supplyAsync(() -> {
            @Cleanup
            EventSegmentReader inputStream = eventSegmentReaderUtility.createEventSegmentReader(pointer);
            try {
                ByteBuffer buffer = inputStream.read();
                return  serializer.deserialize(buffer);
            } catch (EndOfSegmentException e) {
                throw Exceptions.sneakyThrow(new NoSuchEventException(e.getMessage()));
            } catch (NoSuchSegmentException | SegmentTruncatedException e) {
                throw Exceptions.sneakyThrow(new NoSuchEventException("Event no longer exists."));
            }
        });
        return completableFuture;
    }

    @Override
    public void deleteReaderGroup(String groupName) {
        @Cleanup
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup(groupName),
                new ReaderGroupStateUpdatesSerializer(), new ReaderGroupStateInitSerializer(), SynchronizerConfig.builder().build());
        synchronizer.fetchUpdates();
        UUID groupId = synchronizer.getState().getConfig().getReaderGroupId();
        long generation = synchronizer.getState().getConfig().getGeneration();
        getAndHandleExceptions(controller.deleteReaderGroup(scope, groupName, groupId),
                RuntimeException::new);
    }
}
