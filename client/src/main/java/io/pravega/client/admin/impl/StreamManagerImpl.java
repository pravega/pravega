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
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderGroupNotFoundException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.impl.EventSegmentReaderUtility;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.AccessLevel;
import lombok.Cleanup;

import static io.pravega.shared.NameUtils.READER_GROUP_STREAM_PREFIX;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final ConnectionPool connectionPool;
    private final StreamCutHelper streamCutHelper;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final  EventSegmentReaderUtility eventSegmentReaderUtility;

    private final SegmentMetadataClientFactory metaFactory;

    public StreamManagerImpl(ClientConfig clientConfig) {
        this(clientConfig, ControllerImplConfig.builder().clientConfig(clientConfig).build());
    }

    @VisibleForTesting
    public StreamManagerImpl(ClientConfig clientConfig, ControllerImplConfig controllerConfig) {
        this(controllerConfig, new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig)));
    }

    private StreamManagerImpl(ControllerImplConfig controllerConfig, ConnectionPool connectionPool) {
        this(new ControllerImpl(controllerConfig, connectionPool.getInternalExecutor()), connectionPool);
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller, ConnectionPool connectionPool) {
      this(controller, connectionPool, new SegmentInputStreamFactoryImpl(controller, connectionPool));
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller, ConnectionPool connectionPool, SegmentInputStreamFactory inputStreamFactory) {
        this.connectionPool = connectionPool;
        this.controller = controller;
        this.streamCutHelper = new StreamCutHelper(controller, connectionPool);
        this.inputStreamFactory = inputStreamFactory;
        this.eventSegmentReaderUtility = new EventSegmentReaderUtility(inputStreamFactory);
        this.metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Creating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        return  Futures.getThrowingException(controller.createStream(scopeName, streamName, StreamConfiguration.builder()
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .tags(config.getTags())
                        .build()));
    }

    @Override
    public boolean updateStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Updating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        return Futures.getThrowingException(controller.updateStream(scopeName, streamName,
                                                                    StreamConfiguration.builder()
                                                                                       .scalingPolicy(config.getScalingPolicy())
                                                                                       .retentionPolicy(config.getRetentionPolicy())
                                                                                       .tags(config.getTags())
                                                                                       .build()));
    }

    @Override
    public boolean truncateStream(String scopeName, String streamName, StreamCut streamCut) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        Preconditions.checkNotNull(streamCut);
        log.info("Truncating scope/stream: {}/{} with stream cut: {}", scopeName, streamName, streamCut);
        return Futures.getThrowingException(controller.truncateStream(scopeName, streamName, streamCut));
    }

    @Override
    public boolean sealStream(String scopeName, String streamName) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Sealing scope/stream: {}/{}", scopeName, streamName);
        return Futures.getThrowingException(controller.sealStream(scopeName, streamName));
    }

    @Override
    public boolean deleteStream(String scopeName, String streamName) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope/stream: {}/{}", scopeName, streamName);
        return  Futures.getThrowingException(controller.deleteStream(scopeName, streamName));
    }

    @Override
    public Iterator<String> listScopes() {
        log.info("Listing scopes");
        AsyncIterator<String> asyncIterator = controller.listScopes();
        return asyncIterator.asIterator();
    }

    @Override
    public boolean createScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Creating scope: {}", scopeName);
        return  Futures.getThrowingException(controller.createScope(scopeName));
    }

    @Override
    public boolean checkScopeExists(String scopeName) {
        log.info("Checking if scope {} exists", scopeName);
        return  Futures.getThrowingException(controller.checkScopeExists(scopeName));
    }

    @Override
    public List<TransactionInfo> listCompletedTransactions(Stream stream) {
        log.info("Listing completed transactions for stream : {}", stream.getStreamName());
        return Futures.getThrowingException(controller.listCompletedTransactions(stream));
    }

    @Override
    public Iterator<Stream> listStreams(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Listing streams in scope: {}", scopeName);
        AsyncIterator<Stream> asyncIterator = controller.listStreams(scopeName);
        return asyncIterator.asIterator();
    }

    @Override
    public Iterator<Stream> listStreams(String scopeName, String tagName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Listing streams in scope: {} which has tag: {}", scopeName, tagName);
        AsyncIterator<Stream> asyncIterator = controller.listStreamsForTag(scopeName, tagName);
        return asyncIterator.asIterator();
    }

    @Override
    public Collection<String> getStreamTags(String scopeName, String streamName) {
        NameUtils.validateUserScopeName(scopeName);
        NameUtils.validateUserStreamName(streamName);
        log.info("Fetching tags associated with stream: {}/{}", scopeName, streamName);
        return Futures.getThrowingException(controller.getStreamConfiguration(scopeName, streamName)
                                                      .thenApply(StreamConfiguration::getTags));
    }

    @Override
    public boolean checkStreamExists(String scopeName, String streamName) {
        log.info("Checking if stream {} exists in scope {}", streamName, scopeName);
        return  Futures.getThrowingException(controller.checkStreamExists(scopeName, streamName));
    }

    @Override
    public boolean deleteScopeRecursive(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope recursively: {}", scopeName);
        return Futures.getThrowingException(controller.deleteScopeRecursive(scopeName));
    }

    @Override
    public boolean deleteScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope: {}", scopeName);
        return Futures.getThrowingException(controller.deleteScope(scopeName));
    }

    /**
     * A new API is created hence this is going to be deprecated.
     *
     * @deprecated As of Pravega release 0.11.0, replaced by {@link #deleteScopeRecursive(String)}.
     */
    @Override
    @Deprecated
    public boolean deleteScope(String scopeName, boolean forceDelete) throws DeleteScopeFailedException {
        NameUtils.validateUserScopeName(scopeName);
        if (forceDelete) {
            log.info("Deleting scope recursively: {}", scopeName);
            List<String> readerGroupList = new ArrayList<>();
            Iterator<Stream> iterator = listStreams(scopeName);
            while (iterator.hasNext()) {
                Stream stream = iterator.next();
                if (stream.getStreamName().startsWith(READER_GROUP_STREAM_PREFIX)) {
                    readerGroupList.add(stream.getStreamName().substring(
                            READER_GROUP_STREAM_PREFIX.length()));
                }
                try {
                    Futures.getThrowingException(Futures.exceptionallyExpecting(controller.sealStream(stream.getScope(), stream.getStreamName()),
                            e -> {
                                Throwable unwrap = Exceptions.unwrap(e);
                                // If the stream was removed by another request while we attempted to seal it, we could get InvalidStreamException.
                                // ignore failures if the stream doesn't exist or we are unable to seal it.
                                return unwrap instanceof InvalidStreamException || unwrap instanceof ControllerFailureException;
                            }, false).thenCompose(sealed -> controller.deleteStream(stream.getScope(), stream.getStreamName())));
                } catch (Exception e) {
                    String message = String.format("Failed to seal and delete stream %s", stream.getStreamName());
                    throw new DeleteScopeFailedException(message, e);
                }
            }

            Iterator<KeyValueTableInfo> kvtIterator = controller.listKeyValueTables(scopeName).asIterator();
            while (kvtIterator.hasNext()) {
                KeyValueTableInfo kvt = kvtIterator.next();
                try {
                    Futures.getThrowingException(controller.deleteKeyValueTable(scopeName, kvt.getKeyValueTableName()));
                } catch (Exception e) {
                    String message = String.format("Failed to delete key-value table %s", kvt.getKeyValueTableName());
                    throw new DeleteScopeFailedException(message, e);
                }
            }

            for (String groupName: readerGroupList) {
                try {
                    Futures.getThrowingException(controller.getReaderGroupConfig(scopeName, groupName)
                            .thenCompose(conf -> controller.deleteReaderGroup(scopeName, groupName,
                                    conf.getReaderGroupId())));
                } catch (Exception e) {
                    if (Exceptions.unwrap(e) instanceof ReaderGroupNotFoundException) {
                        continue;
                    }
                    String message = String.format("Failed to delete reader group %s", groupName);
                    throw new DeleteScopeFailedException(message, e);
                }
            }
        }
        return Futures.getThrowingException(controller.deleteScope(scopeName));
    }

    @Override
    public CompletableFuture<StreamInfo> fetchStreamInfo(String scopeName, String streamName) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Fetching StreamInfo for scope/stream: {}/{}", scopeName, streamName);
        return fetchStreamInfo(Stream.of(scopeName, streamName));
    }

    /**
     * Fetch the {@link StreamInfo} for a given stream.
     *
     * @param stream The Stream.
     * @return A future representing {@link StreamInfo}.
     */
    private CompletableFuture<StreamInfo> fetchStreamInfo(final Stream stream) {
        // Fetch the stream configuration which includes the tags associated with the stream.
        CompletableFuture<StreamConfiguration> streamConfiguration = controller.getStreamConfiguration(stream.getScope(), stream.getStreamName());
        // Fetch the stream cut representing the current TAIL and current HEAD of the stream.
        CompletableFuture<StreamCut> currentTailStreamCut = streamCutHelper.fetchTailStreamCut(stream);
        CompletableFuture<StreamCut> currentHeadStreamCut = streamCutHelper.fetchHeadStreamCut(stream);
        return CompletableFuture.allOf(streamConfiguration, currentHeadStreamCut, currentTailStreamCut)
                                .thenApply(v -> {
                                    boolean isSealed = ((StreamCutImpl) currentTailStreamCut.join()).getPositions().isEmpty();
                                    return new StreamInfo(stream.getScope(), stream.getStreamName(), streamConfiguration.join(),
                                                          currentTailStreamCut.join(), currentHeadStreamCut.join(), isSealed);
                                });
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
    public void close() {
        if (this.controller != null) {
            Callbacks.invokeSafely(this.controller::close, ex -> log.error("Unable to close Controller client.", ex));
        }
        if (this.connectionPool != null) {
            this.connectionPool.close();
        }
    }

    @Override
    public CompletableFuture<Long> getDistanceBetweenTwoStreamCuts(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut) {
        Preconditions.checkNotNull(stream, "Ensure to pass a valid stream");
        Preconditions.checkNotNull(fromStreamCut, "fromStreamCut");
        Preconditions.checkNotNull(toStreamCut, "toStreamCut");
        final CompletableFuture<StreamSegmentSuccessors> unread;
        final Map<Segment, Long> endPositions;
        StreamCut startSC = fromStreamCut;
        StreamCut endSC = toStreamCut;
        if (!fromStreamCut.equals(StreamCut.UNBOUNDED)) {
            Preconditions.checkArgument(stream.getScopedName().equals(fromStreamCut.asImpl().getStream().getScopedName()), "Ensure that the stream passed is same as that of the input streamcut");
        } else if (!toStreamCut.equals(StreamCut.UNBOUNDED)) {
            Preconditions.checkArgument(stream.getScopedName().equals(toStreamCut.asImpl().getStream().getScopedName()), "Ensure that the stream passed is same as that of the input streamcut");
        }
        // get the head/tail streamcuts in case of UNBOUNDED Streamcuts
        if (fromStreamCut.equals(StreamCut.UNBOUNDED)) {
            startSC = getHeadStreamCut(stream);
        }
        if (toStreamCut.equals(StreamCut.UNBOUNDED)) {
            endSC = getTailStreamCut(stream);
        }
        Preconditions.checkArgument(startSC != endSC, "Ensure that two different stream cuts from same stream is passed");
        unread = controller.getSegments(startSC, endSC);
        endPositions = endSC.asImpl().getPositions();
        return unread.thenCompose(unreadVal -> {
            DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory
                    .create(controller, stream.getScope(), stream.getStreamName(), AccessOperation.READ);
            return Futures.allOfWithResults(unreadVal.getSegments().stream().map(s -> {
                if (endPositions.containsKey(s)) {
                    return CompletableFuture.completedFuture(endPositions.get(s));
                } else {
                    SegmentMetadataClient metadataClient = metaFactory.createSegmentMetadataClient(s, tokenProvider);
                    CompletableFuture<Long> result = metadataClient.fetchCurrentSegmentLength();
                    result.whenComplete((r, e) -> metadataClient.close());
                    return result;
                }
            }).collect(Collectors.toList()));
        }).thenApply(sizes -> {
            long totalLength = 0;
            for (long bytesRemaining : sizes) {
                totalLength += bytesRemaining;
            }
            if (fromStreamCut.equals(StreamCut.UNBOUNDED)) {
                StreamCut headSC = getHeadStreamCut(stream);
                for (long bytesRead : headSC.asImpl().getPositions().values()) {
                    totalLength -= bytesRead;
                }
            } else {
                for (long bytesRead : fromStreamCut.asImpl().getPositions().values()) {
                    totalLength -= bytesRead;
                }
            }
            log.debug("Remaining bytes from position: {} to position: {} is {}", fromStreamCut, toStreamCut, totalLength);
            return totalLength;
        });
    }

    private StreamCut getHeadStreamCut(Stream stream) {
        CompletableFuture<StreamInfo> streamInfo = fetchStreamInfo(stream.getScope(), stream.getStreamName());
        StreamCut headSC = streamInfo.join().getHeadStreamCut();
        return headSC;
    }

    private StreamCut getTailStreamCut(Stream stream) {
        CompletableFuture<StreamInfo> streamInfo = fetchStreamInfo(stream.getScope(), stream.getStreamName());
        StreamCut tailSC = streamInfo.join().getTailStreamCut();
        return tailSC;
    }
}
