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
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.NameUtils;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final ScheduledExecutorService executor;
    private final StreamCutHelper streamCutHelper;


    public StreamManagerImpl(ClientConfig clientConfig) {
        this(clientConfig, ControllerImplConfig.builder().clientConfig(clientConfig).build());
    }

    @VisibleForTesting
    public StreamManagerImpl(ClientConfig clientConfig, ControllerImplConfig controllerConfig) {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "StreamManager-Controller");
        this.controller = new ControllerImpl(controllerConfig, executor);
        this.connectionPool = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        this.streamCutHelper = new StreamCutHelper(controller, connectionPool);
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller, ConnectionPool connectionPool) {
        this.executor = null;
        this.controller = controller;
        this.connectionPool = connectionPool;
        this.streamCutHelper = new StreamCutHelper(controller, connectionPool);
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Creating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        return  Futures.getThrowingException(controller.createStream(scopeName, streamName, StreamConfiguration.builder()
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()));
    }

    @Override
    public boolean updateStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Updating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        return Futures.getThrowingException(controller.updateStream(scopeName, streamName, StreamConfiguration.builder()
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
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
    public Iterator<Stream> listStreams(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Listing streams in scope: {}", scopeName);
        AsyncIterator<Stream> asyncIterator = controller.listStreams(scopeName);
        return asyncIterator.asIterator();
    }

    @Override
    public boolean checkStreamExists(String scopeName, String streamName) {
        log.info("Checking if stream {} exists in scope {}", streamName, scopeName);
        return  Futures.getThrowingException(controller.checkStreamExists(scopeName, streamName));
    }

    @Override
    public boolean deleteScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope: {}", scopeName);
        return Futures.getThrowingException(controller.deleteScope(scopeName));
    }

    @Override
    public boolean deleteScope(String scopeName, boolean deleteStreams) throws DeleteScopeFailedException {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope: {}", scopeName);
        if (deleteStreams) {
            Iterator<Stream> iterator = listStreams(scopeName);
            while (iterator.hasNext()) {
                Stream stream = iterator.next();
                try {
                    Futures.getThrowingException(Futures.exceptionallyExpecting(controller.sealStream(stream.getScope(), stream.getStreamName()),
                            e -> {
                                Throwable unwrap = Exceptions.unwrap(e);
                                // If the stream was removed by another request while we attempted to seal it, we could get InvalidStreamException. 
                                // ignore failures if the stream doesnt exist or we are unable to seal it. 
                                return unwrap instanceof InvalidStreamException || unwrap instanceof ControllerFailureException;
                            }, false).thenCompose(sealed -> controller.deleteStream(stream.getScope(), stream.getStreamName())));
                } catch (Exception e) {
                    String message = String.format("Failed to seal and delete stream %s", stream.getStreamName());
                    throw new DeleteScopeFailedException(message, e);
                }
            }
        }
        return Futures.getThrowingException(controller.deleteScope(scopeName));
    }
    
    @Override
    public StreamInfo getStreamInfo(String scopeName, String streamName) {
        NameUtils.validateUserStreamName(streamName);
        NameUtils.validateUserScopeName(scopeName);
        log.info("Fetching StreamInfo for scope/stream: {}/{}", scopeName, streamName);
        return Futures.getThrowingException(getStreamInfo(Stream.of(scopeName, streamName)));
    }

    /**
     * Fetch the {@link StreamInfo} for a given stream.
     *
     * @param stream The Stream.
     * @return A future representing {@link StreamInfo}.
     */
    private CompletableFuture<StreamInfo> getStreamInfo(final Stream stream) {
        //Fetch the stream cut representing the current TAIL and current HEAD of the stream.
        CompletableFuture<StreamCut> currentTailStreamCut = streamCutHelper.fetchTailStreamCut(stream);
        CompletableFuture<StreamCut> currentHeadStreamCut = streamCutHelper.fetchHeadStreamCut(stream);
        return currentTailStreamCut.thenCombine(currentHeadStreamCut,
                                                (tailSC, headSC) -> {
                                                    boolean isSealed = ((StreamCutImpl) tailSC).getPositions().isEmpty();
                                                    return new StreamInfo(stream.getScope(),
                                                            stream.getStreamName(),
                                                            tailSC, headSC, isSealed);
                                                });
    }

    @Override
    public void close() {
        if (this.controller != null) {
            Callbacks.invokeSafely(this.controller::close, ex -> log.error("Unable to close Controller client.", ex));
        }
        if (this.executor != null) {
            ExecutorServiceHelpers.shutdown(this.executor);
        }
        if (this.connectionPool != null) {
            this.connectionPool.close();
        }
    }
}
