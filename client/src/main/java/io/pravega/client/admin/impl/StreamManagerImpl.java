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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BlockingAsyncIterator;
import io.pravega.shared.NameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService executor;
    private final StreamCutHelper streamCutHelper;
    
    public StreamManagerImpl(ClientConfig clientConfig) {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "StreamManager-Controller");
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig) .build(), executor);
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig);
        this.streamCutHelper = new StreamCutHelper(controller, connectionFactory);
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller, ConnectionFactory connectionFactory) {
        this.executor = null;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.streamCutHelper = new StreamCutHelper(controller, connectionFactory);
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
    public boolean createScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Creating scope: {}", scopeName);
        return  Futures.getThrowingException(controller.createScope(scopeName));
    }

    @Override
    public Iterator<Stream> listStreams(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Listing streams in scope: {}", scopeName);
        AsyncIterator<Stream> asyncIterator = controller.listStreams(scopeName);
        return new BlockingAsyncIterator<>(asyncIterator);
    }

    @Override
    public boolean deleteScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        log.info("Deleting scope: {}", scopeName);
        return  Futures.getThrowingException(controller.deleteScope(scopeName));
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
     * Note: The access level of this method can be reduced once the deprecated method
     * {@link io.pravega.client.batch.BatchClient#getStreamInfo(Stream)} is removed.
     *
     * @param stream The Stream.
     * @return A future representing {@link StreamInfo}.
     */
    public CompletableFuture<StreamInfo> getStreamInfo(final Stream stream) {
        //Fetch the stream cut representing the current TAIL and current HEAD of the stream.
        CompletableFuture<StreamCut> currentTailStreamCut = streamCutHelper.fetchTailStreamCut(stream);
        CompletableFuture<StreamCut> currentHeadStreamCut = streamCutHelper.fetchHeadStreamCut(stream);
        return currentTailStreamCut.thenCombine(currentHeadStreamCut,
                                                (tailSC, headSC) -> new StreamInfo(stream.getScope(),
                                                                                   stream.getStreamName(),
                                                                                   tailSC, headSC));
    }

    @Override
    public void close() {
        if (this.controller != null) {
            this.controller.close();
        }
        if (this.executor != null) {
            ExecutorServiceHelpers.shutdown(this.executor);
        }
        if (this.connectionFactory != null) {
            this.connectionFactory.close();
        }
    }
}
