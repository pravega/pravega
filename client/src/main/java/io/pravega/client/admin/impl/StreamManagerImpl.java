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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.StreamInfo;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A stream manager. Used to bootstrap the client.
 */
@Slf4j
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;
    private ConnectionFactory connectionFactory;
    private SegmentMetadataClientFactory segmentMetadataClientFactory;

    @GuardedBy("this")
    private AtomicReference<String> latestDelegationToken;
    private final ScheduledExecutorService executor; 
    
    public StreamManagerImpl(ClientConfig clientConfig) {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "StreamManager-Controller");
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig) .build(), executor);
        this.connectionFactory = new ConnectionFactoryImpl(clientConfig);
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        this.latestDelegationToken = new AtomicReference<>();
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller, ConnectionFactory connectionFactory) {
        this.executor = null;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        this.latestDelegationToken = new AtomicReference<>();
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        log.info("Creating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        NameUtils.validateUserStreamName(streamName);
        return Futures.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                        .scope(scopeName)
                        .streamName(streamName)
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()),
                RuntimeException::new);
    }

    @Override
    public boolean updateStream(String scopeName, String streamName, StreamConfiguration config) {
        log.info("Updating scope/stream: {}/{} with configuration: {}", scopeName, streamName, config);
        return Futures.getAndHandleExceptions(controller.updateStream(StreamConfiguration.builder()
                        .scope(scopeName)
                        .streamName(streamName)
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()),
                RuntimeException::new);
    }

    @Override
    public boolean truncateStream(String scopeName, String streamName, StreamCut streamCut) {
        Preconditions.checkNotNull(streamCut);

        log.info("Truncating scope/stream: {}/{} with stream cut: {}", scopeName, streamName, streamCut);
        return Futures.getAndHandleExceptions(controller.truncateStream(scopeName, streamName, streamCut),
                RuntimeException::new);
    }

    @Override
    public boolean sealStream(String scopeName, String streamName) {
        return Futures.getAndHandleExceptions(controller.sealStream(scopeName, streamName), RuntimeException::new);
    }

    @Override
    public boolean deleteStream(String scopeName, String toDelete) {
        return Futures.getAndHandleExceptions(controller.deleteStream(scopeName, toDelete), RuntimeException::new);
    }

    @Override
    public boolean createScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        return Futures.getAndHandleExceptions(controller.createScope(scopeName),
                RuntimeException::new);
        
    }

    @Override
    public boolean deleteScope(String scopeName) {
        return Futures.getAndHandleExceptions(controller.deleteScope(scopeName),
                RuntimeException::new);
    }

    @Override
    public StreamInfo getStreamInfo(String scopeName, String streamName) {
        NameUtils.validateScopeName(scopeName);
        NameUtils.validateStreamName(streamName);
        final Stream stream = Stream.of(scopeName, streamName);

        //Fetch the stream cut representing the current TAIL and current HEAD of the stream.
        CompletableFuture<StreamCut> currentTailStreamCut = fetchTailStreamCut(stream);
        CompletableFuture<StreamCut> currentHeadStreamCut = fetchHeadStreamCut(stream);
        CompletableFuture<StreamInfo> streamInfo = currentTailStreamCut.thenCombine(currentHeadStreamCut,
                                                                                    (tailSC, headSC) -> new StreamInfo(scopeName, streamName, tailSC, headSC));
        return Futures.getAndHandleExceptions(streamInfo, RuntimeException::new);
    }

    private CompletableFuture<StreamCut> fetchHeadStreamCut(final Stream stream) {
        //Fetch segments pointing to the current HEAD of the stream.
        return controller.getSegmentsAtTime(new StreamImpl(stream.getScope(), stream.getStreamName()), 0L)
                         .thenApply( s -> new StreamCutImpl(stream, s));
    }

    private CompletableFuture<StreamCut> fetchTailStreamCut(final Stream stream) {
        return controller.getCurrentSegments(stream.getScope(), stream.getStreamName())
                         .thenApply(s -> {
                             synchronized (this) {
                                 latestDelegationToken.set(s.getDelegationToken());
                             }
                             Map<Segment, Long> pos =
                                     s.getSegments().stream().map(this::segmentToInfo)
                                      .collect(Collectors.toMap(SegmentInfo::getSegment, SegmentInfo::getWriteOffset));
                             return new StreamCutImpl(stream, pos);
                         });
    }

    private SegmentInfo segmentToInfo(Segment s) {
        String delegationToken;
        synchronized (this) {
            delegationToken = latestDelegationToken.get();
        }
        @Cleanup
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s, delegationToken);
        return client.getSegmentInfo();
    }

    @Override
    public void close() {
        if (this.controller != null) {
            this.controller.close();
        }
        if (this.executor != null) {
            ExecutorServiceHelpers.shutdown(this.executor);
        }
    }
}
