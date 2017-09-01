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
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;

    private final ScheduledExecutorService executor; 
    
    public StreamManagerImpl(URI controllerUri) {
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "StreamManager-Controller");
        this.controller = new ControllerImpl(controllerUri, ControllerImplConfig.builder().build(), executor);
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller) {
        this.executor = null;
        this.controller = controller;
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        return FutureHelpers.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                        .scope(scopeName)
                        .streamName(streamName)
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()),
                RuntimeException::new);
    }

    @Override
    public boolean updateStream(String scopeName, String streamName, StreamConfiguration config) {
        return FutureHelpers.getAndHandleExceptions(controller.updateStream(StreamConfiguration.builder()
                        .scope(scopeName)
                        .streamName(streamName)
                        .scalingPolicy(config.getScalingPolicy())
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()),
                RuntimeException::new);
    }

    @Override
    public boolean sealStream(String scopeName, String streamName) {
        return FutureHelpers.getAndHandleExceptions(controller.sealStream(scopeName, streamName), RuntimeException::new);
    }

    @Override
    public boolean deleteStream(String scopeName, String toDelete) {
        return FutureHelpers.getAndHandleExceptions(controller.deleteStream(scopeName, toDelete), RuntimeException::new);
    }

    @Override
    public boolean createScope(String scopeName) {
        NameUtils.validateUserScopeName(scopeName);
        return FutureHelpers.getAndHandleExceptions(controller.createScope(scopeName),
                RuntimeException::new);
        
    }

    @Override
    public boolean deleteScope(String scopeName) {
        return FutureHelpers.getAndHandleExceptions(controller.deleteScope(scopeName),
                RuntimeException::new);
    }

    @Override
    public void close() {
        if (this.executor != null) {
            this.executor.shutdown();
        }
    }
}
