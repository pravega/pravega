/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.StreamManager;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.stream.StreamConfiguration;
import com.google.common.annotations.VisibleForTesting;

import java.net.URI;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class StreamManagerImpl implements StreamManager {

    private final Controller controller;

    public StreamManagerImpl(URI controllerUri) {
        this.controller = new ControllerImpl(controllerUri);
    }

    @VisibleForTesting
    public StreamManagerImpl(Controller controller) {
        this.controller = controller;
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        return FutureHelpers.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                                                                                               .scope(scopeName)
                                                                                               .streamName(streamName)
                                                                                               .scalingPolicy(config.getScalingPolicy())
                                                                                               .build()),
                RuntimeException::new);
    }

    @Override
    public boolean alterStream(String scopeName, String streamName, StreamConfiguration config) {
        return FutureHelpers.getAndHandleExceptions(controller.alterStream(StreamConfiguration.builder()
                                                                                              .scope(scopeName)
                                                                                              .streamName(streamName)
                                                                                              .scalingPolicy(config.getScalingPolicy())
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
        // Nothing to do.
    }

}
