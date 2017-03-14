/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;

import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;

import java.net.URI;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.NotImplementedException;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class StreamManagerImpl implements StreamManager {

    private final String scope;
    private final Controller controller;

    public StreamManagerImpl(String scope, URI controllerUri) {
        this.scope = scope;
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    @VisibleForTesting
    StreamManagerImpl(String scope, Controller controller) {
        this.scope = scope;
        this.controller = controller;
    }

    @Override
    public void createStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                                                                                        .scope(scope)
                                                                                        .streamName(streamName)
                                                                                        .scalingPolicy(config.getScalingPolicy())
                                                                                        .build()),
                RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }

    @Override
    public void sealStream(String streamName) {
        FutureHelpers.getAndHandleExceptions(controller.sealStream(scope, streamName), RuntimeException::new);
    }

    @Override
    public void deleteStream(String toDelete) {
        throw new NotImplementedException();
    }

    @Override
    public void createScope(String name) {
        CreateScopeStatus status = FutureHelpers.getAndHandleExceptions(controller.createScope(name),
                RuntimeException::new);
    }

    @Override
    public void deleteScope(String name) {
        DeleteScopeStatus status = FutureHelpers.getAndHandleExceptions(controller.deleteScope(name),
                RuntimeException::new);
    }


    @Override
    public void close() {
        // Nothing to do.
    }

}
