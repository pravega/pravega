/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.client.stream.StreamConfiguration;
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
                        .retentionPolicy(config.getRetentionPolicy())
                        .build()),
                RuntimeException::new);
    }

    @Override
    public boolean alterStream(String scopeName, String streamName, StreamConfiguration config) {
        return FutureHelpers.getAndHandleExceptions(controller.alterStream(StreamConfiguration.builder()
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
        // Nothing to do.
    }

}
