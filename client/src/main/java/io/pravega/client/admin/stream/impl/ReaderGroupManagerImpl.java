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
package io.pravega.client.admin.stream.impl;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import java.net.URI;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class ReaderGroupManagerImpl implements ReaderGroupManager {

    private final String scope;
    private final ClientFactory clientFactory;
    private final Controller controller;

    public ReaderGroupManagerImpl(String scope, URI controllerUri) {
        this.scope = scope;
        this.controller = new ControllerImpl(controllerUri);
        this.clientFactory = ClientFactory.withScope(scope, this.controller);
    }

    public ReaderGroupManagerImpl(String scope, Controller controller, ClientFactory clientFactory) {
        this.scope = scope;
        this.clientFactory = clientFactory;
        this.controller = controller;
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
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, Set<String> streams) {
        NameUtils.validateReaderGroupName(groupName);
        createStreamHelper(NameUtils.getStreamForReaderGroup(groupName),
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(NameUtils.getStreamForReaderGroup(groupName))
                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                              .build());
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        ReaderGroupImpl result = new ReaderGroupImpl(scope,
                                                     groupName,
                                                     synchronizerConfig,
                                                     new JavaSerializer<>(),
                                                     new JavaSerializer<>(),
                                                     clientFactory,
                                                     controller);
        result.initializeGroup(config, streams);
        return result;
    }
    
    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        return new ReaderGroupImpl(scope,
                                   groupName,
                                   synchronizerConfig,
                                   new JavaSerializer<>(),
                                   new JavaSerializer<>(),
                                   clientFactory,
                                   controller);
    }

    @Override
    public void deleteReaderGroup(ReaderGroup group) {
        throw new NotImplementedException();
    }

    @Override
    public void close() {
        clientFactory.close();
    }

}
