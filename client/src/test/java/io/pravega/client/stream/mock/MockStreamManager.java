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
package io.pravega.client.stream.mock;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.impl.ReaderGroupImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.commons.lang.NotImplementedException;

public class MockStreamManager implements StreamManager, ReaderGroupManager {

    private final String scope;
    private final ConnectionFactoryImpl connectionFactory;
    private final MockController controller;
    @Getter
    private final MockClientFactory clientFactory;

    public MockStreamManager(String scope, String endpoint, int port) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = new MockController(endpoint, port, connectionFactory);
        this.clientFactory = new MockClientFactory(scope, controller);
    }

    @Override
    public boolean createScope(String scopeName) {
        return FutureHelpers.getAndHandleExceptions(controller.createScope(scope),
                RuntimeException::new);
    }

    @Override
    public boolean deleteScope(String scopeName) {
        return FutureHelpers.getAndHandleExceptions(controller.deleteScope(scope),
                RuntimeException::new);
    }

    @Override
    public boolean createStream(String scopeName, String streamName, StreamConfiguration config) {
        NameUtils.validateUserStreamName(streamName);
        if (config == null) {
            config = StreamConfiguration.builder()
                                        .scope(scopeName)
                                        .streamName(streamName)
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build();
        }

        return FutureHelpers.getAndHandleExceptions(controller.createStream(StreamConfiguration.builder()
                                                                                               .scope(scopeName)
                                                                                               .streamName(streamName)
                                                                                               .scalingPolicy(config.getScalingPolicy())
                                                                                               .build()),
                RuntimeException::new);
    }

    @Override
    public boolean alterStream(String scopeName, String streamName, StreamConfiguration config) {
        if (config == null) {
            config = StreamConfiguration.builder()
                                        .scope(scopeName)
                                        .streamName(streamName)
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build();
        }

        return FutureHelpers.getAndHandleExceptions(controller.alterStream(StreamConfiguration.builder()
                                                                                              .scope(scopeName)
                                                                                              .streamName(streamName)
                                                                                              .scalingPolicy(config.getScalingPolicy())
                                                                                              .build()),
                RuntimeException::new);
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
    public boolean sealStream(String scopeName, String streamName) {
        return FutureHelpers.getAndHandleExceptions(controller.sealStream(scopeName, streamName), RuntimeException::new);
    }

    @Override
    public void close() {
        clientFactory.close();
        connectionFactory.close();
    }

    @Override
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, Set<String> streamNames) {
        NameUtils.validateReaderGroupName(groupName);
        createStreamHelper(NameUtils.getStreamForReaderGroup(groupName),
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(NameUtils.getStreamForReaderGroup(groupName))
                                              .scalingPolicy(ScalingPolicy.fixed(1)).build());
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        ReaderGroupImpl result = new ReaderGroupImpl(scope,
                                                     groupName,
                                                     synchronizerConfig,
                                                     new JavaSerializer<>(),
                                                     new JavaSerializer<>(),
                                                     clientFactory,
                                                     controller);
        result.initializeGroup(config, streamNames);
        return result;
    }

    public Position getInitialPosition(String stream) {
        return new PositionImpl(controller.getSegmentsForStream(new StreamImpl(scope, stream))
                                          .stream()
                                          .collect(Collectors.toMap(segment -> segment, segment -> 0L)));
    }

    @Override
    public ReaderGroup getReaderGroup(String groupName) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteReaderGroup(ReaderGroup group) {
        throw new NotImplementedException();
    }

    @Override
    public boolean deleteStream(String scopeName, String toDelete) {
        throw new NotImplementedException();
    }
}
