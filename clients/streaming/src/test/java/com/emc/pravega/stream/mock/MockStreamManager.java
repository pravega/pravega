/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.ReaderGroupImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
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
        createStreamHelper(groupName,
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(groupName)
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
