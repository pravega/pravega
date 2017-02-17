/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.ScalingPolicy.Type;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.ReaderGroupImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;

import lombok.Getter;

public class MockStreamManager implements StreamManager {

    private final String scope;
    private final ConnectionFactoryImpl connectionFactory;
    private final Controller controller;
    @Getter
    private final MockClientFactory clientFactory;

    public MockStreamManager(String scope, String endpoint, int port) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = new MockController(endpoint, port, connectionFactory);
        this.clientFactory = new MockClientFactory(scope, endpoint, port);
    }

    @Override
    public void createStream(String streamName, StreamConfiguration config) {
        if (config == null) {
            config = new StreamConfigurationImpl(scope,
                    streamName,
                    new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, 1));
        }
        createStreamHelper(streamName, config);
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller.createStream(new StreamConfigurationImpl(scope,
                streamName,
                config.getScalingPolicy())), RuntimeException::new);
        return new StreamImpl(scope, streamName);
    }

    @Override
    public void sealStream(String streamName) {
        FutureHelpers.getAndHandleExceptions(controller.sealStream(scope, streamName), RuntimeException::new);
    }

    @Override
    public void close() {
        clientFactory.close();
        connectionFactory.close();
    }

    @Override
    public ReaderGroup createReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames) {
        createStreamHelper(groupName,
                           new StreamConfigurationImpl(scope,
                                   groupName,
                                   new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, 1)));
        SynchronizerConfig synchronizerConfig = new SynchronizerConfig(null, null);
        ReaderGroupImpl result = new ReaderGroupImpl(scope,
                groupName,
                streamNames,
                config,
                synchronizerConfig,
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                clientFactory);
        Map<Segment, Long> segments = streamNames.stream()
                .collect(Collectors.toMap((String name) -> new Segment(scope, name, 0), name -> Long.valueOf(0)));
        result.initializeGroup(segments);
        return result;
    }

    public Position getInitialPosition(String stream) {
        return new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0), 0L));
    }
    
    @Override
    public ReaderGroup updateReaderGroup(String groupName, ReaderGroupConfig config, List<String> streamNames) {
        throw new NotImplementedException();
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
    public void deleteStream(String toDelete) {
        throw new NotImplementedException();
    }
}
