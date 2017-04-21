/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.ClientFactory;
import io.pravega.ReaderGroupManager;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.state.SynchronizerConfig;
import io.pravega.stream.ReaderGroup;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Stream;
import io.pravega.stream.StreamConfiguration;
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
