/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
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
        this.clientFactory = ClientFactory.withScope(scope, controllerUri);
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
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
        createStreamHelper(groupName,
                           StreamConfiguration.builder()
                                              .scope(scope)
                                              .streamName(groupName)
                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                              .build());
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        ReaderGroupImpl result = new ReaderGroupImpl(scope,
                                                     groupName,
                                                     streams,
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
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(groupName,
                                                                                         new JavaSerializer<>(),
                                                                                         new JavaSerializer<>(),
                                                                                         synchronizerConfig);
        Set<String> streamNames = sync.getState().getStreamNames();
        return new ReaderGroupImpl(scope,
                groupName,
                streamNames,
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
