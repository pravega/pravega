/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Checkpoint;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import lombok.Data;

import org.apache.commons.lang.NotImplementedException;

@Data
public class ReaderGroupImpl implements ReaderGroup {

    private final String scope;
    private final String groupName;
    private final List<String> streamNames;
    private final ReaderGroupConfig config;
    private final SynchronizerConfig synchronizerConfig;
    private final Serializer<ReaderGroupStateInit> initSerializer;
    private final Serializer<ReaderGroupStateUpdate> updateSerializer;
    private final ClientFactory clientFactory;

    /**
     * Called by the StreamManager to provide the initial segments the stream should use.
     * @param segments The initial segments mapped to the offset within them
     */
    @VisibleForTesting
    public void initializeGroup(Map<Segment, Long> segments) {
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(groupName,
                                                                                                 updateSerializer,
                                                                                                 initSerializer,
                                                                                                 synchronizerConfig);
        ReaderGroupStateManager.initializeReaderGroup(synchronizer, new ArrayList<>(streamNames), config, segments);
    }
    
    @Override
    public void readerOffline(String readerId, Position lastPosition) {
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(groupName,
                                                                                                 updateSerializer,
                                                                                                 initSerializer,
                                                                                                 synchronizerConfig);
        ReaderGroupStateManager.readerShutdown(readerId, lastPosition.asImpl(), synchronizer);
    }

    @Override
    public Set<String> getOnlineReaders() {
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(groupName,
                                                                                                 updateSerializer,
                                                                                                 initSerializer,
                                                                                                 synchronizerConfig);
        synchronizer.fetchUpdates();
        return synchronizer.getState().getOnlineReaders();
    }

    @Override
    public Future<Checkpoint> initiateCheckpoint(String checkpointName) {
        throw new NotImplementedException();
    }

    @Override
    public void resetReadersToCheckpoint(Checkpoint checkpointName) {
        throw new NotImplementedException();
    }

    @Override
    public ReaderGroup alterConfig(ReaderGroupConfig config, List<String> streamNames) {
        throw new NotImplementedException();
    }

}
