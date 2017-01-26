package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateInit;
import com.emc.pravega.stream.impl.ReaderGroupState.ReaderGroupStateUpdate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

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

    public void initalizeGroup(Map<Segment, Long> segments) {
        StateSynchronizer<ReaderGroupState> synchronizer = clientFactory.createStateSynchronizer(groupName,
                                                                                                 updateSerializer,
                                                                                                 initSerializer,
                                                                                                 synchronizerConfig);
        ReaderGroupStateManager.initializeReadererGroup(synchronizer, segments);
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
}
