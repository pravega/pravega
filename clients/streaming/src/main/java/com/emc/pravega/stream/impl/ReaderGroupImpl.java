/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        ReaderGroupStateManager.initializeReaderGroup(synchronizer, segments);
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
}
