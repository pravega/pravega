/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Segment;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import javax.annotation.concurrent.GuardedBy;

import lombok.Synchronized;
import lombok.val;

public class StateSynchronizerImpl<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
        implements StateSynchronizer<StateT, UpdateT, InitT> {

    private final RevisionedStreamClient<UpdateOrInit<StateT, UpdateT, InitT>> client;
    @GuardedBy("$lock")
    private StateT currentState;
    private Segment segment;
    private RevisionImpl initialRevision;
    
    public StateSynchronizerImpl(Segment segment, RevisionedStreamClient<UpdateOrInit<StateT, UpdateT, InitT>> client) {
        this.segment = segment;
        this.initialRevision = new RevisionImpl(segment, 0, 0);
        this.client = client;
    }

    @Override
    @Synchronized
    public StateT getState() {
        return currentState;
    }

    private Revision getRevision() {
        StateT state = getState();
        return state == null ? initialRevision : state.getRevision();
    }
    
    @Override
    public void fetchUpdates() {
        val iter = client.readFrom(getRevision());
        while (iter.hasNext()) {
            Entry<Revision, UpdateOrInit<StateT, UpdateT, InitT>> entry = iter.next();
            if (entry.getValue().isInit()) {
                InitT init = entry.getValue().getInit();
                Revision revision = entry.getValue().getInitRevision();
                if (isNewer(revision)) {
                    updateCurrentState(init.create(segment.getScopedStreamName(), revision));
                }
            } else {
                applyUpdates(entry.getKey().asImpl(), entry.getValue().getUpdates());
            }
        }
    }

    private void applyUpdates(RevisionImpl readRevision, List<? extends UpdateT> updates) {
        int i = 0;
        for (UpdateT update : updates) {
            StateT state = getState();
            RevisionImpl newRevision = new RevisionImpl(segment, readRevision.getOffsetInSegment(), i++);
            if (newRevision.compareTo(state.getRevision()) > 0) {
                updateCurrentState(update.applyTo(state, newRevision));
            }
        }
    }

    @Override
    public void updateState(Function<StateT, List<UpdateT>> updateGenerator) {
        while (true) {
            StateT state = getState();
            List<UpdateT> updates = updateGenerator.apply(state);
            if (updates == null || updates.isEmpty()) {
                break;
            }
            Revision newRevision = client.conditionallyWrite(state.getRevision(), new UpdateOrInit<>(updates));
            if (newRevision == null) {
                fetchUpdates();
            } else {
                applyUpdates(newRevision.asImpl(), updates);
                break;
            }
        }
    }

    @Override
    public void unconditionallyUpdateState(UpdateT update) {
        client.unconditionallyWrite(new UpdateOrInit<>(Collections.singletonList(update)));
    }

    @Override
    public void unconditionallyUpdateState(List<? extends UpdateT> update) {
        client.unconditionallyWrite(new UpdateOrInit<>(update));
    }

    @Override
    public void initialize(InitT initial) {
        Revision result = client.conditionallyWrite(initialRevision, new UpdateOrInit<>(initial, initialRevision));
        if (result == null) {
            fetchUpdates();
        } else {
            updateCurrentState(initial.create(segment.getScopedStreamName(), result));
        }
    }

    @Override
    public void compact(Revision revision, InitT compaction) {
        client.unconditionallyWrite(new UpdateOrInit<>(compaction, revision));
    }
    
    @Synchronized
    private boolean isNewer(Revision revision) {
        return currentState == null || currentState.getRevision().compareTo(revision) < 0;
    }
    
    @Synchronized
    private void updateCurrentState(StateT newValue) {
        if (newValue != null && isNewer(newValue.getRevision())) {
            currentState = newValue;
        }
    }
    
}
