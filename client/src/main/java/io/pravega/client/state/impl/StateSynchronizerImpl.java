/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.Update;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateSynchronizerImpl<StateT extends Revisioned>
        implements StateSynchronizer<StateT> {

    private final RevisionedStreamClient<UpdateOrInit<StateT>> client;
    @GuardedBy("$lock")
    private StateT currentState;
    private Segment segment;

    /**
     * Creates a new instance of StateSynchronizer class.
     *
     * @param segment The segment.
     * @param client  The revisioned stream client this state synchronizer builds upon."
     */
    public StateSynchronizerImpl(Segment segment, RevisionedStreamClient<UpdateOrInit<StateT>> client) {
        this.segment = segment;
        this.client = client;
    }

    @Override
    @Synchronized
    public StateT getState() {
        return currentState;
    }

    private Revision getRevisionToReadFrom() {
        StateT state = getState();
        Revision revision;
        if (state == null) {
            revision = client.getMark();
            if (revision == null) {
                revision = client.fetchOldestRevision();
            }
        } else {
            revision = state.getRevision();
        }
        return revision;
    }

    @Override
    public void fetchUpdates() {
        Revision revision = getRevisionToReadFrom();
        log.trace("Fetching updates after {} ", revision);
        val iter = client.readFrom(revision);
        while (iter.hasNext()) {
            Entry<Revision, UpdateOrInit<StateT>> entry = iter.next();
            log.trace("Found entry {} ", entry.getValue());
            if (entry.getValue().isInit()) {
                InitialUpdate<StateT> init = entry.getValue().getInit();
                if (isNewer(entry.getKey())) {
                    updateCurrentState(init.create(segment.getScopedStreamName(), entry.getKey()));
                }
            } else {
                applyUpdates(entry.getKey().asImpl(), entry.getValue().getUpdates());
            }
        }
    }

    private void applyUpdates(Revision readRevision, List<? extends Update<StateT>> updates) {
        int i = 0;
        for (Update<StateT> update : updates) {
            StateT state = getState();
            synchronized (state) {
                RevisionImpl newRevision = new RevisionImpl(segment, readRevision.asImpl().getOffsetInSegment(), i++);
                if (newRevision.compareTo(state.getRevision()) > 0) {
                    updateCurrentState(update.applyTo(state, newRevision));
                }
            }
        }
    }

    @Override
    public void updateState(Function<StateT, List<? extends Update<StateT>>> updateGenerator) {
        conditionallyWrite(state -> {
            List<? extends Update<StateT>> update = updateGenerator.apply(state);
            return (update == null || update.isEmpty()) ? null : new UpdateOrInit<>(update);
        });
    }

    @Override
    public void updateStateUnconditionally(Update<StateT> update) {
        log.trace("Unconditionally Writing {} ", update);
        client.writeUnconditionally(new UpdateOrInit<>(Collections.singletonList(update)));
    }

    @Override
    public void updateStateUnconditionally(List<? extends Update<StateT>> update) {
        log.trace("Unconditionally Writing {} ", update);
        client.writeUnconditionally(new UpdateOrInit<>(update));
    }

    @Override
    public void initialize(InitialUpdate<StateT> initial) {
        Revision result = client.writeConditionally(client.fetchOldestRevision(), new UpdateOrInit<>(initial));
        if (result == null) {
            fetchUpdates();
        } else {
            updateCurrentState(initial.create(segment.getScopedStreamName(), result));
        }
    }

    @Override
    public void compact(Function<StateT, InitialUpdate<StateT>> compactor) {
        AtomicReference<Revision> compactedVersion = new AtomicReference<Revision>(null);
        conditionallyWrite(state -> {
            InitialUpdate<StateT> init = compactor.apply(state);
            if (init == null) {
                compactedVersion.set(null);
                return null;
            } else {
                compactedVersion.set(state.getRevision());
                return new UpdateOrInit<>(init);
            }
        });
        Revision newMark = compactedVersion.get();
        if (newMark != null) {
            Revision oldMark = client.getMark();
            if (oldMark == null || oldMark.compareTo(newMark) < 0) {
                client.compareAndSetMark(oldMark, newMark);
            }
        }
    }

    private void conditionallyWrite(Function<StateT, UpdateOrInit<StateT>> generator) {
        while (true) {
            StateT state = getState();
            if (state == null) {
                fetchUpdates();
                state = getState();
                if (state == null) {
                    throw new IllegalStateException("Write was called before the state was initialized.");
                }
            }
            log.trace("Conditionally Writing {} ", state);
            Revision revision = state.getRevision();
            UpdateOrInit<StateT> toWrite = generator.apply(state);
            if (toWrite == null) {
                break;
            }
            Revision newRevision = client.writeConditionally(revision, toWrite);
            log.trace("Conditionally write returned {} ", newRevision);
            if (newRevision == null) {
                fetchUpdates();
            } else {
                if (!toWrite.isInit()) {
                    applyUpdates(newRevision, toWrite.getUpdates());
                }
                break;
            }
        }
    }

    @Synchronized
    private boolean isNewer(Revision revision) {
        return currentState == null || currentState.getRevision().compareTo(revision) < 0;
    }

    @Synchronized
    private void updateCurrentState(StateT newValue) {
        if (newValue != null && isNewer(newValue.getRevision())) {
            log.trace("Updating new state to {} ", newValue.getRevision());
            currentState = newValue;
        }
    }

}
