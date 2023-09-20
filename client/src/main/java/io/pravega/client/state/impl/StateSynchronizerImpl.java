/**
 * Copyright Pravega Authors.
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
package io.pravega.client.state.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.Update;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.common.util.Retry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;

@Slf4j
@ToString(of = { "segment", "currentState" })
public class StateSynchronizerImpl<StateT extends Revisioned>
        implements StateSynchronizer<StateT> {

    private static final Retry.RetryWithBackoff RETRY_INDEFINITELY = Retry.withExpBackoff(1, 1, Integer.MAX_VALUE);
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

    private Revision getRevisionToReadFrom(boolean useState) {
        StateT state = getState();
        Revision revision;
        if (!useState || state == null) {
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
        Revision revision = getRevisionToReadFrom(true);
        log.trace("Fetching updates after {} ", revision);
        try {
            val iter = client.readFrom(revision);
            while (iter.hasNext()) {
                Entry<Revision, UpdateOrInit<StateT>> entry = iter.next();
                log.trace("Found entry {} ", entry.getValue());
                if (entry.getValue().isInit()) {
                    InitialUpdate<StateT> init = entry.getValue().getInit();
                    updateCurrentState(init.create(segment.getScopedStreamName(), entry.getKey()));
                } else {
                    applyUpdates(entry.getKey().asImpl(), entry.getValue().getUpdates());
                }
            }
        } catch (TruncatedDataException e) {
            log.info("{} encountered truncation on segment {}, Details: {}", this, segment, e.getMessage());
            RETRY_INDEFINITELY
                 .retryingOn(TruncatedDataException.class)
                 .throwingOn(RuntimeException.class)
                 .run(() -> handleTruncation());
        }
    }
    
    private Void handleTruncation() {
        Revision revision = getRevisionToReadFrom(false);
        log.info("{} encountered truncation, attempting to read from revision {}", this, revision);
        boolean foundInit = false;
        val iter = client.readFrom(revision);
        Revision currentRevision = null;
        while (!foundInit && iter.hasNext()) {
            Entry<Revision, UpdateOrInit<StateT>> entry = iter.next();
            currentRevision = entry.getKey();
            if (entry.getValue().isInit()) {
                log.trace("Found entry {} ", entry.getValue());
                InitialUpdate<StateT> init = entry.getValue().getInit();
                foundInit = true;
                updateCurrentState(init.create(segment.getScopedStreamName(), currentRevision));
            }
        }
        if (!foundInit) {
            throw new IllegalStateException(format("Data was truncated but there is no Init state after the truncation point. Last read revision is %s", currentRevision));
        }
        fetchUpdates();
        return null;
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
    public void updateState(UpdateGenerator<StateT> updateGenerator) {
        conditionallyWrite(state -> {
            List<Update<StateT>> update = new ArrayList<>();
            updateGenerator.accept(state, update);
            return update.isEmpty() ? null : new UpdateOrInit<>(update);
        });
    }

    @Override
    public <ReturnT> ReturnT updateState(UpdateGeneratorFunction<StateT, ReturnT> updateGenerator) {
        AtomicReference<ReturnT> result = new AtomicReference<>();
        conditionallyWrite(state -> {
            List<Update<StateT>> update = new ArrayList<>();
            result.set(updateGenerator.apply(state, update));
            return update.isEmpty() ? null : new UpdateOrInit<>(update);
        });
        return result.get();
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
        Revision result = client.writeConditionally(new RevisionImpl(segment, 0, 0), new UpdateOrInit<>(initial));
        if (result == null) {
            log.info("Segment {} was already initialized", segment);
            handleTruncation();
        } else {
            updateCurrentState(initial.create(segment.getScopedStreamName(), result));
        }
    }
    
    @Override
    public long bytesWrittenSinceCompaction() {
        Revision mark = client.getMark();
        StateT state = getState();
        long compaction = (mark == null) ? 0 : mark.asImpl().getOffsetInSegment();
        long current = (state == null) ? 0 : state.getRevision().asImpl().getOffsetInSegment();
        return Math.max(0, current - compaction);
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
                log.info("Compacted state is written at {} the oldMark is {}", newMark, oldMark);
            }
            if (oldMark != null) {
                client.truncateToRevision(oldMark);
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
                log.debug("Conditional write to segment {} completed as there is nothing to update after revision {}", segment, revision);
                break;
            }
            Revision newRevision = client.writeConditionally(revision, toWrite);
            log.trace("Conditional write returned {} ", newRevision);
            if (newRevision == null) {
                fetchUpdates();
            } else {
                if (!toWrite.isInit()) {
                    applyUpdates(newRevision, toWrite.getUpdates());
                }
                log.debug("Conditional write to segment {} completed with revision {}", segment, newRevision);
                break;
            }
        }
    }

    @Synchronized
    private boolean isNewer(Revision revision) {
        boolean result = currentState == null || currentState.getRevision().compareTo(revision) < 0;
        if (!result ) {
            log.debug("In memory state {} is newer than the provided revision {}", currentState.getRevision(), revision);
        }
        return result;
    }

    @Synchronized
    private void updateCurrentState(StateT newValue) {
        if (newValue != null && isNewer(newValue.getRevision())) {
            log.trace("Updating new state to {} ", newValue.getRevision());
            currentState = newValue;
        }
    }

    @Override
    public void close() {
        log.info("Closing stateSynchronizer {}", this);
        client.close();
    }
}
