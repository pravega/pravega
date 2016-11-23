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
package com.emc.pravega.state.examples;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.JavaSerializer;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class SetSynchronizer<T extends Serializable> {

    @RequiredArgsConstructor
    private static class UpdatableSet<T> implements Revisioned, Serializable {
        private final String streamName;
        private final LinkedHashSet<T> impl;
        private final Revision currentRevision;

        private Set<T> getCurrentValues() {
            return Collections.unmodifiableSet(impl);
        }

        private int getCurrentSize() {
            return impl.size();
        }

        @Override
        public Revision getRevision() {
            return currentRevision;
        }

        @Override
        public String getQualifiedStreamName() {
            return streamName;
        }
    }

    private static abstract class SetUpdate<T> implements Update<UpdatableSet<T>>, Serializable {
        @Override
        public UpdatableSet<T> applyTo(UpdatableSet<T> oldState, Revision newRevision) {
            LinkedHashSet<T> impl = new LinkedHashSet<>(oldState.impl);
            process(impl);
            return new UpdatableSet<>(oldState.streamName, impl, newRevision);
        }

        public abstract void process(LinkedHashSet<T> updatableList);
    }

    @RequiredArgsConstructor
    private static class AddToSet<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.add(value);
        }
    }

    @RequiredArgsConstructor
    private static class RemoveFromSet<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.remove(value);
        }
    }

    @RequiredArgsConstructor
    private static class ClearSet<T> extends SetUpdate<T> {
        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.clear();
        }
    }
    
    @RequiredArgsConstructor
    private static class CreateSet<T> implements InitialUpdate<UpdatableSet<T>>, Serializable {
        private final String streamName;
        private final LinkedHashSet<T> impl;
        
        @Override
        public UpdatableSet<T> create(Revision revision) {
            return new UpdatableSet<>(streamName, impl, revision);
        }
    }

    private static final int REMOVALS_BEFORE_COMPACTION = 5;

    private final Synchronizer<UpdatableSet<T>, SetUpdate<T>, CreateSet<T>> synchronizer;
    private UpdatableSet<T> current;
    private int countdownToCompaction = REMOVALS_BEFORE_COMPACTION;

    private SetSynchronizer(Synchronizer<UpdatableSet<T>, SetUpdate<T>, CreateSet<T>> synchronizer) {
        this.synchronizer = synchronizer;
        String stream = synchronizer.getStream().getQualifiedName();
        UpdatableSet<T> state = synchronizer.initialize(new CreateSet<T>(stream, new LinkedHashSet<>()));
        if (state != null) {
            current = state;
        } else {
            current = synchronizer.getLatestState();
        }
    }

    @Synchronized
    public void update() {
        current = synchronizer.getLatestState(current);
    }

    @Synchronized
    public Set<T> getCurrentValues() {
        return current.getCurrentValues();
    }

    @Synchronized
    public int getCurrentSize() {
        return current.getCurrentSize();
    }

    @Synchronized
    public boolean attemptAdd(T value) {
        UpdatableSet<T> newSet = synchronizer.conditionallyUpdateState(current, new AddToSet<>(value));
        if (newSet == null) {
            return false;
        }
        current = newSet;
        return true;
    }

    @Synchronized
    public boolean attemptRemove(T value) {
        UpdatableSet<T> newSet = synchronizer.conditionallyUpdateState(current, new RemoveFromSet<>(value));
        if (newSet == null) {
            return false;
        }
        current = newSet;
        countdownToCompaction--;
        if (countdownToCompaction <= 0) {
            synchronizer.compact(current, new CreateSet<T>(current.streamName, current.impl));
            countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        }
        return true;
    }

    @Synchronized
    public boolean attemptClear() {
        UpdatableSet<T> newSet = synchronizer.conditionallyUpdateState(current, new ClearSet<>());
        if (newSet == null) {
            return false;
        }
        current = newSet;
        synchronizer.compact(current, new CreateSet<T>(current.streamName, current.impl));
        countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        return true;
    }
    
    public static <T extends Serializable> SetSynchronizer<T> createNewSet(Stream stream) {
        return new SetSynchronizer<>(stream.createSynchronizer(new JavaSerializer<SetUpdate<T>>(),
                                                               new JavaSerializer<CreateSet<T>>(),
                                                               new SynchronizerConfig(null, null)));
    }

}
