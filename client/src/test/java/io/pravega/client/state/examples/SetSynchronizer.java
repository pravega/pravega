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
package io.pravega.client.state.examples;

import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.impl.JavaSerializer;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * An example of how to use StateSynchronizer that coordinates the values in a set.
 * @param <T> The type of the values in the set.
 */
@ToString
@Slf4j
public class SetSynchronizer<T extends Serializable> {

    /**
     * The Object to by synchronized.
     */
    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static class UpdatableSet<T> implements Revisioned, Serializable {
        private static final long serialVersionUID = 1L;
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
        public String getScopedStreamName() {
            return streamName;
        }
    }

    /**
     * A base class for all updates to the state. This allows for several different types of updates.
     */
    @ToString
    private static abstract class SetUpdate<T> implements Update<UpdatableSet<T>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public UpdatableSet<T> applyTo(UpdatableSet<T> oldState, Revision newRevision) {
            LinkedHashSet<T> impl = new LinkedHashSet<>(oldState.impl);
            log.trace("Applying update {} to {} ", this, oldState);
            process(impl);
            return new UpdatableSet<>(oldState.streamName, impl, newRevision);
        }

        public abstract void process(LinkedHashSet<T> updatableList);
    }

    /**
     * Add an item to the set.
     */
    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static class AddToSet<T> extends SetUpdate<T> {
        private static final long serialVersionUID = 1L;
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.add(value);
        }
    }

    /**
     * Remove an item from the set.
     */
    @ToString
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static class RemoveFromSet<T> extends SetUpdate<T> {
        private static final long serialVersionUID = 1L;
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.remove(value);
        }
    }

    /**
     * Clear the set.
     */
    @ToString
    @EqualsAndHashCode(callSuper = false)
    private static class ClearSet<T> extends SetUpdate<T> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.clear();
        }
    }
    
    /**
     * Create a set. (This is used to initialize things)
     */
    @Data
    private static class CreateSet<T> implements InitialUpdate<UpdatableSet<T>>, Serializable {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        private final LinkedHashSet<T> impl;
        
        @Override
        public UpdatableSet<T> create(String streamName, Revision revision) {
            return new UpdatableSet<>(streamName, impl, revision);
        }
    }

    //----
    // Below this point is some example code that uses the classes above and the StateSynchronizer.
    //----
    
    private static final int REMOVALS_BEFORE_COMPACTION = 5;

    private final StateSynchronizer<UpdatableSet<T>> stateSynchronizer;
    private final AtomicInteger countdownToCompaction = new AtomicInteger(REMOVALS_BEFORE_COMPACTION);

    private SetSynchronizer(String scopedStreamName, StateSynchronizer<UpdatableSet<T>> synchronizer) {
        this.stateSynchronizer = synchronizer;
        synchronizer.initialize(new CreateSet<T>(new LinkedHashSet<>()));
    }

    /**
     * Get updates from other SetSynchronizers.
     */
    public void update() {
        stateSynchronizer.fetchUpdates();
    }

    /**
     * Returns the current values in the set.
     */
    public Set<T> getCurrentValues() {
        return stateSynchronizer.getState().getCurrentValues();
    }

    /**
     * Returns the size of the current set.
     */
    public int getCurrentSize() {
        return stateSynchronizer.getState().getCurrentSize();
    }

    /**
     * Add a new item to the set if it does not currently have it.
     * @param value The value to be added.
     */
    public void add(T value) {
        stateSynchronizer.updateState((set, updates) -> {
            if (!set.impl.contains(value)) {
                updates.add(new AddToSet<>(value));
            }
        });
    }
    
    /**
     * Remove an item from the set if it is present.
     * @param value The value to be removed.
     */
    public void remove(T value) {
        stateSynchronizer.updateState((set, updates) -> {
            if (set.impl.contains(value)) {
                updates.add(new RemoveFromSet<>(value));
            }
        });
        if (countdownToCompaction.decrementAndGet() <= 0) {
            compact();
        }
    }

    private void compact() {
        countdownToCompaction.set(REMOVALS_BEFORE_COMPACTION);
        stateSynchronizer.compact(state -> new CreateSet<T>(state.impl));
    }

    /**
     * Clears the set.
     */
    public void clear() {
        stateSynchronizer.updateState((set, updates) -> { 
            if (set.getCurrentSize() > 0) {
                updates.add(new ClearSet<>());
            }
        });
        compact();
    }
    
    public static <T extends Serializable> SetSynchronizer<T> createNewSet(String streamName, SynchronizerClientFactory factory) {
        return new SetSynchronizer<>(streamName,
                factory.createStateSynchronizer(streamName,
                                                new JavaSerializer<SetUpdate<T>>(),
                                                new JavaSerializer<CreateSet<T>>(),
                                                SynchronizerConfig.builder().build()));
    }

}
