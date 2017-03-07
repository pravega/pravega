/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state.examples;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.impl.JavaSerializer;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.RequiredArgsConstructor;

/**
 * An example of how to use StateSynchronizer that coordinates the values in a set.
 * @param <T> The type of the values in the set.
 */
public class SetSynchronizer<T extends Serializable> {

    /**
     * The Object to by synchronized.
     */
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
        public String getScopedStreamName() {
            return streamName;
        }
    }

    /**
     * A base class for all updates to the state. This allows for several different types of updates.
     */
    private static abstract class SetUpdate<T> implements Update<UpdatableSet<T>>, Serializable {
        @Override
        public UpdatableSet<T> applyTo(UpdatableSet<T> oldState, Revision newRevision) {
            LinkedHashSet<T> impl = new LinkedHashSet<>(oldState.impl);
            process(impl);
            return new UpdatableSet<>(oldState.streamName, impl, newRevision);
        }

        public abstract void process(LinkedHashSet<T> updatableList);
    }

    /**
     * Add an item to the set.
     */
    @RequiredArgsConstructor
    private static class AddToSet<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.add(value);
        }
    }

    /**
     * Remove an item from the set.
     */
    @RequiredArgsConstructor
    private static class RemoveFromSet<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.remove(value);
        }
    }

    /**
     * Clear the set.
     */
    @RequiredArgsConstructor
    private static class ClearSet<T> extends SetUpdate<T> {
        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.clear();
        }
    }
    
    /**
     * Create a set. (This is used to initialize things)
     */
    @RequiredArgsConstructor
    private static class CreateSet<T> implements InitialUpdate<UpdatableSet<T>>, Serializable {
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
        stateSynchronizer.updateState(set -> {
            if (set.impl.contains(value)) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(new AddToSet<>(value));
            }
        });
    }
    
    /**
     * Remove an item from the set if it is present.
     * @param value The value to be removed.
     */
    public void remove(T value) {
        stateSynchronizer.updateState(set -> {
            if (set.impl.contains(value)) {
                return  Collections.singletonList(new RemoveFromSet<>(value));
            } else {
                return Collections.emptyList();
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
        stateSynchronizer.updateState(set -> { 
            if (set.getCurrentSize() > 0) {
                return Collections.singletonList(new ClearSet<>());
            } else {
                return Collections.emptyList();
            }
        });
        compact();
    }
    
    public static <T extends Serializable> SetSynchronizer<T> createNewSet(String streamName, ClientFactory factory) {
        return new SetSynchronizer<>(streamName,
                factory.createStateSynchronizer(streamName,
                                                new JavaSerializer<SetUpdate<T>>(),
                                                new JavaSerializer<CreateSet<T>>(),
                                                SynchronizerConfig.builder().build()));
    }

}
