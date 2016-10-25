package com.emc.pravega.state;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class SetSynchronizer<T> {

    @RequiredArgsConstructor
    private static class UpdatableSet<T> implements Updatable<UpdatableSet<T>, SetUpdate<T>>, Serializable {
        private final Set<T> impl;
        private final Revision currentRevision;

        private Set<T> getCurrentValues() {
            return Collections.unmodifiableSet(impl);
        }

        private int getCurrentSize() {
            return impl.size();
        }

        @Override
        public UpdatableSet<T> applyUpdate(Revision newRevision, SetUpdate<T> update) {
            LinkedHashSet<T> newImpl = new LinkedHashSet<>(impl);
            update.process(newImpl);
            return new UpdatableSet<T>(newImpl, newRevision);
        }

        @Override
        public Revision getCurrentRevision() {
            return currentRevision;
        }
    }

    private static abstract class SetUpdate<T> implements Serializable {
        public abstract void process(LinkedHashSet<T> updatableList);
    }

    @RequiredArgsConstructor
    private static class AddToList<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.add(value);
        }
    }

    @RequiredArgsConstructor
    private static class RemoveFromList<T> extends SetUpdate<T> {
        private final T value;

        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.remove(value);
        }
    }

    @RequiredArgsConstructor
    private static class ClearList<T> extends SetUpdate<T> {
        @Override
        public void process(LinkedHashSet<T> impl) {
            impl.clear();
        }
    }

    private static final int REMOVALS_BEFORE_COMPACTION = 5;

    private final StateSynchronizer<UpdatableSet<T>, SetUpdate<T>> synchronizer;
    private UpdatableSet<T> current;
    private int countdownToCompaction = REMOVALS_BEFORE_COMPACTION;

    private SetSynchronizer(StateSynchronizer<UpdatableSet<T>, SetUpdate<T>> synchronizer) {
        this.synchronizer = synchronizer;
        update();
    }

    @Synchronized
    public void update() {
        current = synchronizer.getCurrentState(current);
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
        UpdatableSet<T> newSet = synchronizer.attemptUpdate(current, new AddToList<>(value));
        if (newSet == null) {
            return false;
        }
        current = newSet;
        return true;
    }

    @Synchronized
    public boolean attemptRemove(T value) {
        UpdatableSet<T> newSet = synchronizer.attemptUpdate(current, new RemoveFromList<>(value));
        if (newSet == null) {
            return false;
        }
        current = newSet;
        countdownToCompaction--;
        if (countdownToCompaction <= 0) {
            synchronizer.compact(current);
            countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        }
        return true;
    }

    @Synchronized
    public boolean attemptClear() {
        UpdatableSet<T> newSet = synchronizer.attemptUpdate(current, new ClearList<>());
        if (newSet == null) {
            return false;
        }
        current = newSet;
        synchronizer.compact(current);
        countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        return true;
    }

}
