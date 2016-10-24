package com.emc.pravega.state;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class SetSynchronizer<T> {

    private static class UpdatableSet<T> implements Updatable<SetUpdate<T>>, Serializable {
        private final Set<T> impl = new LinkedHashSet<>();
        private Revision currentRevision;
        
        @Synchronized
        private Set<T> getCurrentValues(){
            return Collections.unmodifiableSet(impl);
        }
        
        @Synchronized
        private int getCurrentSize() {
            return impl.size();
        }
        
        @Synchronized
        private void add(T value) {
            impl.add(value);
        }

        @Synchronized
        private void remove(T value) {
            impl.remove(value);
        }

        @Synchronized
        private void clear() {
            impl.clear();
        }

        @Synchronized
        @Override
        public void applyUpdate(Revision newRevision, SetUpdate<T> update) {
            update.process(this);
            currentRevision = newRevision;
        }

        @Override
        @Synchronized
        public Revision getCurrentRevision() {
            return currentRevision;
        }
    }
    
    private static abstract class SetUpdate<T> implements Serializable {
        public abstract void process(UpdatableSet<T> updatableList);
    }
    
    @RequiredArgsConstructor
    private static class AddToList<T> extends SetUpdate<T> {
        private final T value;
        @Override
        public void process(UpdatableSet<T> updatableList) {
            updatableList.add(value);
        }
    }
    @RequiredArgsConstructor
    private static class RemoveFromList<T> extends SetUpdate<T> {
        private final T value;
        @Override
        public void process(UpdatableSet<T> updatableList) {
            updatableList.remove(value);
        }
    }
    @RequiredArgsConstructor
    private static class ClearList<T> extends SetUpdate<T> {
        @Override
        public void process(UpdatableSet<T> updatableList) {
            updatableList.clear();
        }
    }
    
    private static final int REMOVALS_BEFORE_COMPACTION = 5;
    
    private final StateSynchronizer<UpdatableSet<T>, SetUpdate<T>> synchronizer;
    private UpdatableSet<T> current;
    private int countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
    
    private SetSynchronizer(StateSynchronizer<UpdatableSet<T>, SetUpdate<T>> synchronizer) {
        this.synchronizer = synchronizer;
        getNewBaseVersion();
        update();
    }

    private void getNewBaseVersion() {
        current = synchronizer.getInitialState();
    }

    public void update() {
        while(!synchronizer.synchronizeLocalState(current)) {
            getNewBaseVersion();
        }
    }
    
    public Set<T> getCurrentValues(){
        return current.getCurrentValues();
    }
    
    public int getCurrentSize() {
        return current.getCurrentSize();
    }
    
    public boolean attemptAdd(T value) {
        return synchronizer.attemptUpdate(current, new AddToList<>(value));
    }
    
    public boolean attemptRemove(T value) {
        boolean result = synchronizer.attemptUpdate(current, new RemoveFromList<>(value));
        if (result) {
            countdownToCompaction--;
            if (countdownToCompaction <= 0) {
                synchronizer.compact(current);
                countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
            }
        }
        return result;
    }
    
    public boolean attemptClear() {
        boolean result = synchronizer.attemptUpdate(current, new ClearList<>());
        if (result) {
            synchronizer.compact(current);
            countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        }
        return result;
    }
    
}
