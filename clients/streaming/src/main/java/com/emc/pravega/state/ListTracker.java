package com.emc.pravega.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

public class ListTracker<T> {

    private static class UpdatableList<T> implements Updatable<ListUpdate<T>>, Serializable {
        private final List<T> impl = new ArrayList<>();
        private Revision currentRevision;
        
        @Synchronized
        private List<T> getCurrentValues(){
            return new ArrayList<>(impl);
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
        public void applyUpdate(Revision newRevision, ListUpdate<T> update) {
            update.process(this);
            currentRevision = newRevision;
        }

        @Override
        @Synchronized
        public Revision getCurrentRevision() {
            return currentRevision;
        }
    }
    
    private static abstract class ListUpdate<T> implements Serializable {
        public abstract void process(UpdatableList<T> updatableList);
    }
    
    @RequiredArgsConstructor
    private static class AddToList<T> extends ListUpdate<T> {
        private final T value;
        @Override
        public void process(UpdatableList<T> updatableList) {
            updatableList.add(value);
        }
    }
    @RequiredArgsConstructor
    private static class RemoveFromList<T> extends ListUpdate<T> {
        private final T value;
        @Override
        public void process(UpdatableList<T> updatableList) {
            updatableList.remove(value);
        }
    }
    @RequiredArgsConstructor
    private static class ClearList<T> extends ListUpdate<T> {
        @Override
        public void process(UpdatableList<T> updatableList) {
            updatableList.clear();
        }
    }
    
    private static final int REMOVALS_BEFORE_COMPACTION = 5;
    
    private final StateTracker<UpdatableList<T>, ListUpdate<T>> tracker;
    private UpdatableList<T> current;
    private int countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
    
    public ListTracker(StateTracker<UpdatableList<T>, ListUpdate<T>> tracker) {
        this.tracker = tracker;
        getNewBaseVersion();
        update();
    }

    private void getNewBaseVersion() {
        current = tracker.getInitialState();
    }

    public void update() {
        while(!tracker.updateLocalState(current)) {
            getNewBaseVersion();
        }
    }
    
    public List<T> getCurrentValues(){
        return current.getCurrentValues();
    }
    
    public int getCurrentSize() {
        return current.getCurrentSize();
    }
    
    public boolean attemptAdd(T value) {
        return tracker.attemptUpdate(current, new AddToList<>(value));
    }
    
    public boolean attemptRemove(T value) {
        boolean result = tracker.attemptUpdate(current, new RemoveFromList<>(value));
        if (result) {
            countdownToCompaction--;
            if (countdownToCompaction <=0) {
                tracker.compact(current);
                countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
            }
        }
        return result;
    }
    
    public boolean attemptClear() {
        boolean result = tracker.attemptUpdate(current, new ClearList<>());
        if (result) {
            tracker.compact(current);
            countdownToCompaction = REMOVALS_BEFORE_COMPACTION;
        }
        return result;
    }
    
}
