package com.emc.pravega.stream.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;

import lombok.Synchronized;

public class CheckpointState {
    @GuardedBy("$lock")
    private final List<String> checkpoints = new ArrayList<>();
    @GuardedBy("$lock")
    private final Map<String, List<String>> uncheckpointedHosts = new HashMap<>(); // Maps CheckpointId to remaining hosts.
    @GuardedBy("$lock")
    private final Map<String, Map<Segment, Long>> checkpointPositions = new HashMap<>(); // Maps CheckpointId to positions in segments.
    
    @Synchronized
    void beginNewCheckpoint(String checkpointId, List<String> currentReaders) {
        if (!checkpointPositions.containsKey(checkpointId)) {
            uncheckpointedHosts.put(checkpointId, new ArrayList<>(currentReaders));
            checkpointPositions.put(checkpointId, new HashMap<>());
            checkpoints.add(checkpointId);
        }
    }
    
    @Synchronized
    List<String> getCheckpointsForReader(String readerName) {
        List<String> result = new ArrayList<>(1);
        for (Entry<String, List<String>> entry : uncheckpointedHosts.entrySet()) {
            if (entry.getValue().contains(readerName)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }
    
    @Synchronized
    void readerDied(String readerName, Map<Segment, Long> position) {
        for (String checkpointId : getCheckpointsForReader(readerName)) {            
            readerCheckpointed(checkpointId, readerName, position);
        }
    }
    
    @Synchronized
    void readerCheckpointed(String checkpointId, String readerName, Map<Segment, Long> position) {
        List<String> readers = uncheckpointedHosts.get(checkpointId);
        if (readers != null) {
            boolean removed = readers.remove(readerName);
            Preconditions.checkState(removed, "Reader already checkpointed.");
            Map<Segment, Long> positions = checkpointPositions.get(checkpointId);
            positions.putAll(position);
            if (readers.isEmpty()) {
                uncheckpointedHosts.remove(checkpointId);
            }
        }
    }
    
    @Synchronized
    Boolean isCheckpointCompleted(String checkpointId) { 
        if (!checkpointPositions.containsKey(checkpointId)) {
            return null;
        }
        if (uncheckpointedHosts.containsKey(checkpointId)) {
            return false;
        } else {
            return true;
        }
    }
    
    @Synchronized
    Map<Segment, Long> getPositionsForCompletedCheckpoint(String checkpointId) {
        if (uncheckpointedHosts.containsKey(checkpointId)) {
            return null;
        }
        return checkpointPositions.get(checkpointId);
    }
    
    @Synchronized
    void clearCheckpointsBefore(String checkpointId) {
        if (checkpointPositions.containsKey(checkpointId)) {
            for (Iterator<String> iterator = checkpoints.iterator(); iterator.hasNext();) {
                String cp = iterator.next();
                if (cp == checkpointId) {
                    break;
                }
                uncheckpointedHosts.remove(cp);
                checkpointPositions.remove(cp);
                iterator.remove();
            }
        }
    }

}
