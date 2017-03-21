/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;

public class CheckpointState {
    @GuardedBy("$lock")
    private final List<String> checkpoints = new ArrayList<>();
    /**
     * Maps CheckpointId to remaining hosts.
     */
    @GuardedBy("$lock")
    private final Map<String, List<String>> uncheckpointedHosts = new HashMap<>();
    /**
     *  Maps CheckpointId to positions in segments.
     */
    @GuardedBy("$lock")
    private final Map<String, Map<Segment, Long>> checkpointPositions = new HashMap<>();
    
    @Synchronized
    void beginNewCheckpoint(String checkpointId, Set<String> currentReaders) {
        if (!checkpointPositions.containsKey(checkpointId)) {
            uncheckpointedHosts.put(checkpointId, new ArrayList<>(currentReaders));
            checkpointPositions.put(checkpointId, new HashMap<>());
            checkpoints.add(checkpointId);
        }
    }
    
    @Synchronized
    String getCheckpointForReader(String readerName) {
        OptionalInt min = getCheckpointsForReader(readerName).stream().mapToInt(checkpoints::indexOf).min();
        if (min.isPresent()) {
            return checkpoints.get(min.getAsInt());
        } else {
            return null;
        }
    }
    
    private List<String> getCheckpointsForReader(String readerName) {
        return uncheckpointedHosts.entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains(readerName))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    @Synchronized
    void removeReader(String readerName, Map<Segment, Long> position) {
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
    boolean isCheckpointComplete(String checkpointId) {
        return !uncheckpointedHosts.containsKey(checkpointId);
    }
    
    @Synchronized
    Map<Segment, Long> getPositionsForCompletedCheckpoint(String checkpointId) {
        if (uncheckpointedHosts.containsKey(checkpointId)) {
            return null;
        }
        return checkpointPositions.get(checkpointId);
    }
    
    @Synchronized
    void clearCheckpointsThrough(String checkpointId) {
        if (checkpointPositions.containsKey(checkpointId)) {
            for (Iterator<String> iterator = checkpoints.iterator(); iterator.hasNext();) {
                String cp = iterator.next();
                uncheckpointedHosts.remove(cp);
                checkpointPositions.remove(cp);
                iterator.remove();
                if (cp.equals(checkpointId)) {
                    break;
                }
            }
        }
    }

}
