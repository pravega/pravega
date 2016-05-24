package com.emc.logservice.server.containers;

import com.emc.logservice.server.RecoverableMetadata;
import com.emc.logservice.server.SegmentMetadataCollection;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metadata for Truncation Markers.
 */
public class TruncationMarkerCollection implements RecoverableMetadata {

    //region Members

    private final AbstractMap<Long, Long> truncationMarkers;
    private final AtomicBoolean recoveryMode;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TruncationMarkerCollection class.
     */
    public TruncationMarkerCollection() {
        this.truncationMarkers = new ConcurrentHashMap<>();
        this.recoveryMode = new AtomicBoolean(false);
    }

    //endregion

    //region Truncation Marker management

    /**
     * Records a new Truncation Marker in the metadata.
     * A Truncation Marker is a particular position in the Log where we can execute truncation operations.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param dataFrameSequenceNumber The Sequence Number of the corresponding Data Frame that can be truncated (up to, and including).
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    public void recordTruncationMarker(long operationSequenceNumber, long dataFrameSequenceNumber) {
        if (operationSequenceNumber == SegmentMetadataCollection.NoStreamSegmentId) {
            throw new IllegalArgumentException("operationSequenceNumber is invalid.");
        }

        if (dataFrameSequenceNumber == SegmentMetadataCollection.NoStreamSegmentId) {
            throw new IllegalArgumentException("dataFrameSequenceNumber is invalid.");
        }

        this.truncationMarkers.put(operationSequenceNumber, dataFrameSequenceNumber);
    }

    /**
     * Removes all truncation markers up to, and including the given Operation Sequence Number.
     *
     * @param upToOperationSequenceNumber The Operation Sequence Number to remove Truncation Markers up to.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    public void removeTruncationMarkers(long upToOperationSequenceNumber) {
        ensureNonRecoveryMode();
        ArrayList<Long> toRemove = new ArrayList<>();
        this.truncationMarkers.keySet().forEach(key ->
        {
            if (key <= upToOperationSequenceNumber) {
                toRemove.add(key);
            }
        });

        toRemove.forEach(this.truncationMarkers::remove);
    }

    /**
     * Gets the closest Truncation Marker to the given Operation Sequence Number that does not exceed it.
     *
     * @param operationSequenceNumber The Operation Sequence Number to query.
     * @return The requested Truncation Marker, or null if no such marker exists.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    public long getClosestTruncationMarker(long operationSequenceNumber) {
        ensureNonRecoveryMode();

        //TODO: make more efficient, maybe by using a different data structure, like TreeMap.
        Map.Entry<Long, Long> result = null;
        for (Map.Entry<Long, Long> tm : this.truncationMarkers.entrySet()) {
            long seqNo = tm.getKey();
            if (seqNo == operationSequenceNumber) {
                // Found the best result.
                return tm.getValue();
            }
            else if (seqNo < operationSequenceNumber) {
                if (result == null || (result.getKey() < seqNo)) {
                    // We found a better result.
                    result = tm;
                }
            }
        }

        return result.getValue();
    }

    //endregion

    //region RecoverableMetadata Implementation

    @Override
    public void enterRecoveryMode() {
        ensureNonRecoveryMode();
        this.recoveryMode.set(true);
    }

    @Override
    public void exitRecoveryMode() {
        ensureRecoveryMode();
        this.recoveryMode.set(false);
    }

    @Override
    public void reset() {
        ensureRecoveryMode();
        this.truncationMarkers.clear();
    }

    private void ensureRecoveryMode() {
        if (!this.recoveryMode.get()) {
            throw new IllegalStateException("TruncationMarkerCollection is not in recovery mode. Cannot execute this operation.");
        }
    }

    private void ensureNonRecoveryMode() {
        if (this.recoveryMode.get()) {
            throw new IllegalStateException("TruncationMarkerCollection is in recovery mode. Cannot execute this operation.");
        }
    }

    //endregion
}
