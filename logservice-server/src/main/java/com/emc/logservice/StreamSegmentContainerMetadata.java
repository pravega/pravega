package com.emc.logservice;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata for a Stream Segment Container.
 */
public class StreamSegmentContainerMetadata implements StreamSegmentMetadataSource
{
    //region Members

    public static final long NoStreamSegmentId = Long.MIN_VALUE;
    private final AtomicLong sequenceNumber;
    private final AbstractMap<String, Long> streamSegmentIds;
    private final AbstractMap<Long, StreamSegmentMetadata> streamMetadata;
    private final AbstractMap<Long, TruncationMarker> truncationMarkers;
    private final AtomicBoolean recoveryMode;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerMetadata.
     */
    public StreamSegmentContainerMetadata()
    {
        //TODO: need to define a MetadataReaderWriter class which we can pass to this. Metadata always need to be persisted somewhere.
        this.sequenceNumber = new AtomicLong();
        this.streamSegmentIds = new ConcurrentHashMap<>();
        this.streamMetadata = new ConcurrentHashMap<>();
        this.truncationMarkers = new ConcurrentHashMap<>();
        this.recoveryMode = new AtomicBoolean();
    }

    //endregion

    //region Metadata Operations

    /**
     * Puts the StreamSegmentMetadata into Recovery Mode. Recovery Mode indicates that the Metadata is about to be
     * regenerated from various sources, and is not yet ready for normal operation.
     * <p>
     * If the Metadata is in Recovery Mode, some operations may not be executed, while others are allowed to. Inspect
     * the documentation for each method to find the behavior of each.
     *
     * @throws IllegalStateException If the Metadata is already in Recovery Mode.
     */
    public void enterRecoveryMode()
    {
        ensureNonRecoveryMode();
        this.recoveryMode.set(true);
    }

    /**
     * Takes the StreamSegmentMetadata out of Recovery Mode.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    public void exitRecoveryMode()
    {
        ensureRecoveryMode();
        this.recoveryMode.set(false);
    }

    /**
     * Gets a value indicating whether we are currently in Recovery Mode.
     *
     * @return
     */
    public boolean isRecoveryMode()
    {
        return this.recoveryMode.get();
    }

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return The next available Operation Sequence Number.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    public long getNewOperationSequenceNumber()
    {
        ensureNonRecoveryMode();
        return this.sequenceNumber.incrementAndGet();
    }

    /**
     * Sets the current Operation Sequence Number.
     *
     * @param value The new Operation Sequence Number.
     * @throws IllegalStateException    If the Metadata is not in Recovery Mode.
     * @throws IllegalArgumentException If the new Sequence Number is not greater than the previous one.
     */
    public void setOperationSequenceNumber(long value)
    {
        ensureRecoveryMode();

        // Note: This check-and-set is not atomic, but in recovery mode we are executing in a single thread, so this is ok.
        if (value < this.sequenceNumber.get())
        {
            throw new IllegalArgumentException(String.format("Invalid SequenceNumber. Expecting greater than %d.", this.sequenceNumber.get()));
        }

        this.sequenceNumber.set(value);
    }

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @return The Id of the StreamSegment, or NoStreamSegmentId if the Metadata has no knowledge of it.
     */
    public long getStreamSegmentId(String streamSegmentName)
    {
        return this.streamSegmentIds.getOrDefault(streamSegmentName, NoStreamSegmentId);
    }

    /**
     * Maps a new StreamSegment Name to its assigned Id.
     *
     * @param streamSegmentName The case-sensitive name of the StreamSegment to map.
     * @param streamSegmentId   The Id of the StreamSegment.
     */
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId)
    {
        // TODO: atomically check that the values aren't already mapped.
        this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
        this.streamMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId));
    }

    /**
     * Maps a new StreamSegment to its Parent StreamSegment.
     * This is used for batches that are dependent on their parent StreamSegments.
     *
     * @param streamSegmentName     The case-sensitive name of the StreamSegment to map.
     * @param streamSegmentId       The Id of the StreamSegment to map.
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @throws IllegalArgumentException If the parentStreamSegmentId refers to an unknown StreamSegment.
     */
    public void mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId)
    {
        if (!this.streamMetadata.containsKey(parentStreamSegmentId))
        {
            throw new IllegalArgumentException("Invalid Parent Stream Id.");
        }

        // TODO: atomically check that the values aren't already mapped.
        this.streamSegmentIds.put(streamSegmentName, streamSegmentId);
        this.streamMetadata.put(streamSegmentId, new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentStreamSegmentId));
    }

    /**
     * Gets Metadata for a particular StreamSegment.
     *
     * @param streamSegmentId The Id of the StreamSegment to get metadata for.
     * @return The StreamSegmentMetadata, or null if the given StreamSegment is unknown.
     */
    public StreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId)
    {
        return this.streamMetadata.getOrDefault(streamSegmentId, null);
    }

    /**
     * Records a new Truncation Marker in the metadata.
     * A Truncation Marker is a particular position in the Log where we can execute truncation operations.
     *
     * @param tm The Truncation Marker to set.
     */
    public void recordTruncationMarker(TruncationMarker tm)
    {
        this.truncationMarkers.put(tm.getOperationSequenceNumber(), tm);
    }

    /**
     * Removes all truncation markers up to, and including the given Operation Sequence Number.
     *
     * @param upToOperationSequenceNumber The Operation Sequence Number to remove Truncation Markers up to.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    public void removeTruncationMarkers(long upToOperationSequenceNumber)
    {
        ensureNonRecoveryMode();
        ArrayList<Long> toRemove = new ArrayList<>();
        this.truncationMarkers.keySet().forEach(key ->
        {
            if (key <= upToOperationSequenceNumber)
            {
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
    public TruncationMarker getClosestTruncationMarker(long operationSequenceNumber)
    {
        ensureNonRecoveryMode();

        //TODO: make more efficient, maybe by using a different data structure, like TreeMap.
        TruncationMarker result = null;
        for (TruncationMarker tm : this.truncationMarkers.values())
        {
            long seqNo = tm.getOperationSequenceNumber();
            if (seqNo == operationSequenceNumber)
            {
                // Found the best result.
                return tm;
            }
            else if (seqNo < operationSequenceNumber)
            {
                if (result == null || (result.getOperationSequenceNumber() < seqNo))
                {
                    // We found a better result.
                    result = tm;
                }
            }
        }

        return result;
    }

    /**
     * Resets the entire StreamSegmentMetadata by clearing out all data it has.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    public void reset()
    {
        ensureRecoveryMode();
        this.sequenceNumber.set(0);
        this.streamSegmentIds.clear();
        this.streamMetadata.clear();
        this.truncationMarkers.clear();
    }

    private void ensureRecoveryMode()
    {
        if (!isRecoveryMode())
        {
            throw new IllegalStateException("StreamSegmentContainerMetadata is not in recovery mode. Cannot execute this operation.");
        }
    }

    private void ensureNonRecoveryMode()
    {
        if (isRecoveryMode())
        {
            throw new IllegalStateException("StreamSegmentContainerMetadata is in recovery mode. Cannot execute this operation.");
        }
    }

    //endregion
}
