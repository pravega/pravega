/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service;

import io.pravega.server.segmentstore.storage.LogAddress;

/**
 * Defines a repository for Truncation Markers.
 * Truncation Markers are mappings (of Operation Sequence Numbers to DataFrame Sequence Numbers) where the DurableDataLog
 * can be truncated.
 */
public interface TruncationMarkerRepository {
    /**
     * Records a new Truncation Marker in the metadata.
     * A Truncation Marker is a particular position in the Log where we can execute truncation operations.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param address                 The LogAddress of the corresponding Data Frame that can be truncated (up to, and including).
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    void recordTruncationMarker(long operationSequenceNumber, LogAddress address);

    /**
     * Removes all truncation markers up to, and including the given Operation Sequence Number.
     *
     * @param upToOperationSequenceNumber The Operation Sequence Number to remove Truncation Markers up to.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    void removeTruncationMarkers(long upToOperationSequenceNumber);

    /**
     * Gets the closest Truncation Marker to the given Operation Sequence Number that does not exceed it.
     *
     * @param operationSequenceNumber The Operation Sequence Number to query.
     * @return The requested Truncation Marker, or null if no such marker exists.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    LogAddress getClosestTruncationMarker(long operationSequenceNumber);

    /**
     * Records the fact that the given Operation Sequence Number is a valid Truncation Point. We can only truncate on
     * valid Sequence Numbers. For instance, a valid Truncation Point refers to a MetadataCheckpointOperation, which
     * is a good place to start a recovery from.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     */
    void setValidTruncationPoint(long operationSequenceNumber);

    /**
     * Gets a value indicating whether the given Operation Sequence Number is a valid Truncation Point, as set by
     * setValidTruncationPoint().
     *
     * @param operationSequenceNumber The Sequence number to query.
     */
    boolean isValidTruncationPoint(long operationSequenceNumber);

    /**
     * Gets a value representing the highest Truncation Point that is smaller than or equal to the given Sequence Number.
     *
     * @param operationSequenceNumber The Sequence number to query.
     */
    long getClosestValidTruncationPoint(long operationSequenceNumber);
}
