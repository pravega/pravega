/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.SegmentProperties;

import java.util.Collection;
import java.util.UUID;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     */
    long getParentId();

    /**
     * Gets a value indicating the id of the Container this StreamSegment belongs to.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether this StreamSegment has been merged into another.
     */
    boolean isMerged();

    /**
     * Gets a value indicating whether this StreamSegment has been sealed in Storage.
     * This is different from isSealed(), which returns true if the StreamSegment has been sealed in DurableLog or in Storage.
     */
    boolean isSealedInStorage();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     */
    long getDurableLogLength();

    /**
     * Gets the Append Context for the Last Committed Append related to the given client.
     * Note that this may not be available for appends that occurred long in the past (this data is not persisted with
     * the metadata).
     *
     * @param clientId The Client Id to inquire for.
     */
    AppendContext getLastAppendContext(UUID clientId);

    /**
     * Gets a collection of all known Client Ids (mapped to AppendContexts).
     */
    Collection<UUID> getKnownClientIds();
}
