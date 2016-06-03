package com.emc.logservice.server;

import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.SegmentProperties;

import java.util.Collection;
import java.util.UUID;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     *
     * @return
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     *
     * @return
     */
    long getParentId();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     *
     * @return
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     *
     * @return
     */
    long getDurableLogLength();

    /**
     * Gets the Append Context for the Last Committed Append related to the given client.
     * Note that this may not be available for appends that occurred long in the past (this data is not persisted with
     * the metadata).
     * @param clientId The Client Id to inquire for.
     * @return
     */
    AppendContext getLastAppendContext(UUID clientId);

    /**
     * Gets a collection of all known Client Ids (mapped to AppendContexts).
     *
     * @return
     */
    Collection<UUID> getKnownClientIds();
}
