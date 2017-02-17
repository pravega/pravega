/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

import lombok.Data;

import java.util.UUID;

/**
 * Externally-specified Append Context.
 */
@Data
public class AppendContext {
    private final UUID clientId;
    private final long eventNumber;

    /**
     * Creates a new instance of the AppendContext class.
     *
     * @param clientId    The Unique Id of the client this append was received from.
     * @param eventNumber The client-assigned event number within the client context.
     */
    public AppendContext(UUID clientId, long eventNumber) {
        this.clientId = clientId;
        this.eventNumber = eventNumber;
    }

    @Override
    public String toString() {
        return String.format("ClientId = %s, eventNumber = %d", getClientId().toString(), getEventNumber());
    }
}
