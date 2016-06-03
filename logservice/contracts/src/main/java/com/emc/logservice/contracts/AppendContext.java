package com.emc.logservice.contracts;

import java.util.UUID;

/**
 * Externally-specified Append Context.
 */
public class AppendContext {
    private final UUID clientId;
    private final long clientOffset;

    /**
     * Creates a new instance of the AppendContext class.
     *
     * @param clientId     The Unique Id of the client this append was received from.
     * @param clientOffset The append offset within the client context.
     */
    public AppendContext(UUID clientId, long clientOffset) {
        this.clientId = clientId;
        this.clientOffset = clientOffset;
    }

    /**
     * Gets a value indicating the Unique Id of the client.
     *
     * @return
     */
    public UUID getClientId() {
        return this.clientId;
    }

    /**
     * Gets a value indicating the append offset within the client context.
     *
     * @return
     */
    public long getClientOffset() {
        return this.clientOffset;
    }

    @Override
    public String toString() {
        return String.format("ClientId = %s, Offset = %d", getClientId().toString(), getClientOffset());
    }
}
