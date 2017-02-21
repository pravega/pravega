/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.server.logs.SerializationException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * No-op operation that can be used as an "operation barrier". This can be added to the Log and when it completes, the
 * caller knows that all operations up to, and including it, have been completed (successfully or not). This operation
 * cannot be serialized or recovered.
 */
public class ProbeOperation extends Operation {
    public static final byte OPERATION_TYPE = 0;

    public ProbeOperation() {
        super();
    }

    @Override
    public boolean canSerialize() {
        // We cannot (and should not) process this operation in the log. It serves no real purpose except as a control
        // op (see class-level doc).
        return false;
    }

    @Override
    protected byte getOperationType() {
        return OPERATION_TYPE;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized.");
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be deserialized.");
    }
}
