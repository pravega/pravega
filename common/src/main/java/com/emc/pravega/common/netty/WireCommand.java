/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.netty;

import java.io.DataOutput;
import java.io.IOException;

/**
 * The interface for all things that go over the Wire. (This is the basic element of the network protocol)
 */
public interface WireCommand {
    WireCommandType getType();

    void writeFields(DataOutput out) throws IOException;
}
