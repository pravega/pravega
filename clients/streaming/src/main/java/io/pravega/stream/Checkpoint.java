/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

import io.pravega.stream.impl.CheckpointImpl;

import java.io.Serializable;

public interface Checkpoint extends Serializable {

    /**
     * Returns the name of the Checkpoint specified in {@link ReaderGroup#initiateCheckpoint(String, java.util.concurrent.ScheduledExecutorService)}.
     * @return The checkpoint name;
     */
    String getName();
    
    /**
     * For internal use. Do not call.
     * @return This
     */
    CheckpointImpl asImpl();
}
