/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.stream.impl.CheckpointImpl;
import java.nio.ByteBuffer;

public interface Checkpoint {

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
    
    /**
     * Serializes the checkpoint to a compact byte array.
     * @return A serialized version of this checkpoint.
     */
    ByteBuffer toBytes();
    
    
    /**
     * Deserializes the checkpoint from its serialized from obtained from calling {@link #toBytes()}.
     * 
     * @param serializedCheckpoint A serialized checkpoint.
     * @return The checkpoint object.
     */
    static Checkpoint fromBytes(ByteBuffer serializedCheckpoint) {
        return CheckpointImpl.fromBytes(serializedCheckpoint);
    }
}
