/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

public interface AppendBatchSizeTracker {

    static final int MAX_BATCH_SIZE = WireCommands.MAX_WIRECOMMAND_SIZE / 2;
    
    /**
     * Records that an append has been sent.
     * 
     * @param eventNumber the number of the event
     * @param size the size of the event
     */
    void recordAppend(long eventNumber, int size);

    /**
     * Records that one or more events have been acked.
     *
     * @param eventNumber the number of the last event
     * @return The number of outstanding appends.
     */
    long recordAck(long eventNumber);

    /**
     * Returns the size that should be used for the next append block.
     *
     * @return Integer indicating block size that should be used for the next append.
     */
    int getAppendBlockSize();
    
    /**
     * Returns the timeout that should be used for append blocks.
     *
     * @return Integer indicating the batch timeout.
     */
    int getBatchTimeout();

}