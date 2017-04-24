/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

public interface AppendBatchSizeTracker {

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
     */
    void recordAck(long eventNumber);

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