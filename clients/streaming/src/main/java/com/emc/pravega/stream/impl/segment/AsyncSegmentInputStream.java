/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.stream.Segment;

import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Allows for reading from a Segment asynchronously.
 */
@RequiredArgsConstructor
abstract class AsyncSegmentInputStream implements AutoCloseable {
    @Getter
    protected final Segment segmentId;
    
    public abstract CompletableFuture<StreamSegmentInfo> getSegmentInfo();

    public interface ReadFuture {
        /**
         * @return True if the read has completed and is successful.
         */
        boolean isSuccess();
        
        /**
         * Waits for the provided future to be complete and returns true if it completed and false if it did not.
         * 
         * @param timeout The maximum number of milliseconds to block
         */
        boolean await(long timeout);
    }
    
    /**
     * Given an ongoing read request, blocks on its completion and returns its result.
     */
    public abstract SegmentRead getResult(ReadFuture ongoingRead);
    
    /**
     * Reads from the Segment at the specified offset asynchronously.
     * 
     * 
     * @param offset The offset in the segment to read from
     * @param length The suggested number of bytes to read. (Note the result may contain either more or less than this
     *            value.)
     * @return A future for the result of the read call. The result can be obtained by calling {@link #getResult(ReadFuture)}
     */
    public abstract ReadFuture read(long offset, int length);

    @Override
    public abstract void close();
}