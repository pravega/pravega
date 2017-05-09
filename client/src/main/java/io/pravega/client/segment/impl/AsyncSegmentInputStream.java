/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;

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