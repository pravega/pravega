/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;

/**
 * Allows for reading from a Segment asynchronously.
 */
abstract class AsyncSegmentInputStream implements AutoCloseable {
    
    public abstract CompletableFuture<StreamSegmentInfo> getSegmentInfo();

    public interface ReadFuture {
        /**
         * @return True if the read has completed and is successful.
         */
        boolean isSuccess();
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