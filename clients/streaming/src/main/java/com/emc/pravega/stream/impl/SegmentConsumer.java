/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;

/**
 * The mirror of {@link Consumer} but that is specific to a single segment.
 */
public interface SegmentConsumer<Type> extends AutoCloseable {
    Segment getSegmentId();

    /**
     * Return the next event, blocking for at most timeout.
     * If there is no event after timeout null will be returned.
     * EndOfSegmentException indicates the segment has ended an no more events may be read.
     *
     * @param timeout Timeout for the operation, in milliseconds.
     * @throws EndOfSegmentException If we reached the end of the segment.
     */
    Type getNextEvent(long timeout) throws EndOfSegmentException;

    /**
     * Returns the current offset. This can be passed to {@link #setOffset(long)} to restore to the current position.
     */
    long getOffset();

    /**
     * Given an offset obtained from {@link SegmentConsumer#getOffset()} reset consumption to that position.
     *
     * @param offset The offset to set.
     */
    void setOffset(long offset);

    /**
     * Closes the consumer. Frees any resources associated with it.
     * No further opertations may be performed.
     */
    @Override
    void close();
}
