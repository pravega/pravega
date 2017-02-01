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

import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.util.List;

/**
 * This is the mirror of EventStreamWriter but that only deals with one segment.
 */
public interface SegmentEventWriter<Type> extends AutoCloseable {

    /**
     * Writes an pending event to the segment.
     *
     * @param m The event to be written.
     * @throws SegmentSealedException if segment is sealed already.
     */
    void write(PendingEvent<Type> m) throws SegmentSealedException;

    /**
     * Blocks on all outstanding writes.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    void flush() throws SegmentSealedException;

    /**
     * Closes the Event Writer for the segment.
     *
     * @throws SegmentSealedException if the segment has already been closed.
     */
    @Override
    void close() throws SegmentSealedException;

    /**
     * Returns if the segment is already sealed or not.
     *
     * @return a boolean indicating the seal status of the segment.
     */
    boolean isAlreadySealed();

    /**
     * Gets all events that have been sent to {@link #write(PendingEvent)} but are not yet acknowledged.
     * @return List of events that are sent, but haven't acked.
     */
    List<PendingEvent<Type>> getUnackedEvents();
}
