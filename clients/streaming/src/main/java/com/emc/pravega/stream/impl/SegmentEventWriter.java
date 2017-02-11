/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.util.List;

/**
 * This is the mirror of EventStreamWriter but that only deals with one segment.
 */
public interface SegmentEventWriter<Type> extends AutoCloseable {
    void write(PendingEvent<Type> m) throws SegmentSealedException;

    /**
     * Blocks on all outstanding writes.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    void flush() throws SegmentSealedException;

    @Override
    void close() throws SegmentSealedException;

    boolean isAlreadySealed();

    /**
     * Gets all events that have been sent to {@link #write(PendingEvent)} but are not yet acknowledged.
     */
    List<PendingEvent<Type>> getUnackedEvents();
}
