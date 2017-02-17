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
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.RebalancerUtils;

/**
 * An event read from a stream.
 * 
 * @param <T> The type of the event.
 */
public interface EventRead<T> {
    
    /**
     * Returns a sequence that events are ordered by. This can be used to align events from
     * different readers.
     * 
     * Sequences returned have the guarantee that it will always increase for consecutive calls to
     * {@link EventStreamReader#readNextEvent(long)} unless the previous call set
     * {@link #isRoutingRebalance()} to true. (When rebalancing occurs on a reader in a
     * {@link ReaderGroup} this reader could have acquired more segments of the stream from another
     * reader that was behind) If all of the readers in a group have passed a certain value, it is
     * guaranteed that no event will ever be read below that level.
     *
     * The highOrder value of the sequence is approximately the time the event was written to the
     * stream. The sequence has nothing to do with any sequence passed to
     * {@link EventStreamWriter#writeEvent(String, Object)}
     *
     * The value will be populated on all calls to {@link EventStreamReader#readNextEvent(long)}
     * even if {@link #getEvent()} is null because no events are available. The value will continue
     * to increase even when no events are available. This is useful as it can bound the values
     * associated with future events.
     */
    Sequence getEventSequence();

    /**
     * Returns the event itself.
     */
    T getEvent();

    /**
     * The position in the stream that represents where the reader is immediately following this
     * event. It is useful to store this so that
     * {@link ReaderGroup#readerOffline(String, Position)} can be called if the reader dies.
     */
    Position getPosition();

    /**
     * Returns the pointer object for the event read. The event pointer enables a random read of the
     * event at a future time.
     */
    EventPointer getEventPointer();

    /**
     * Returns a boolean indicating if a rebalance of which events are being routed to which readers
     * is about to or needs to occur.
     * 
     * For a reader that is part of a {@link ReaderGroup} this means the next call to
     * {@link EventStreamReader#readNextEvent(long)} this reader may receive events from a different
     * set of RoutingKeys. Once this reader calls {@link EventStreamReader#readNextEvent(long)} some
     * of the routing keys it was previously reading may be taken over by another reader. This is
     * relevant if the reader processes events in a way that involves holding state in memory. For
     * example an app that is maintaining counters in memory for different types of events should
     * interpret this boolean as an indication that it should flush its values to external storage,
     * as the set routing keys it is working with are about to change.
     * 
     * For a reader that is not part of a {@link ReaderGroup} and is rebalanced manually, this means
     * the application should call {@link RebalancerUtils#rebalance(java.util.Collection, int)} with
     * the positions of its readers and recreate new readers from the resulting positions.
     * 
     * It is the goal of the implementation to not set this to true unless it is required.
     */
    boolean isRoutingRebalance();
}
