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

package com.emc.pravega.stream;

/**
 * Event object returned by a getNextEvent call. The
 * event object includes the event itself and some
 * additional metadata. Currently, the additional metadata
 * is a pointer to the position of the event in the
 * stream.
 *
 * @param <T>
 */
public interface Event<T> {
    /**
     * Get the event.
     *
     * @return the event
     */
    T getEvent();

    /**
     * Get the pointer object of the event. The pointer object
     * is an opaque object containing the segment identifier
     * and the offset within the segment of the event.
     *
     * @return
     */
    EventPointer getPointer();
}
