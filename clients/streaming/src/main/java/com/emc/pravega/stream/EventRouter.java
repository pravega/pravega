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

/**
 * A class that determines to which segment an event associated with a routing key will go.
 * This is invoked on every publish call to decide how to send a particular segment.
 * It is acceptable for an event router to cache the current set of segments for a stream, as it will be queried again
 * if a segment has been sealed.
 */
public interface EventRouter {

    /**
     * Selects which segment an event should be published to.
     *
     * @param routingKey The key that should be used to select from the segment that the event should go to.
     * @return The Segment that has been selected.
     */
    Segment getSegmentForEvent(String routingKey);

    void refreshSegmentList();

}
