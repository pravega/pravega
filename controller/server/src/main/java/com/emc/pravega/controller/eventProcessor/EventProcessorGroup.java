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
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.stream.EventStreamWriter;

import java.util.Set;

/**
 * ActorGroup Interface. It provides mechanism to manage Actors
 * processing events from a Pravega Stream by participating in
 * the same Pravega ReaderGroup.
 */
public interface EventProcessorGroup<T extends StreamEvent> {

    /**
     * Notifies Pravega ReaderGroup about failure of a host
     * participating in the Reader Group.
     * @param host Failed host.
     */
    void notifyHostFailure(String host);

    /**
     * Increase/decrease the number of Actors reading from the Pravega
     * Stream and participating in the ReaderGroup. This method may be
     * invoked if the number of active segments in the Pravega Stream
     * increases/decreased on account of a Scale event due to increased/
     * decreased event throughput.
     * @param count Number of Actors to add. Negative number indicates
     *              decreasing the Actor count.
     */
    void changeEventProcessorCount(int count);

    /**
     * Returns a reference to self.
     * @return ActorGroupRef of this actor group.
     */
    EventStreamWriter<T> getSelf();

    /**
     * Gets the list of physical representations of processes participating in
     * the logical EventProcessorGroup.
     * @returns list of physical representations of processes participating
     *          in the Logical EventProcessorGroup.
     */
    Set<String> getHosts();

    /**
     * Initiates stop on all event processors in this group and waits for
     * their termination.
     */
    void stopAll();
}
