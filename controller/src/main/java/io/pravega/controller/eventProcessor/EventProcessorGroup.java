/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.eventProcessor;

import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.EventStreamWriter;
import com.google.common.util.concurrent.Service;

import java.util.Set;

/**
 * EventProcessor group interface. It provides mechanism to manage event
 * processors processing events from a Pravega Stream by participating in
 * the same ReaderGroup.
 */
public interface EventProcessorGroup<T extends ControllerEvent> extends Service, AutoCloseable {

    /**
     * Notifies Pravega ReaderGroup about failure of a process
     * participating in the Reader Group.
     * @param process Failed process's identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void notifyProcessFailure(String process) throws CheckpointStoreException;

    /**
     * Returns a reference to its writer.
     * @return writer reference of this event processor group.
     */
    EventStreamWriter<T> getWriter();

    /**
     * Gets the list of processes participating in the logical EventProcessorGroup.
     * @return list of processes participating in the Logical EventProcessorGroup.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    Set<String> getProcesses() throws CheckpointStoreException;
}
