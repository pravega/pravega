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
package io.pravega.controller.store.checkpoint;

import io.pravega.client.stream.Position;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Store maintains the following two mappings
 * 1. ProcessId -{@literal >} ReaderGroupName, which maps ProcessId to all the reader groups within it.
 * 2. (ProcessId, ReaderGroupName) -{@literal >} List (readerId, position), which maps the pair
 * (ProcessId, ReaderGroupName) to the readers participating in the reader group and running
 * within the process.
 *
 * When a ReaderGroup is added to the store, it is in ACTIVE state. Readers can be added to the
 * ReaderGroup in this state. Subsequently, ReaderGroup may transition to SEALED state by
 * invocation of sealReaderGroup API. Once sealed, no more readers can be added to the ReaderGroup.
 */
public interface CheckpointStore {

    /**
     * Store position of the specified reader that belongs the the specified readerGroup and created
     * in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     * @param position Position of reader in the stream.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void setPosition(final String process, final String readerGroup, final String readerId, final Position position)
            throws CheckpointStoreException;

    /**
     * Returns the map of readers to their respective positions running in the specified
     * process and participating in the specified reader group.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @return Map of readers to their respective positions.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    Map<String, Position> getPositions(final String process, final String readerGroup)
            throws CheckpointStoreException;

    /**
     * Add a new entry (process, readerGroup) to the map. This readerGroup entry is in ACTIVE
     * state,which means readers can be added to this readerGroup in the specified process.
     * @param process Process identifier.
     * @param readerGroup ReaderGroup identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void addReaderGroup(final String process, final String readerGroup)
            throws CheckpointStoreException;

    /**
     * Seals the readerGroup in the specified process. Once a readerGroup is sealed, no more readers can be added to it.
     * @param process Process identifier.
     * @param readerGroup ReaderGroup identifier.
     * @return Map of readers to their respective positions in the specified readerGroup.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    Map<String, Position> sealReaderGroup(final String process, final String readerGroup)
            throws CheckpointStoreException;

    /**
     * Removes the specified readerGroup from specified process if
     * (1) it has no active readers, and
     * (2) it is in SEALED state.
     * If these preconditions do not hold, a CheckpointStoreException shall be thrown with type field set to
     * NodeNotEmpty and Active, respectively.
     *
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void removeReaderGroup(final String process, final String readerGroup)
            throws CheckpointStoreException;

    /**
     * List all the reader groups added to a specified process.
     * @param process Process identifier.
     * @return List of reader groups added to the specified process.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    List<String> getReaderGroups(final String process) throws CheckpointStoreException;

    /**
     * Adds the specified reader in the specified reader group in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void addReader(final String process, final String readerGroup, final String readerId)
            throws CheckpointStoreException;

    /**
     * Removes the specified reader in the specified reader group in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void removeReader(final String process, final String readerGroup, final String readerId)
            throws CheckpointStoreException;

    Set<String> getProcesses() throws CheckpointStoreException;

    /**
     * Get the health status.
     *
     * @return true/false.
     */
    public boolean isHealthy();
}
