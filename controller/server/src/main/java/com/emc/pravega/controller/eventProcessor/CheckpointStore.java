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

import com.emc.pravega.stream.Position;

import java.util.List;
import java.util.Map;

/**
 * Store to mapping (readerId, position) of readers running in a process and participating
 * in a reader group. Schema of each entry in the map is as follows.
 * (ProcessId, ReaderGroupName) -> Map (readerId, position)
 */
public interface CheckpointStore {

    /**
     * Store position of the specified reader that belongs the the specified readerGroup and created
     * in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     * @param position Position of reader in the stream.
     */
    void setPosition(final String process, final String readerGroup, final String readerId, final Position position);

    /**
     * Returns the map of readers to their respective positions running in the specified
     * process and participating in the specified reader group.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @return Map of readers to their respective positions.
     */
    Map<String, Position> getPositions(final String process, final String readerGroup);

    /**
     * Add a new entry (process, readerGroup) to the map. This readerGroup entry is in ACTIVE
     * state,which means readers can be added to this readerGroup in the specified process.
     * @param process Process identifier.
     * @param readerGroup ReaderGroup identifier.
     */
    void addReaderGroup(final String process, final String readerGroup);

    /**
     * Seals the readerGroup in the specified process. Once a readerGroup is sealed, no more readers can be added to it.
     * @param process Process identifier.
     * @param readerGroup ReaderGroup identifier.
     * @return Map of readers to their respective positions in the specified readerGroup.
     */
    Map<String, Position> sealReaderGroup(final String process, final String readerGroup);

    /**
     * Removes the specified readerGroup from specified process if
     * (1) it has no active readers, and
     * (2) it is in SEALED state.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     */
    void removeReaderGroup(final String process, final String readerGroup);

    /**
     * List all the reader groups added to a specified process.
     * @param process Process identifier.
     * @return List of reader groups added to the specified process.
     */
    List<String> getReaderGroups(final String process);

    /**
     * Adds the specified reader in the specified reader group in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     */
    void addReader(final String process, final String readerGroup, final String readerId);

    /**
     * Removes the specified reader in the specified reader group in the specified process.
     * @param process Process identifier.
     * @param readerGroup Reader group name.
     * @param readerId Reader identifier.
     */
    void removeReader(final String process, final String readerGroup, final String readerId);
}
