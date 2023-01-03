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
package io.pravega.shared.protocol.netty;

/**
 * A class that handles each type of Admin-specific Request.
 */
public interface AdminRequestProcessor extends RequestProcessor {

    /**
     * Call to flush a particular container to storage.
     *
     * @param flushToStorage {@link WireCommand} call for flush a container to storage.
     */
    void flushToStorage(WireCommands.FlushToStorage flushToStorage);

    /**
     * Call to list the storage chunks for the given segment name.
     *
     * @param listStorageChunks {@link WireCommand} call to list storage chunks for the given segment.
     */
    void listStorageChunks(WireCommands.ListStorageChunks listStorageChunks);
}
