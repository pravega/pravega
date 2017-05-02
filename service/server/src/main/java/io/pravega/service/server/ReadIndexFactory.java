/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server;

import io.pravega.service.storage.ReadOnlyStorage;

/**
 * Defines a Factory for ReadIndex objects.
 */
public interface ReadIndexFactory extends AutoCloseable {
    /**
     * Creates an instance of a ReadIndex class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this ReadIndex.
     * @param storage           A ReadOnlyStorage to use for reading data that is not in the cache.
     */
    ReadIndex createReadIndex(ContainerMetadata containerMetadata, ReadOnlyStorage storage);

    @Override
    void close();
}
