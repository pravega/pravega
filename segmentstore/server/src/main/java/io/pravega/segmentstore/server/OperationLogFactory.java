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
package io.pravega.segmentstore.server;

/**
 * Defines a Factory for DurableLog Components.
 */
public interface OperationLogFactory {

    /**
     * Creates a new instance of an OperationLog class.
     *
     * @param containerMetadata The Metadata for the create the DurableLog for.
     * @param readIndex         A ReadIndex that can be used to store new appends in.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the metadata is already in recovery mode.
     * @return The OperationLog instance.
     */
    OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, ReadIndex readIndex);
}
