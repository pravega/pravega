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

import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.storage.Storage;
import java.util.Collection;

/**
 * Defines a Factory for Writers.
 */
public interface WriterFactory {
    /**
     * Creates a new Writer with given arguments.
     *
     * @param containerMetadata Metadata for the container that this Writer will be for.
     * @param operationLog      The OperationLog to attach to.
     * @param readIndex         The ReadIndex to attach to (to provide feedback for mergers).
     * @param attributeIndex    The ContainerAttributeIndex to attach to (to durably store Extended Attributes for
     *                          processed appends).
     * @param storage           The Storage adapter to use.
     * @param createProcessors  A Function that, when invoked with a Segment's Metadata, will create any additional
     *                          WriterSegmentProcessors for that Segment.
     * @return An instance of a class that implements the Writer interface.
     */
    Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex,
                        ContainerAttributeIndex attributeIndex, Storage storage, CreateProcessors createProcessors);

    @FunctionalInterface
    interface CreateProcessors {
        /**
         * Instantiates the WriterSegmentProcessors for the segment represented by the given metadata.
         *
         * @param segmentMetadata The UpdateableSegmentMetadata for the segment for which to create processors.
         * @return A collection containing new instances of all needed WriterSegmentProcessors.
         */
        Collection<WriterSegmentProcessor> apply(UpdateableSegmentMetadata segmentMetadata);
    }
}
