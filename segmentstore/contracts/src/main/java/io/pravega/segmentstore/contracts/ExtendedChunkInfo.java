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
package io.pravega.segmentstore.contracts;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Extended information about the chunk.
 */
@Builder
@Data
public class ExtendedChunkInfo {
    /**
     * Length of the chunk in metadata.
     */
    private volatile long lengthInMetadata;

    /**
     * Length of the chunk in storage.
     */
    private volatile long lengthInStorage;

    /**
     * startOffset of chunk in segment.
     */
    private volatile long startOffset;

    /**
     * Name of the chunk.
     */
    @NonNull
    private final String chunkName;

    /**
     * Whether chunk exists in storage.
     */
    private volatile boolean existsInStorage;
}
