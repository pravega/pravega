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
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Object representing the argument to the concat operation.
 */
@Builder
@Data
final public class ConcatArgument {
    /**
     * Length of the chunk.
     */
    private final long length;

    /**
     * Name of the chunk.
     */
    @NonNull
    private final String name;

    /**
     * Creates ConcatArgument instance from {@link ChunkInfo}.
     *
     * @param chunkInfo ChunkInfo object to copy from.
     * @return New ConcatArgument object.
     */
    public static ConcatArgument fromChunkInfo(ChunkInfo chunkInfo) {
        return ConcatArgument.builder().length(chunkInfo.getLength()).name(chunkInfo.getName()).build();
    }
}
