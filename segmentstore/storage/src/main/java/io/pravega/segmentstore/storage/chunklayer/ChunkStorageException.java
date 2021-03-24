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

import lombok.Getter;

import java.io.IOException;

/**
 * Generic exception related to chunk storage operations.
 */
public class ChunkStorageException extends IOException {
    @Getter
    private final String chunkName;

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     */
    public ChunkStorageException(String chunkName, String message) {
        super(message);
        this.chunkName = chunkName;
    }

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     * @param cause     The causing exception.
     */
    public ChunkStorageException(String chunkName, String message, Throwable cause) {
        super(message, cause);
        this.chunkName = chunkName;
    }
}
