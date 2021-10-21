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

/**
 * Exception thrown when offset provided is invalid.
 */
public class InvalidOffsetException extends ChunkStorageException {
    @Getter
    private final long expectedOffset;
    @Getter
    private final long givenOffset;
    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param expectedOffset The offset that was expected.
     * @param givenOffset The offset that was actually supplied.
     * @param message   The message for this exception.
     */
    public InvalidOffsetException(String chunkName, long expectedOffset, long givenOffset, String message) {
        super(chunkName, getMessage(chunkName, expectedOffset, givenOffset, message));
        this.expectedOffset = expectedOffset;
        this.givenOffset = givenOffset;
    }

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param expectedOffset The offset that was expected.
     * @param givenOffset The offset that was actually supplied.
     * @param message   The message for this exception.
     * @param cause     The causing exception.
     */
    public InvalidOffsetException(String chunkName, long expectedOffset, long givenOffset, String message, Throwable cause) {
        super(chunkName, getMessage(chunkName, expectedOffset, givenOffset, message), cause);
        this.expectedOffset = expectedOffset;
        this.givenOffset = givenOffset;
    }

    private static String getMessage(String chunkName, long expectedOffset, long givenOffset, String message) {
        return String.format("Expected offset (%d) did not match given offset (%d) for chunk %s - %s.",
                expectedOffset,
                givenOffset,
                chunkName,
                message);
    }
}
