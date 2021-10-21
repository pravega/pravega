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
 * Exception that is thrown whenever we detect an unrecoverable data corruption.
 * Usually, after this is thrown, our only resolution may be to suspend processing in the container or completely bring it offline.
 */
public class DataCorruptionException extends ServiceHaltException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param message        The message for the exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public DataCorruptionException(String message, Object... additionalData) {
        super(message, additionalData);
    }

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param message        The message for the exception.
     * @param cause          The causing exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public DataCorruptionException(String message, Throwable cause, Object... additionalData) {
        super(message, cause, additionalData);
    }
}
