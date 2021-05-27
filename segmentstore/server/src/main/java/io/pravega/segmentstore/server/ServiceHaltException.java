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

import io.pravega.segmentstore.contracts.StreamingException;
import lombok.Getter;

/**
 * Exception that is thrown whenever we detect an unrecoverable state.
 * Usually, after this is thrown, our only resolution will be to shut down the Segment Container, DurableLog and StorageWriter.
 */
public class ServiceHaltException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Gets an array of objects that may contain additional context-related information.
     */
    @Getter
    private final Object[] additionalData;

    /**
     * Creates a new instance of the ServiceHaltException class.
     *
     * @param message        The message for the exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public ServiceHaltException(String message, Object... additionalData) {
        this(message, null, additionalData);
    }

    /**
     * Creates a new instance of the ServiceHaltException class.
     *
     * @param message        The message for the exception.
     * @param cause          The causing exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public ServiceHaltException(String message, Throwable cause, Object... additionalData) {
        super(message, cause);
        this.additionalData = additionalData;
    }
}
