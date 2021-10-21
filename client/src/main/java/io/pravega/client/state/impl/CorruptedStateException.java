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
package io.pravega.client.state.impl;

public class CorruptedStateException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor for corrupted state exception with message.
     *
     * @param message Exception description
     */
    public CorruptedStateException(String message) {
        super(message);
    }

    /**
     * Constructor for corrupted state exception with message and exception.
     *
     * @param string Exception message
     * @param e      The exception instance indicating a corrupt state and on which the new exception will build on.
     */
    public CorruptedStateException(String string, Exception e) {
        super(string, e);
    }

}
