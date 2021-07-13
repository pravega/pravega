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
package io.pravega.controller.store.task;

import io.pravega.controller.retryable.RetryableException;

/**
 * Lock failed exception.
 */
public class LockFailedException extends RuntimeException implements RetryableException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Failed locking resource %s.";

    /**
     * Creates a new instance of LockFailedException class.
     *
     * @param name resource on which lock failed
     */
    public LockFailedException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of LockFailedException class.
     *
     * @param name  resource on which lock failed
     * @param cause error cause
     */
    public LockFailedException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
