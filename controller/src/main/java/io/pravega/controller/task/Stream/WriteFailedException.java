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
package io.pravega.controller.task.Stream;

import io.pravega.controller.retryable.RetryableException;

/**
 * Exception thrown when write to a pravega stream fails.
 */
public class WriteFailedException extends RuntimeException implements RetryableException {

    public WriteFailedException() {
        super();
    }

    public WriteFailedException(String message) {
        super(message);
    }

    public WriteFailedException(Throwable cause) {
        super(cause);
    }

    public WriteFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
