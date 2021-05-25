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
package io.pravega.controller.server.eventProcessor.requesthandlers;

import io.pravega.controller.retryable.RetryableException;
import lombok.AllArgsConstructor;

public class TaskExceptions {
    @AllArgsConstructor
    private static class StreamTaskException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String message;
        private final Throwable cause;
    }
    
    /**
     * This exception is thrown if we are unable to post the event into request stream.
     */
    public static class PostEventException extends StreamTaskException implements RetryableException {

        public PostEventException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * This exception is thrown if event processing is not enabled.
     */
    public static class ProcessingDisabledException extends StreamTaskException {
        public ProcessingDisabledException(String message) {
            super(message, null);
        }
    }
}
