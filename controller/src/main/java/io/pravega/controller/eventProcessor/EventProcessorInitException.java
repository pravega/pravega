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
package io.pravega.controller.eventProcessor;

import io.pravega.controller.server.ControllerServerException;

/**
 * Wrapper for exceptions thrown from Actor's preStart initialization hook.
 */
public class EventProcessorInitException extends ControllerServerException {

    private static final long serialVersionUID = 1L;

    public EventProcessorInitException(final String message) {
        super(message);
    }

    public EventProcessorInitException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public EventProcessorInitException(final Throwable throwable) {
        super(throwable);
    }

}
