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
 * Wrapper for exceptions thrown from Actor's preRestart reinitialization hook.
 */
public class EventProcessorReinitException extends ControllerServerException {

    public EventProcessorReinitException(final String message) {
        super(message);
    }

    public EventProcessorReinitException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public EventProcessorReinitException(final Throwable throwable) {
        super(throwable);
    }

}
