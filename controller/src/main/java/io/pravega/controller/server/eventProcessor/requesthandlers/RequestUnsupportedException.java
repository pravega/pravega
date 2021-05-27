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

import io.pravega.controller.server.ControllerServerException;

/**
 * Unsupported Request thrown when there is no requestHandler to process the request.
 */
public class RequestUnsupportedException extends ControllerServerException {

    public RequestUnsupportedException(Throwable t) {
        super(t);
    }

    public RequestUnsupportedException(String message) {
        super(message);
    }

    public RequestUnsupportedException(String message, Throwable t) {
        super(message, t);
    }
}
