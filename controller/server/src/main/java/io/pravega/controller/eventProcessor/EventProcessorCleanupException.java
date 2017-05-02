/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.controller.eventProcessor.impl.EventProcessor;
import lombok.Getter;

/**
 * Wrapper for exceptions thrown from Actor's postStop cleanup hook.
 */
public class EventProcessorCleanupException extends Exception {
    private static final long serialVersionUID = 1L;
    
    @Getter
    private final EventProcessor<?> actor;

    EventProcessorCleanupException(final EventProcessor<?> actor, final String message) {
        super(message);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor<?> actor, final String message, final Throwable throwable) {
        super(message, throwable);
        this.actor = actor;
    }

    EventProcessorCleanupException(final EventProcessor<?> actor, final Throwable throwable) {
        super(throwable);
        this.actor = actor;
    }

}
