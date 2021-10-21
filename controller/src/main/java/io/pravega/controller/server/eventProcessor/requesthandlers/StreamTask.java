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

import io.pravega.shared.controller.event.ControllerEvent;

import java.util.concurrent.CompletableFuture;

public interface StreamTask<T extends ControllerEvent> {

    /**
     * Method to process the supplied event.
     * @param event event to process
     * @return future of processing
     */
    CompletableFuture<Void> execute(T event);

    /**
     * Method to write back event into the stream.
     * @param event event to write back.
     * @return future of processing
     */
    CompletableFuture<Void> writeBack(T event);

    /**
     * Method that indicates to the processor if the work was already started and this is a rerun. 
     * @param event event to process
     * @return Completable Future which when completed will indicate if the event processing has already started or not. 
     */
    CompletableFuture<Boolean> hasTaskStarted(T event);
}
