/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
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
    CompletableFuture<Boolean> isRerun(T event);
}
