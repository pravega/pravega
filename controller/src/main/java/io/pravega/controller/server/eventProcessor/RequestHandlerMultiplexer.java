/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Data
public class RequestHandlerMultiplexer implements RequestHandler<ControllerEvent> {
    private final AutoScaleRequestHandler autoScaleRequestHandler;
    private final ScaleOperationRequestHandler scaleOperationRequestHandler;

    @Override
    public CompletableFuture<Void> process(ControllerEvent controllerEvent, EventProcessor.Writer<ControllerEvent> writer) {
        if (controllerEvent instanceof AutoScaleEvent) {
            return autoScaleRequestHandler.process((AutoScaleEvent) controllerEvent, writer::write);
        }
        if (controllerEvent instanceof ScaleOpEvent) {
            return scaleOperationRequestHandler.process((ScaleOpEvent) controllerEvent, writer::write);
        }
        String errorMessage = "RequestHandlerMultiplexer: Unknown event received";
        log.error(errorMessage);

        return FutureHelpers.failedFuture(new RequestUnsupportedException(errorMessage));
    }
}
