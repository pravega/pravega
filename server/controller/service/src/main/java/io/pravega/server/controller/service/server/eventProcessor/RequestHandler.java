/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.server.eventProcessor;

import io.pravega.shared.controller.event.ControllerEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for request handlers.
 *
 * @param <Request> Type of request this handler will process.
 */
@FunctionalInterface
public interface RequestHandler<Request extends ControllerEvent> {
    CompletableFuture<Void> process(Request request);
}
