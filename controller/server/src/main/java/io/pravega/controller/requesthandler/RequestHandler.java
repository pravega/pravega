/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.requesthandler;

import io.pravega.shared.controller.requests.ControllerRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for request handlers.
 *
 * @param <Request> Type of request this handler will process.
 */
@FunctionalInterface
public interface RequestHandler<Request extends ControllerRequest> {
    CompletableFuture<Void> process(Request request);
}
