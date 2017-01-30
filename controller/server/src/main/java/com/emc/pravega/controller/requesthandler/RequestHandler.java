package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.controller.requests.ControllerRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public interface RequestHandler<Request extends ControllerRequest> {
    CompletableFuture<Void> process(Request request, ScheduledExecutorService executor);

    String getKey(Request request);
}
