/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.Data;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Mock EventStreamWriter.
 */
@Data
public class ControllerEventStreamWriterMock implements EventStreamWriter<ControllerEvent> {
    private final StreamRequestHandler streamRequestHandler;
    private final ScheduledExecutorService executor;

    @Override
    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        Futures.delayedFuture(() ->
                streamRequestHandler.processEvent(event), 1000, executor);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEvent(String routingKey, ControllerEvent event) {
        return writeEvent(event);
    }

    @Override
    public CompletableFuture<Void> writeEventToSegment(int segmentId, ControllerEvent event) {
        return writeEvent(event);
    }

    @Override
    public Transaction<ControllerEvent> beginTxn() {
        return null;
    }

    @Override
    public Transaction<ControllerEvent> getTxn(UUID transactionId) {
        return null;
    }

    @Override
    public EventWriterConfig getConfig() {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }
}
