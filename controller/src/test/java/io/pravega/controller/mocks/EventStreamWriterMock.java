/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Mock EventStreamWriter.
 */
public class EventStreamWriterMock<T> implements EventStreamWriter<T> {
    BlockingQueue<T> eventList = new LinkedBlockingQueue<>();

    @Override
    public CompletableFuture<Void> writeEvent(T event) {
        eventList.add(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEvent(String routingKey, T event) {
        eventList.add(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> writeEventToSegment(int segmentId, T event) {
        eventList.add(event);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Transaction<T> beginTxn() {
        throw new NotImplementedException("beginTxn");
    }

    @Override
    public Transaction<T> getTxn(UUID transactionId) {
        throw new NotImplementedException("getTxn");
    }

    @Override
    public EventWriterConfig getConfig() {
        throw new NotImplementedException("getClientConfig");
    }

    @Override
    public void flush() {
        throw new NotImplementedException("flush");
    }

    @Override
    public void close() {

    }

    public List<T> getEventList() {
        List<T> list = new ArrayList<>();
        eventList.drainTo(list);
        return list;
    }

    public EventStreamReader<T> getReader() {
        return new EventStreamReaderMock<>(eventList);
    }
}
