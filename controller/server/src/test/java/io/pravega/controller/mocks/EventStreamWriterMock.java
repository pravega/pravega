/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.mocks;

import io.pravega.stream.AckFuture;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.Transaction;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Mock EventStreamWriter.
 */
public class EventStreamWriterMock<T> implements EventStreamWriter<T> {
    List<T> eventList = new ArrayList<>();

    @Override
    public AckFuture writeEvent(T event) {
        eventList.add(event);
        return new AckFutureMock(CompletableFuture.completedFuture(true));
    }

    @Override
    public AckFuture writeEvent(String routingKey, T event) {
        eventList.add(event);
        return new AckFutureMock(CompletableFuture.completedFuture(true));
    }

    @Override
    public Transaction<T> beginTxn(long transactionTimeout, long maxExecutionTime, long scaleGracePeriod) {
        throw new NotImplementedException();
    }

    @Override
    public Transaction<T> getTxn(UUID transactionId) {
        throw new NotImplementedException();
    }

    @Override
    public EventWriterConfig getConfig() {
        throw new NotImplementedException();
    }

    @Override
    public void flush() {
        throw new NotImplementedException();
    }

    @Override
    public void close() {

    }

    public List<T> getEventList() {
        return Collections.unmodifiableList(eventList);
    }
}
