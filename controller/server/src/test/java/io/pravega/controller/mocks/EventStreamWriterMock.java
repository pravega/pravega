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
package io.pravega.controller.mocks;

import io.pravega.stream.AckFuture;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.Transaction;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Mock EventStreamWriter.
 */
public class EventStreamWriterMock<T> implements EventStreamWriter<T> {
    BlockingQueue<T> eventList = new LinkedBlockingQueue<>();

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
        List<T> list = new ArrayList<>();
        eventList.drainTo(list);
        return list;
    }

    public EventStreamReader<T> getReader() {
        return new EventStreamReaderMock<>(eventList);
    }
}
