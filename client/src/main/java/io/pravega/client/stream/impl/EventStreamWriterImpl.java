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
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.Transaction.Status;
import io.pravega.client.stream.TxnFailedException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

/**
 * This class takes in events, finds out which segment they belong to and then calls write on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
public class EventStreamWriterImpl<Type> extends IdempotentEventStreamWriterImpl<Type> implements EventStreamWriter<Type> {

    private final Stream stream;
    private final UUID writerId;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final Object lock = new Object();
    private final AtomicLong sequenceGenerator = new AtomicLong();

    EventStreamWriterImpl(Stream stream, UUID writerId, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
            Serializer<Type> serializer, EventWriterConfig config) {
        super(stream, writerId, controller, outputStreamFactory, serializer, config);
        this.stream = stream;
        this.writerId = writerId;
        this.controller = controller;
        this.outputStreamFactory = outputStreamFactory;
        this.serializer = serializer;
    }

    @Override
    public AckFuture writeEvent(Type event) {
        synchronized (lock) {
            long sequence = sequenceGenerator.incrementAndGet();
            return writeEventInternal(null, sequence, event);
        }
    }

    @Override
    public AckFuture writeEvent(String routingKey, Type event) {
        Preconditions.checkNotNull(routingKey);
        synchronized (lock) {
            long sequence = sequenceGenerator.incrementAndGet();
            return writeEventInternal(routingKey, sequence, event);
        }
    }
    
    private static class TransactionImpl<Type> implements Transaction<Type> {

        private final Map<Segment, SegmentTransaction<Type>> inner;
        private final UUID txId;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Controller controller;
        private final Stream stream;
        private StreamSegments segments;

        TransactionImpl(UUID txId, Map<Segment, SegmentTransaction<Type>> transactions, StreamSegments segments,
                Controller controller, Stream stream) {
            this.txId = txId;
            this.inner = transactions;
            this.segments = segments;
            this.controller = controller;
            this.stream = stream;
        }
        
        /**
         * Create closed transaction
         */
        TransactionImpl(UUID txId, Controller controller, Stream stream) {
            this.txId = txId;
            this.inner = null;
            this.segments = null;
            this.controller = controller;
            this.stream = stream;
            this.closed.set(true);
        }

        /**
         * Uses the transactionId to generate the routing key so that we only need to use one segment.
         */
        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            writeEvent(txId.toString(), event);
        }
        
        @Override
        public void writeEvent(String routingKey, Type event) throws TxnFailedException {
            Preconditions.checkNotNull(event);
            throwIfClosed();
            Segment s = segments.getSegmentForKey(routingKey);
            SegmentTransaction<Type> transaction = inner.get(s);
            transaction.writeEvent(event);
        }

        @Override
        public void commit() throws TxnFailedException {
            throwIfClosed();
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.close();
            }
            getAndHandleExceptions(controller.commitTransaction(stream, txId), TxnFailedException::new);
            closed.set(true);
        }

        @Override
        public void abort() {
            if (!closed.get()) {
                for (SegmentTransaction<Type> tx : inner.values()) {
                    try {
                        tx.close();
                    } catch (TxnFailedException e) {
                        log.debug("Got exception while writing to transaction on abort: {}", e.getMessage());
                    }
                }
                getAndHandleExceptions(controller.abortTransaction(stream, txId), RuntimeException::new);
                closed.set(true);
            }
        }

        @Override
        public Status checkStatus() {
            return getAndHandleExceptions(controller.checkTransactionStatus(stream, txId), RuntimeException::new);
        }

        @Override
        public void flush() throws TxnFailedException {
            throwIfClosed();
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
        }

        @Override
        public void ping(long lease) throws PingFailedException {
            Preconditions.checkArgument(lease > 0);
            getAndHandleExceptions(controller.pingTransaction(stream, txId, lease), PingFailedException::new);
        }

        @Override
        public UUID getTxnId() {
            return txId;
        }
        
        private void throwIfClosed() throws TxnFailedException {
            if (closed.get()) {
                throw new TxnFailedException();
            }
        }

    }

    @Override
    public Transaction<Type> beginTxn(long timeout, long maxExecutionTime, long scaleGracePeriod) {
        TxnSegments txnSegments = getAndHandleExceptions(
                controller.createTransaction(stream, timeout, maxExecutionTime, scaleGracePeriod),
                RuntimeException::new);
        UUID txnId = txnSegments.getTxnId();
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        for (Segment s : txnSegments.getSteamSegments().getSegments()) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txnId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txnId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txnId, transactions, txnSegments.getSteamSegments(), controller, stream);
    }
    
    @Override
    public Transaction<Type> getTxn(UUID txId) {
        StreamSegments segments = getAndHandleExceptions(
                controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new);
        Status status = getAndHandleExceptions(controller.checkTransactionStatus(stream, txId), RuntimeException::new);
        if (status != Status.OPEN) {
            return new TransactionImpl<>(txId, controller, stream);
        }
        
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        for (Segment s : segments.getSegments()) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, segments, controller, stream);
        
    }
}
