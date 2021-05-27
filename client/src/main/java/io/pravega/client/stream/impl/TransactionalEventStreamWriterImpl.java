/**
 * Copyright Pravega Authors.
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
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.Transaction.Status;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.security.auth.AccessOperation;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.NameUtils.getEpoch;

/**
 * This class creates transactions, and manages their lifecycle.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = { "stream", "closed" })
public class TransactionalEventStreamWriterImpl<Type> implements TransactionalEventStreamWriter<Type> {

    private final Stream stream;
    private final String writerId;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventWriterConfig config;
    private final Pinger pinger;
    
    TransactionalEventStreamWriterImpl(Stream stream, String writerId, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
            Serializer<Type> serializer, EventWriterConfig config, ScheduledExecutorService executor) {
        this.stream = Preconditions.checkNotNull(stream);
        this.writerId = Preconditions.checkNotNull(writerId);
        this.controller = Preconditions.checkNotNull(controller);
        this.outputStreamFactory = Preconditions.checkNotNull(outputStreamFactory);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.config = config;
        this.pinger = new Pinger(config.getTransactionTimeoutTime(), stream, controller, executor);
    }

    @RequiredArgsConstructor
    private static class TransactionImpl<Type> implements Transaction<Type> {
        private final String writerId;
        @NonNull
        private final UUID txId;
        private final Map<Segment, SegmentTransaction<Type>> inner;
        private final StreamSegments segments;
        @NonNull
        private final Controller controller;
        @NonNull
        private final Stream stream;
        private final Pinger pinger;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        
        /**
         * Create closed transaction
         */
        TransactionImpl(String writerId, UUID txId, Controller controller, Stream stream) {
            this.writerId = writerId;
            this.txId = txId;
            this.inner = null;
            this.segments = null;
            this.controller = controller;
            this.stream = stream;
            this.pinger = null;
            this.closed.set(true);
        }

        /**
         * Uses the transactionId to generate the routing key so that we only use one segment.
         */
        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            writeEvent(txId.toString(), event);
        }
        
        @Override
        public void writeEvent(String routingKey, Type event) throws TxnFailedException {
            Preconditions.checkNotNull(routingKey);
            Preconditions.checkNotNull(event);
            throwIfClosed();
            Segment s = segments.getSegmentForKey(routingKey);
            SegmentTransaction<Type> transaction = inner.get(s);
            transaction.writeEvent(event);
        }

        @Override
        public void commit() throws TxnFailedException {
            commitTransaction(null);
        }

        @Override
        public void commit(long timestamp) throws TxnFailedException {
            commitTransaction(timestamp);
        }

        private void commitTransaction(Long timestamp) throws TxnFailedException {
            log.info("Commit transaction {}", txId);
            throwIfClosed();
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.close();
            }
            inner.clear(); // clear all references to SegmentTransaction to enable garbage collection.
            Futures.getThrowingException(controller.commitTransaction(stream, writerId, timestamp, txId));
            pinger.stopPing(txId);
            closed.set(true);
        }

        @Override
        public void abort() {
            log.info("Abort transaction {}", txId);
            if (!closed.get()) {
                pinger.stopPing(txId);
                for (SegmentTransaction<Type> tx : inner.values()) {
                    try {
                        tx.close();
                    } catch (TxnFailedException e) {
                        log.debug("Got exception while writing to transaction on abort: {}", e.getMessage());
                    }
                }
                inner.clear(); // clear all references to SegmentTransaction to enable garbage collection.
                Futures.getThrowingException(controller.abortTransaction(stream, txId));
                closed.set(true);
            }
        }

        @Override
        public Status checkStatus() {
            log.info("Check transaction status {}", txId);
            return Futures.getThrowingException(controller.checkTransactionStatus(stream, txId));
        }

        @Override
        public void flush() throws TxnFailedException {
            throwIfClosed();
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
        }

        @Override
        public UUID getTxnId() {
            return txId;
        }
        
        private void throwIfClosed() throws TxnFailedException {
            if (closed.get()) {
                throw new TxnFailedException(getTxnId().toString());
            }
        }
    }

    @Override
    public Transaction<Type> beginTxn() {
        TxnSegments txnSegments = Futures.getThrowingException(controller.createTransaction(stream, config.getTransactionTimeoutTime()));
        log.info("Transaction {} created", txnSegments.getTxnId());
        UUID txnId = txnSegments.getTxnId();
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        DelegationTokenProvider tokenProvider = null;
        for (Segment s : txnSegments.getStreamSegments().getSegments()) {
            if (tokenProvider == null) {
                tokenProvider = DelegationTokenProviderFactory.create(controller, s, AccessOperation.WRITE);
            }
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txnId,
                    config, tokenProvider);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txnId, out, serializer);
            transactions.put(s, impl);
        }
        pinger.startPing(txnId);
        return new TransactionImpl<Type>(writerId, txnId, transactions, txnSegments.getStreamSegments(), controller, stream, pinger);
    }

    @Override
    public Transaction<Type> getTxn(UUID txId) {
        // check if the transaction is open.
        Status status = Futures.getThrowingException(controller.checkTransactionStatus(stream, txId));
        if (status != Status.OPEN) {
            return new TransactionImpl<>(writerId, txId, controller, stream);
        }

        // get the segments corresponding to the transaction.
        StreamSegments segments = Futures.getThrowingException(
                controller.getEpochSegments(stream.getScope(), stream.getStreamName(), getEpoch(txId)));
        assert segments != null : "Epoch segments returned is null";
        Preconditions.checkState(segments.getSegments().size() > 0, "There should be at least 1 epoch segment");

        //Create OutputStream for every segment.
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        DelegationTokenProvider tokenProvider = null;
        for (Segment s : segments.getSegments()) {
            if (tokenProvider == null) {
                tokenProvider = DelegationTokenProviderFactory.create(controller, s, AccessOperation.WRITE);
            }
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId, config,
                    tokenProvider);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        pinger.startPing(txId);
        return new TransactionImpl<Type>(writerId, txId, transactions, segments, controller, stream, pinger);
    }

    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }
        pinger.close();
    }

    @Override
    public EventWriterConfig getConfig() {
        return config;
    }

}
