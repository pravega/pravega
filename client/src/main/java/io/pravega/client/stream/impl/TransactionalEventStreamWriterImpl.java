/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

/**
 * This class creates transactions, and manages their lifecycle.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = { "stream", "closed" })
public class TransactionalEventStreamWriterImpl<Type> implements TransactionalEventStreamWriter<Type> {

    private final Stream stream;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventWriterConfig config;
    private final Pinger pinger;
    
    TransactionalEventStreamWriterImpl(Stream stream, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
                                       Serializer<Type> serializer, EventWriterConfig config, ScheduledExecutorService executor) {
        this.stream = Preconditions.checkNotNull(stream);
        this.controller = Preconditions.checkNotNull(controller);
        this.outputStreamFactory = Preconditions.checkNotNull(outputStreamFactory);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.config = config;
        this.pinger = new Pinger(config, stream, controller, executor);
    }

    @RequiredArgsConstructor
    private static class TransactionImpl<Type> implements Transaction<Type> {

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
        TransactionImpl(UUID txId, Controller controller, Stream stream) {
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
            throwIfClosed();
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.close();
            }
            getAndHandleExceptions(controller.commitTransaction(stream, txId), TxnFailedException::new);
            pinger.stopPing(txId);
            closed.set(true);
        }

        @Override
        public void abort() {
            if (!closed.get()) {
                pinger.stopPing(txId);
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
    public Transaction<Type> beginTxn() {
        TxnSegments txnSegments = getAndHandleExceptions(controller.createTransaction(stream, config.getTransactionTimeoutTime()),
                RuntimeException::new);
        UUID txnId = txnSegments.getTxnId();
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        for (Segment s : txnSegments.getSteamSegments().getSegments()) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txnId,
                    config, txnSegments.getSteamSegments().getDelegationToken());
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txnId, out, serializer);
            transactions.put(s, impl);
        }
        pinger.startPing(txnId);
        return new TransactionImpl<Type>(txnId, transactions, txnSegments.getSteamSegments(), controller, stream, pinger);
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
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId, config, segments.getDelegationToken());
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, segments, controller, stream, pinger);
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
