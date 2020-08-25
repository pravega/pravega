/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.SingleRoutingKeyTransaction;
import io.pravega.client.stream.SingleRoutingKeyTransactionWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import lombok.NonNull;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class creates transactions, and manages their lifecycle.
 *
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = {"stream", "closed"})
public class SingleRoutingKeyTransactionWriterImpl<Type> implements SingleRoutingKeyTransactionWriter<Type> {
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final String routingKey;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed;

    @GuardedBy("$lock")
    private SegmentOutputStream segmentOutputStream;
    @GuardedBy("$lock")
    private SingleRoutingKeyTransactionImpl<Type> transaction;
    private final DelegationTokenProvider tokenProvider;

    SingleRoutingKeyTransactionWriterImpl(Stream stream, String routingKey, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
                                          Serializer<Type> serializer) {
        this.stream = Preconditions.checkNotNull(stream);
        this.routingKey = Preconditions.checkNotNull(routingKey);
        this.controller = Preconditions.checkNotNull(controller);
        this.outputStreamFactory = Preconditions.checkNotNull(outputStreamFactory);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.closed = new AtomicBoolean(false);
        this.tokenProvider = DelegationTokenProviderFactory.create(controller, stream.getScope(), stream.getStreamName());

    }

    private static class SingleRoutingKeyTransactionImpl<Type> implements SingleRoutingKeyTransaction<Type> {
        @NonNull
        private final Serializer<Type> serializer;
        @GuardedBy("$lock")
        private Transaction.Status state;
        @GuardedBy("$lock")
        private final List<ByteBuffer> events;
        private final Function<List<ByteBuffer>, CompletableFuture<Void>> write;

        SingleRoutingKeyTransactionImpl(Serializer<Type> serializer, Function<List<ByteBuffer>, CompletableFuture<Void>> write) {
            this.serializer = serializer;
            this.state = Transaction.Status.OPEN;
            this.events = new ArrayList<>();
            this.write = write;
        }

        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            Preconditions.checkNotNull(event);

            ByteBuffer buffer = serializer.serialize(event);
            synchronized (this) {
                throwIfClosed();
                events.add(buffer);
            }
        }

        @Override
        public CompletableFuture<Void> commit() throws TxnFailedException {
            synchronized (this) {
                throwIfClosed();
                if (state.equals(Transaction.Status.OPEN)) {
                    state = Transaction.Status.COMMITTING;
                } else {
                    throw new IllegalStateException("Transaction is already closed.");
                }
            }
            return write.apply(events);
        }

        @Override
        public void abort() {
            synchronized (this) {
                if (state.equals(Transaction.Status.OPEN)) {
                    state = Transaction.Status.ABORTED;
                }
            }
        }

        private void throwIfClosed() throws TxnFailedException {
            synchronized (this) {
                if (!state.equals(Transaction.Status.OPEN)) {
                    throw new TxnFailedException();
                }
            }
        }
    }

    @Override
    public SingleRoutingKeyTransaction<Type> beginTxn() {
        synchronized (this) {
            if (transaction == null) {
                transaction = new SingleRoutingKeyTransactionImpl<>(serializer, this::writeEvents);
                return transaction;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private SegmentOutputStream getSegmentOutputStream() {
        synchronized (this) {
            if (segmentOutputStream != null) {
                return segmentOutputStream;
            } else {
                return controller.getCurrentSegments(stream.getScope(), stream.getStreamName())
                                 .thenApply(segments -> {
                                     Segment segment = segments.getSegmentForKey(routingKey);
                                     return createSegmentOutputStream(segment);
                                 }).join();
            }
        }
    }

    private SegmentOutputStream createSegmentOutputStream(Segment segment) {
        synchronized (this) {
            if (!segmentOutputStream.getSegmentName().equals(segment.getScopedName())) {
                segmentOutputStream = outputStreamFactory.createOutputStreamForSegment(segment,
                        this::segmentSealedCallback,
                        EventWriterConfig.builder().build(), tokenProvider);
            }
            return segmentOutputStream;
        }
    }

    @Synchronized
    private void segmentSealedCallback(Segment segment) {
        // instead of traversing thru successors we will call current segments and fetch the appropriate segment.
        // by setting segmentOutputStream to null we will force the caller to get the replacement segmentoutputstream.
        // we will call resend while holding the lock so that successor segment outputstream is setup and events are
        // replayed to it before new writeevents into successor are triggered. 
        List<PendingEvent> toResend = segmentOutputStream.getUnackedEventsOnSeal();
        segmentOutputStream = null;
        for (PendingEvent event : toResend) {
            SegmentOutputStream segmentWriter = getSegmentOutputStream();
            segmentWriter.write(event);
        }
    }

    @Synchronized
    private CompletableFuture<Void> writeEvents(List<ByteBuffer> events) {
        Exceptions.checkNotClosed(closed.get(), this);
        CompletableFuture<Void> ackFuture = new CompletableFuture<Void>();
        SegmentOutputStream segmentWriter = getSegmentOutputStream();
        segmentWriter.write(PendingEvent.withHeader(routingKey, events, ackFuture));
        return ackFuture;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (transaction != null && transaction.state.equals(Transaction.Status.OPEN)) {
                transaction.abort();
            }
        }
        closed.set(true);
    }
}
