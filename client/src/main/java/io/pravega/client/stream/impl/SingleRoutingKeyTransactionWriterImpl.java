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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This class creates transactions, and manages their lifecycle.
 *
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = {"stream", "closed"})
public class SingleRoutingKeyTransactionWriterImpl<Type> implements SingleRoutingKeyTransactionWriter<Type> {
    private static final long TIMEOUT = Duration.ofSeconds(10).toMillis();
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
        private static final long MAX_TRANSACTION_SIZE = 16 * 1024 * 1024;
        @NonNull
        private final Controller controller;
        @NonNull
        private final Stream stream;
        @NonNull
        private final Serializer<Type> serializer;
        private final Supplier<SegmentOutputStream> segmentWriterSupplier;
        @GuardedBy("$lock")
        private Transaction.Status state;
        @GuardedBy("$lock")
        private final List<ByteBuffer> events;
        @GuardedBy("$lock")
        private long size;
        private final String routingKey;

        SingleRoutingKeyTransactionImpl(Controller controller, Stream stream, Serializer<Type> serializer,
                                        String routingKey, Supplier<SegmentOutputStream> segmentWriterSupplier) {
            this.controller = controller;
            this.stream = stream;
            this.serializer = serializer;
            this.segmentWriterSupplier = segmentWriterSupplier;
            this.routingKey = routingKey;
            this.state = Transaction.Status.OPEN;
            this.events = new ArrayList<>();
            this.size = 0L;
        }

        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            Preconditions.checkNotNull(event);

            ByteBuffer buffer = serializer.serialize(event);
            synchronized (this) {
                throwIfClosed();
                size += buffer.remaining();
                if (size > MAX_TRANSACTION_SIZE) {
                    throw new TxnFailedException("Size limit exceeded. Combined size for all events should be less than 16 mb.");
                }
                events.add(buffer);
            }
        }

        @Override
        public void commit() throws TxnFailedException {
            synchronized (this) {
                throwIfClosed();
                if (state.equals(Transaction.Status.OPEN)) {
                    state = Transaction.Status.COMMITTING;
                } else {
                    throw new IllegalStateException("commit has already been issued on the transaction");
                }
            }

            // send append block end
            CompletableFuture<Void> commitFuture = new CompletableFuture<>();

            // write the composite pending event and call flush so that the events are all written into 
            // the segment.
            // if this throws segment sealed exception, we will retry a few times before throwing the exception 
            // back to the user.
            Retry.withExpBackoff(0L, 1, 10, 0L)
                 .retryWhen(e -> Exceptions.unwrap(e) instanceof SegmentSealedException)
                 .run(() -> {
                     SegmentOutputStream segmentOutputStream = segmentWriterSupplier.get();

                     segmentOutputStream.write(PendingEvent.withHeader(routingKey, events, commitFuture));
                     segmentOutputStream.flush();
                     Futures.getAndHandleExceptions(commitFuture, TxnFailedException::new);
                     return null;
                 });

            synchronized (this) {
                state = Transaction.Status.COMMITTED;
            }
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
                transaction = new SingleRoutingKeyTransactionImpl<>(controller, stream, serializer, routingKey,
                        this::createSegmentOutputStream);
                return transaction;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private SegmentOutputStream createSegmentOutputStream() {
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

    private void segmentSealedCallback(Segment segment) {
        controller.getSuccessors(segment)
                  .thenApply(successors -> successors.getSegmentToPredecessor().keySet().stream().filter(x -> {
                      // TODO: get routing key hash -- this is not exposed directly
                      double d = 0.0;
                      return d >= x.getRange().getLow() && d < x.getRange().getHigh();
                  }).findFirst().orElse(null))
                  .thenApply(successor -> createSegmentOutputStream(successor.getSegment()));
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
